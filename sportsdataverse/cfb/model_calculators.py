"""User-facing CFB model calculators.

Hand a data frame to one of these and get model output back, whether the rows
came from a real game or were typed by hand to ask a hypothetical -- the same
shape ``sportsdataverse.nfl`` already provides via
``calculate_expected_points()``.

Every calculator validates its input against the model's OWN published card
rather than a feature list restated here. Six such restatements exist in this
package and are exactly the drift that produced cfbfastR-cfb-data#70.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import polars as pl
from xgboost import DMatrix

from sportsdataverse.cfb.model_cards import card_era_contract, card_features

__all__ = [
    "calculate_completion_probability",
    "calculate_epa",
    "calculate_expected_points",
    "calculate_field_goal_probability",
    "calculate_fourth_down",
    "calculate_qbr",
    "calculate_two_point_probability",
    "calculate_win_probability",
    "calculate_wpa",
    "calculate_xpass",
    "predict_from_card",
    "add_era_columns",
]


def predict_from_card(df: pl.DataFrame, model: str, booster: Any) -> np.ndarray:
    """Score ``df`` with ``booster``, validated and ordered by the model's card.

    Args:
        df: Frame carrying at least the model's declared features. Extra
            columns are ignored, so a full pbp frame passes through unchanged.
        model: Bundle stem, used to look up the card and to name the model in
            any error.
        booster: The loaded ``xgboost.Booster``.

    Returns:
        The booster's raw predictions.

    Raises:
        ValueError: When any declared feature is absent from ``df``. Every
            missing column is named in one message -- the failure this surface
            exists to remove is a caller unable to tell what their frame lacks.

    Example:
        Quick start::

            from sportsdataverse.cfb.model_calculators import predict_from_card
            predict_from_card(pbp, "xpass_model", booster)
    """
    feats = card_features(model)
    missing = [f for f in feats if f not in df.columns]
    if missing:
        raise ValueError(
            f"{model} needs {len(missing)} column(s) not present: {', '.join(missing)}. "
            f"Its card declares: {', '.join(feats)}"
        )
    # Selected in the CARD's order, never the frame's: XGBoost aligns a DMatrix
    # by position, so frame order would silently score against wrong columns.
    matrix = df.select(feats).to_pandas()
    return booster.predict(DMatrix(matrix, feature_names=feats))


def add_era_columns(df: pl.DataFrame, model: str, season: int | None = None) -> pl.DataFrame:
    """Add the era column(s) ``model`` consumes, using ITS card's cuts.

    The cuts are read from the published contract, never restated here. That is
    the fix for cfbfastR-cfb-data#70, where both consumers kept a private copy of
    the era boundary, both drifted to a 2017 cut the trainer never used, and
    2018-2020 scored an era off the models trained with them.

    Args:
        df: Frame carrying a ``season`` column, or any frame when ``season`` is
            given explicitly.
        model: Bundle stem, used to look up the era contract.
        season: Season to use when ``df`` has no ``season`` column -- the
            hand-built-row case.

    Returns:
        ``df`` with the contract's columns added. Returned unchanged when the
        model declares no era contract, or when the columns are already present.

    Raises:
        ValueError: When the model needs an era column but neither a ``season``
            column nor a ``season`` argument is available to derive it.

    Example:
        Quick start::

            from sportsdataverse.cfb.model_calculators import add_era_columns
            add_era_columns(pl.DataFrame({"season": [2018]}), "xpass_model")
    """
    contract = card_era_contract(model)
    if not contract:
        return df
    columns = contract["columns"]
    if all(c in df.columns for c in columns):
        return df
    lo, mid, hi = contract["cuts"]
    if season is not None:
        season_expr = pl.lit(season, dtype=pl.Int64)
    elif "season" in df.columns:
        season_expr = pl.col("season").cast(pl.Int64)
    else:
        raise ValueError(f"{model} needs an era column; supply a 'season' column or the season= argument")
    bucket = (
        pl.when(season_expr <= lo).then(0).when(season_expr <= mid).then(1).when(season_expr <= hi).then(2).otherwise(3)
    )
    if contract["encoding"] == "ordinal":
        return df.with_columns(bucket.cast(pl.Int32).alias(columns[0]))
    return df.with_columns([(bucket == i).cast(pl.Int32).alias(col) for i, col in enumerate(columns)])


def _booster_for(model: str) -> Any:
    """Resolve a card stem to its loaded booster.

    The boosters are module-level names across three modules rather than one
    mapping, and two are lazily loaded, so this is the single place that knows
    where each lives. Imports are function-local: ``cfb_pbp`` is a heavy import
    and a module-level one risks a cycle.

    Args:
        model: Bundle stem, e.g. ``"wp_spread"``.

    Returns:
        The loaded ``xgboost.Booster``.

    Raises:
        KeyError: When the stem has no registered booster.
        RuntimeError: When the booster exists but did not load -- ``fg_model``
            and ``two_pt_model`` are ``None`` when their file is absent, and
            scoring against ``None`` would fail far from the cause.
    """
    from sportsdataverse.cfb import cfb_pbp

    direct = {
        "ep_model": lambda: cfb_pbp.ep_model,
        # wp_model IS wp_spread -- it loads from wp_spread_file (cfb_pbp.py:245)
        "wp_spread": lambda: cfb_pbp.wp_model,
        "wp_naive": lambda: cfb_pbp.wp_naive_model,
        "qbr_model": lambda: cfb_pbp.qbr_model,
        "cfb_cp_model": lambda: cfb_pbp.cp_model,
        "xpass_model": lambda: cfb_pbp.xpass_model,
    }
    if model in direct:
        return direct[model]()
    if model == "two_pt_model":
        from sportsdataverse.cfb import cfb_two_point

        booster = cfb_two_point.two_pt_model
    elif model == "fg_model":
        from sportsdataverse.cfb import cfb_fourth_down

        booster = cfb_fourth_down.fg_model
    elif model == "fd_model":
        from sportsdataverse.cfb import cfb_fourth_down

        return cfb_fourth_down._load_fd_model()
    else:
        raise KeyError(f"no booster registered for {model!r}")
    if booster is None:
        raise RuntimeError(f"{model} did not load; its .ubj is missing from the package")
    return booster


def _calculate(
    df: pl.DataFrame,
    model: str,
    out_col: str,
    *,
    season: int | None = None,
    return_as_pandas: bool = False,
    transform: Any = None,
) -> Any:
    """Shared body: derive era, validate against the card, predict, append.

    Args:
        df: Caller's frame.
        model: Bundle stem.
        out_col: Name for the appended output column.
        season: Season for era derivation when ``df`` carries none.
        return_as_pandas: Return a pandas frame instead of polars.
        transform: Optional callable mapping raw predictions to the output
            column, used by the multiclass EP model.

    Returns:
        The caller's frame with ``out_col`` appended. Every input column is
        preserved -- a calculator that dropped columns would make chaining two
        of them lossy.
    """
    prepared = add_era_columns(df, model, season=season)
    raw = predict_from_card(prepared, model, _booster_for(model))
    values = transform(raw) if transform is not None else raw
    out = prepared.with_columns(pl.Series(out_col, values))
    return out.to_pandas() if return_as_pandas else out


def calculate_xpass(df, *, season=None, return_as_pandas=False):
    """Expected pass probability for each row.

    Mirrors the shape of ``sportsdataverse.nfl``'s calculators. Rows may come
    from a play-by-play frame or be typed by hand to ask a hypothetical; only
    the model card's declared columns are required, and extra columns pass
    through untouched.

    Args:
        df: Frame carrying ``down``, ``distance``, ``yards_to_goal``, ``pos_score_diff``, ``TimeSecsRem``, ``period``, plus either a ``season`` column or the
            ``season`` argument when the model consumes an era feature.
        season: Season used to derive era columns when ``df`` has none.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        ``df`` with an ``xpass`` column (probability the play is a pass) appended. Input columns are preserved, so
        chaining two calculators is lossless.

    Raises:
        ValueError: When a declared feature is missing; the message names each
            absent column and the model that wanted it.

    Example:
        Quick start::

            from sportsdataverse.cfb import calculate_xpass
            calculate_xpass(df, season=2024)
    """
    return _calculate(df, "xpass_model", "xpass", season=season, return_as_pandas=return_as_pandas)


def calculate_field_goal_probability(df, *, season=None, return_as_pandas=False):
    """Field-goal make probability for each row.

    Mirrors the shape of ``sportsdataverse.nfl``'s calculators. Rows may come
    from a play-by-play frame or be typed by hand to ask a hypothetical; only
    the model card's declared columns are required, and extra columns pass
    through untouched.

    Args:
        df: Frame carrying ``yards_to_goal``, plus either a ``season`` column or the
            ``season`` argument when the model consumes an era feature.
        season: Season used to derive era columns when ``df`` has none.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        ``df`` with an ``fg_prob`` column appended. Input columns are preserved, so
        chaining two calculators is lossless.

    Raises:
        ValueError: When a declared feature is missing; the message names each
            absent column and the model that wanted it.

    Example:
        Quick start::

            from sportsdataverse.cfb import calculate_field_goal_probability
            calculate_field_goal_probability(df, season=2024)
    """
    return _calculate(df, "fg_model", "fg_prob", season=season, return_as_pandas=return_as_pandas)


def calculate_completion_probability(df, *, season=None, return_as_pandas=False):
    """Completion probability for each pass attempt.

    Mirrors the shape of ``sportsdataverse.nfl``'s calculators. Rows may come
    from a play-by-play frame or be typed by hand to ask a hypothetical; only
    the model card's declared columns are required, and extra columns pass
    through untouched.

    Args:
        df: Frame carrying ``down``, ``distance``, ``yards_to_goal``, ``score_diff``, ``seconds_remaining``, ``is_home``, ``period``, ``passing_down``, plus either a ``season`` column or the
            ``season`` argument when the model consumes an era feature.
        season: Season used to derive era columns when ``df`` has none.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        ``df`` with a ``cp`` column appended. Input columns are preserved, so
        chaining two calculators is lossless.

    Raises:
        ValueError: When a declared feature is missing; the message names each
            absent column and the model that wanted it.

    Example:
        Quick start::

            from sportsdataverse.cfb import calculate_completion_probability
            calculate_completion_probability(df, season=2024)
    """
    return _calculate(df, "cfb_cp_model", "cp", season=season, return_as_pandas=return_as_pandas)


def calculate_two_point_probability(df, *, season=None, return_as_pandas=False):
    """Two-point conversion success probability.

    Mirrors the shape of ``sportsdataverse.nfl``'s calculators. Rows may come
    from a play-by-play frame or be typed by hand to ask a hypothetical; only
    the model card's declared columns are required, and extra columns pass
    through untouched.

    Args:
        df: Frame carrying ``posteam_spread``, ``posteam_total``, ``pos_score_diff``, plus either a ``season`` column or the
            ``season`` argument when the model consumes an era feature.
        season: Season used to derive era columns when ``df`` has none.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        ``df`` with a ``two_pt_prob`` column appended. Input columns are preserved, so
        chaining two calculators is lossless.

    Raises:
        ValueError: When a declared feature is missing; the message names each
            absent column and the model that wanted it.

    Example:
        Quick start::

            from sportsdataverse.cfb import calculate_two_point_probability
            calculate_two_point_probability(df, season=2024)
    """
    return _calculate(df, "two_pt_model", "two_pt_prob", season=season, return_as_pandas=return_as_pandas)


def calculate_fourth_down(df, *, season=None, return_as_pandas=False):
    """Fourth-down conversion model output for each row.

    Mirrors the shape of ``sportsdataverse.nfl``'s calculators. Rows may come
    from a play-by-play frame or be typed by hand to ask a hypothetical; only
    the model card's declared columns are required, and extra columns pass
    through untouched.

    Args:
        df: Frame carrying ``down``, ``distance``, ``yards_to_goal``, ``posteam_total``, ``posteam_spread``, plus either a ``season`` column or the
            ``season`` argument when the model consumes an era feature.
        season: Season used to derive era columns when ``df`` has none.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        ``df`` with an ``fd_prob`` column appended. Input columns are preserved, so
        chaining two calculators is lossless.

    Raises:
        ValueError: When a declared feature is missing; the message names each
            absent column and the model that wanted it.

    Example:
        Quick start::

            from sportsdataverse.cfb import calculate_fourth_down
            calculate_fourth_down(df, season=2024)
    """
    return _calculate(df, "fd_model", "fd_prob", season=season, return_as_pandas=return_as_pandas)


def calculate_qbr(df, *, season=None, return_as_pandas=False):
    """Model QBR for each row.

    Mirrors the shape of ``sportsdataverse.nfl``'s calculators. Rows may come
    from a play-by-play frame or be typed by hand to ask a hypothetical; only
    the model card's declared columns are required, and extra columns pass
    through untouched.

    Args:
        df: Frame carrying ``qbr_epa``, ``sack_epa``, ``pass_epa``, ``rush_epa``, ``pen_epa``, ``spread``, plus either a ``season`` column or the
            ``season`` argument when the model consumes an era feature.
        season: Season used to derive era columns when ``df`` has none.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        ``df`` with a ``qbr`` column appended. Input columns are preserved, so
        chaining two calculators is lossless.

    Raises:
        ValueError: When a declared feature is missing; the message names each
            absent column and the model that wanted it.

    Example:
        Quick start::

            from sportsdataverse.cfb import calculate_qbr
            calculate_qbr(df, season=2024)
    """
    return _calculate(df, "qbr_model", "qbr", season=season, return_as_pandas=return_as_pandas)


def calculate_expected_points(df, *, season=None, return_as_pandas=False):
    """Expected points for each row.

    Mirrors ``sportsdataverse.nfl.calculate_expected_points()``. The EP booster
    is ``multi:softprob`` over seven next-score classes; this collapses those
    probabilities to a points expectation using the package's own
    ``ep_class_to_score_mapping`` rather than restating the class order.

    Args:
        df: Frame carrying ``TimeSecsRem``, ``yards_to_goal``, ``distance``,
            ``down_1`` through ``down_4`` and ``pos_score_diff_start``.
        season: Unused by this model (EP consumes no era feature); accepted so
            every calculator shares one signature.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        ``df`` with the seven class probability columns and an ``ep`` column
        appended.

    Raises:
        ValueError: When a declared feature is missing.

    Example:
        Quick start::

            from sportsdataverse.cfb import calculate_expected_points
            calculate_expected_points(pbp)
    """
    from sportsdataverse.cfb.model_vars import ep_class_to_score_mapping

    prepared = add_era_columns(df, "ep_model", season=season)
    probs = predict_from_card(prepared, "ep_model", _booster_for("ep_model"))
    probs = np.asarray(probs)
    if probs.ndim == 1:  # a single row comes back flat
        probs = probs.reshape(1, -1)
    #: Column names in the booster's own class order, matched to the score each
    #: class is worth. Reading the mapping keeps this from becoming a seventh
    #: private copy of the class contract.
    names = ["td_prob", "opp_td_prob", "fg_prob", "opp_fg_prob", "safety_prob", "opp_safety_prob", "no_score_prob"]
    scores = np.array([ep_class_to_score_mapping[i] for i in range(probs.shape[1])], dtype=float)
    out = prepared.with_columns([pl.Series(names[i], probs[:, i]) for i in range(probs.shape[1])]).with_columns(
        pl.Series("ep", probs @ scores)
    )
    return out.to_pandas() if return_as_pandas else out


def calculate_win_probability(df, *, season=None, return_as_pandas=False):
    """Win probability for each row.

    Selects the booster the way the pipeline does: ``wp_spread`` when the frame
    carries a ``spread_time`` column, ``wp_naive`` otherwise. The naive model is
    the spread model minus that single feature, so which one applies is decided
    by whether the caller has spread information at all.

    Args:
        df: Frame carrying the win-probability features. Include
            ``spread_time`` to use the spread model.
        season: Unused by these models; accepted for signature consistency.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        ``df`` with a ``wp`` column appended.

    Raises:
        ValueError: When a declared feature is missing; the message names the
            model actually selected, so a caller can tell which contract failed.

    Example:
        Quick start::

            from sportsdataverse.cfb import calculate_win_probability
            calculate_win_probability(pbp)
    """
    model = "wp_spread" if "spread_time" in df.columns else "wp_naive"
    return _calculate(df, model, "wp", season=season, return_as_pandas=return_as_pandas)


def calculate_epa(df, *, season=None, return_as_pandas=False):
    """Expected points added: the change in EP across a play.

    Recomputes ``ep`` when it is absent, matching nflfastR's behaviour. Requires
    ``ep_end`` -- the expected points after the play -- because EPA is a
    difference and this function scores rows, not sequences.

    Args:
        df: Frame with the EP features and an ``ep_end`` column.
        season: Unused by the EP model; accepted for signature consistency.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        ``df`` with ``ep`` (if it was absent) and ``epa`` appended.

    Raises:
        ValueError: When ``ep_end`` is missing, or an EP feature is.

    Example:
        Quick start::

            from sportsdataverse.cfb import calculate_epa
            calculate_epa(pbp)
    """
    if "ep_end" not in df.columns:
        raise ValueError(
            "calculate_epa needs an 'ep_end' column (expected points after the play); "
            "EPA is a difference, and this scores rows rather than sequences"
        )
    out = df if "ep" in df.columns else calculate_expected_points(df, season=season)
    out = out.with_columns((pl.col("ep_end") - pl.col("ep")).alias("epa"))
    return out.to_pandas() if return_as_pandas else out


def calculate_wpa(df, *, season=None, return_as_pandas=False):
    """Win probability added: the change in WP across a play.

    Recomputes ``wp`` when it is absent. Requires ``wp_end`` for the same reason
    ``calculate_epa`` requires ``ep_end``.

    Args:
        df: Frame with the WP features and a ``wp_end`` column.
        season: Unused by these models; accepted for signature consistency.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        ``df`` with ``wp`` (if it was absent) and ``wpa`` appended.

    Raises:
        ValueError: When ``wp_end`` is missing, or a WP feature is.

    Example:
        Quick start::

            from sportsdataverse.cfb import calculate_wpa
            calculate_wpa(pbp)
    """
    if "wp_end" not in df.columns:
        raise ValueError(
            "calculate_wpa needs a 'wp_end' column (win probability after the play); "
            "WPA is a difference, and this scores rows rather than sequences"
        )
    out = df if "wp" in df.columns else calculate_win_probability(df, season=season)
    out = out.with_columns((pl.col("wp_end") - pl.col("wp")).alias("wpa"))
    return out.to_pandas() if return_as_pandas else out
