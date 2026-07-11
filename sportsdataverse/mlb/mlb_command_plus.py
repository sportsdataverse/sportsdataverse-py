"""Command+ / Location+ ② — bundled xgboost location-value model.

Regresses Savant per-pitch ``delta_run_exp`` on location + count/handedness/
pitch-type context (no raw physics — isolates *where* the pitch went, not
*what* it was), then maps to the ``+``-scale via the shared
:func:`sportsdataverse.mlb.mlb_stuff_plus._to_plus` normalization (DRY — one
"+"-scale definition for both bundled models).

**Scope note.** Statcast has no intended-target field (no catcher setup /
target-location signal), so this model cannot separate "aimed here, missed"
from "aimed here, hit it." It therefore ships as **Location+** — the run
value of the *actual* pitch location — under the public name ``command_plus``.
The distinction is real: a pitcher who consistently paints the intended corner
gets the same score here as one who got lucky center-cutting a location that
happened to be a called strike. Upgrading to true intended-vs-actual command
would require a catcher-target or miss-distance signal not in the shipped
Statcast data.
"""

from __future__ import annotations

from importlib.resources import files
from typing import TYPE_CHECKING, List, Literal, Optional, Union, overload

import polars as pl

from sportsdataverse.mlb.mlb_pitching_constants import (
    COMMAND_LEAGUE_MEAN_RV,
    COMMAND_LEAGUE_SD_RV,
    COMMAND_PLUS_ARTIFACT,
)
from sportsdataverse.mlb.mlb_stuff_plus import _to_plus

if TYPE_CHECKING:  # pragma: no cover -- annotation-only imports
    import pandas as pd

__all__ = ["COMMAND_FEATURES", "mlb_command_plus"]

#: location + context features (no raw physics) -- isolates *location* value.
COMMAND_FEATURES: List[str] = [
    "plate_x_abs",
    "plate_z_norm",
    "in_zone",
    "dist_from_heart",
    "balls",
    "strikes",
]

#: numeric-encoded categorical features appended after one-hot/ordinal mapping.
_CATEGORICAL_RAW: List[str] = ["stand", "p_throws", "pitch_type"]

#: FIXED ordinal codes for each categorical column -- deliberately NOT derived
#: from ``.cast(Categorical).to_physical()`` on whatever data happens to be
#: passed in. That per-call approach assigns codes by first-occurrence order
#: in the *current* frame, so the same pitch type gets a DIFFERENT integer
#: code depending on what other data is in the frame -- silently corrupting
#: the model whenever training data and scoring data don't share an identical
#: row order (i.e. always, in practice: caught via a real held-out-population
#: oracle-gate flake during this build). These fixed maps guarantee the same
#: string always encodes to the same number at train time AND score time.
#: Unmapped values fall back to a shared "other" code (``len(mapping)``).
_STAND_CODES: dict = {"L": 0, "R": 1}
_P_THROWS_CODES: dict = {"L": 0, "R": 1}
#: Canonical Statcast ``pitch_type`` abbreviations (fastballs, breaking,
#: offspeed, and the rare/legacy codes Savant occasionally ships).
_PITCH_TYPE_CODES: dict = {
    "FF": 0,
    "SI": 1,
    "FC": 2,
    "FS": 3,
    "FO": 4,
    "SL": 5,
    "ST": 6,
    "SV": 7,
    "CU": 8,
    "KC": 9,
    "CS": 10,
    "CH": 11,
    "KN": 12,
    "SC": 13,
    "EP": 14,
    "PO": 15,
    "FA": 16,
    "UN": 17,
}
_CATEGORICAL_CODE_MAPS: dict = {"stand": _STAND_CODES, "p_throws": _P_THROWS_CODES, "pitch_type": _PITCH_TYPE_CODES}


def _fixed_ordinal_expr(col: str, mapping: dict) -> pl.Expr:
    """Build a ``pl.when/then`` chain mapping ``col``'s known values to fixed codes.

    Args:
        col: Column name.
        mapping: ``{raw_value: code}``. Values not present in ``mapping`` map
            to ``len(mapping)`` (a shared "other" code).

    Returns:
        A polars expression aliased ``f"{col}_code"``, dtype ``Float64``.
    """
    fallback = float(len(mapping))
    expr = pl.lit(fallback)
    for value, code in mapping.items():
        expr = pl.when(pl.col(col) == value).then(pl.lit(float(code))).otherwise(expr)
    return expr.alias(f"{col}_code")


_EMPTY_SCHEMA: dict = {
    "pitcher": pl.Int64,
    "pitch_type": pl.Utf8,
    "location_rv_hat": pl.Float64,
    "command_plus": pl.Float64,
}
_EMPTY_SCHEMA_PITCHER_LEVEL: dict = {
    "pitcher": pl.Int64,
    "location_rv_hat": pl.Float64,
    "command_plus": pl.Float64,
}


def _encode_categoricals(df: pl.DataFrame) -> pl.DataFrame:
    """Ordinal-encode ``stand``/``p_throws``/``pitch_type`` using the FIXED code maps.

    Unlike ``.cast(Categorical).to_physical()`` (which assigns codes by
    first-occurrence order within whatever frame is passed in -- different
    between the training corpus and any scoring call), this uses the shared
    ``_CATEGORICAL_CODE_MAPS`` so a given raw value always encodes to the same
    number, train time or score time, regardless of what else is in the frame.
    """
    exprs = []
    for col in _CATEGORICAL_RAW:
        if col in df.columns:
            exprs.append(_fixed_ordinal_expr(col, _CATEGORICAL_CODE_MAPS[col]))
    return df.with_columns(exprs) if exprs else df


def _model_feature_names() -> List[str]:
    return COMMAND_FEATURES + [f"{c}_code" for c in _CATEGORICAL_RAW]


def _load_command_booster(models_dir: Optional[str] = None):  # type: ignore[no-untyped-def]
    from xgboost import Booster

    if models_dir is not None:
        path = f"{models_dir}/{COMMAND_PLUS_ARTIFACT}"
    else:
        path = str(files("sportsdataverse.mlb.models").joinpath(COMMAND_PLUS_ARTIFACT))
    booster = Booster()
    booster.load_model(path)
    return booster


@overload
def mlb_command_plus(
    pitches: pl.DataFrame, *, level: Literal["pitch"] = "pitch", return_as_pandas: Literal[False] = False
) -> pl.DataFrame: ...
@overload
def mlb_command_plus(
    pitches: pl.DataFrame, *, level: Literal["pitcher"], return_as_pandas: Literal[False] = False
) -> pl.DataFrame: ...
@overload
def mlb_command_plus(
    pitches: pl.DataFrame, *, level: str = "pitch", return_as_pandas: Literal[True]
) -> "pd.DataFrame": ...
def mlb_command_plus(
    pitches: pl.DataFrame, *, level: str = "pitch", return_as_pandas: bool = False
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Score pitches with the bundled Command+/Location+ (②) run-value model.

    Args:
        pitches: Output of :func:`sportsdataverse.mlb.mlb_pitch_features.pitch_features`
            (needs ``plate_x_abs``, ``plate_z_norm``, ``in_zone``,
            ``dist_from_heart``, ``balls``, ``strikes``, ``stand``,
            ``p_throws``, ``pitch_type``).
        level: ``"pitch"`` (default) for per-pitch output, or ``"pitcher"``
            for a per-pitcher mean.
        return_as_pandas: When ``True``, return a ``pandas.DataFrame``.

    Returns:
        ``level="pitch"``: ``pitcher``, ``pitch_type``, ``location_rv_hat``,
        ``command_plus``. ``level="pitcher"``: ``pitcher``,
        ``location_rv_hat``, ``command_plus`` (per-pitcher mean). Empty input
        returns a zero-row frame with the documented schema.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_pitch_features import pitch_features
            from sportsdataverse.mlb.mlb_command_plus import mlb_command_plus
            feats = pitch_features(raw_pitches)
            out = mlb_command_plus(feats)
            print(out.select("pitcher", "command_plus").head())

        Pipeline next step::

            out.group_by("pitcher").agg(pl.col("command_plus").mean()).sort("command_plus", descending=True)

        See Also:
            * `baseballr`_ -- companion R package for Statcast-based pitching analysis.

        .. _baseballr: https://baseballr.sportsdataverse.org
    """
    if pitches is None or pitches.height == 0:
        schema = _EMPTY_SCHEMA_PITCHER_LEVEL if level == "pitcher" else _EMPTY_SCHEMA
        out = pl.DataFrame(schema=schema)
        return out.to_pandas() if return_as_pandas else out

    df = _encode_categoricals(pitches)
    feature_names = _model_feature_names()
    have = [c for c in feature_names if c in df.columns]
    scored = df.filter(pl.all_horizontal([pl.col(c).is_not_null() for c in have]))

    booster = _load_command_booster()
    from xgboost import DMatrix

    x = scored.select(have).to_numpy()
    dmat = DMatrix(x, feature_names=have)
    rv_hat = booster.predict(dmat)
    plus = _to_plus(rv_hat, mean_rv=COMMAND_LEAGUE_MEAN_RV, sd_rv=COMMAND_LEAGUE_SD_RV)

    scored = scored.with_columns(
        pl.Series("location_rv_hat", rv_hat, dtype=pl.Float64),
        pl.Series("command_plus", plus, dtype=pl.Float64),
    )

    if level == "pitcher":
        out = scored.group_by("pitcher").agg(
            pl.col("location_rv_hat").mean(),
            pl.col("command_plus").mean(),
        )
    else:
        out = scored.select("pitcher", "pitch_type", "location_rv_hat", "command_plus")

    return out.to_pandas() if return_as_pandas else out
