"""Stuff+ ① — bundled xgboost pitch-quality run-value model.

Regresses Savant per-pitch ``delta_run_exp`` on physics + fastball-relative
standardized features (no location, no count — isolates raw *stuff*), then
maps the predicted run value to the published Stuff+ ``+``-scale (100 =
league average, higher = better) via :func:`_to_plus`. Follows the
FanGraphs/Eno-Sarris Stuff+ methodology (cited as a reference; no code
copied, so no license obligation).
"""

from __future__ import annotations

from importlib.resources import files
from typing import TYPE_CHECKING, List, Literal, Optional, Union, overload

import numpy as np
import polars as pl

from sportsdataverse.mlb.mlb_pitching_constants import (
    PLUS_SCALE,
    STUFF_LEAGUE_MEAN_RV,
    STUFF_LEAGUE_SD_RV,
    STUFF_PLUS_ARTIFACT,
)

if TYPE_CHECKING:  # pragma: no cover -- annotation-only imports
    import pandas as pd

__all__ = ["STUFF_FEATURES", "_to_plus", "mlb_stuff_plus"]

_EMPTY_SCHEMA_PITCH: dict = {
    "pitcher": pl.Int64,
    "pitch_type": pl.Utf8,
    "stuff_rv_hat": pl.Float64,
    "stuff_plus": pl.Float64,
}
_EMPTY_SCHEMA_ARSENAL: dict = {
    "pitcher": pl.Int64,
    "pitch_type": pl.Utf8,
    "stuff_rv_hat": pl.Float64,
    "stuff_plus": pl.Float64,
}

#: physics + fastball-relative standardized features (no location, no count) --
#: isolates pitch *stuff* from command/sequencing signal.
STUFF_FEATURES: List[str] = [
    "velo_z",
    "spin_z",
    "pfx_x_z",
    "pfx_z_z",
    "release_pos_x_z",
    "release_pos_z_z",
    "extension_z",
]


def _to_plus(rv_hat: np.ndarray, mean_rv: float, sd_rv: float, *, scale: float = PLUS_SCALE) -> np.ndarray:
    """Map predicted run value to the "+"-scale (100 = league average).

    Sign is inverted: a more-negative predicted run value (good for the
    pitcher) maps to a higher "+"-scale score.

    Args:
        rv_hat: Predicted per-pitch (or aggregated) run value.
        mean_rv: League mean of ``rv_hat`` (the fitting-task output).
        sd_rv: League standard deviation of ``rv_hat``. When ``0``, every
            input maps to exactly ``100.0`` (avoids a divide-by-zero).
        scale: "+"-scale points per league SD of ``rv_hat`` (default
            :data:`sportsdataverse.mlb.mlb_pitching_constants.PLUS_SCALE`).

    Returns:
        numpy.ndarray: The "+"-scale score, same shape as ``rv_hat``.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.mlb.mlb_stuff_plus import _to_plus
            _to_plus(np.array([0.0, -0.1, 0.1]), mean_rv=0.0, sd_rv=0.1, scale=10.0)
    """
    rv_hat = np.asarray(rv_hat, dtype=float)
    if sd_rv == 0:
        return np.full_like(rv_hat, 100.0)
    return 100.0 - scale * (rv_hat - mean_rv) / sd_rv


def _load_stuff_booster(models_dir: Optional[str] = None):  # type: ignore[no-untyped-def]
    """Load the bundled Stuff+ booster (or from ``models_dir``); no first-use download."""
    from xgboost import Booster

    if models_dir is not None:
        path = f"{models_dir}/{STUFF_PLUS_ARTIFACT}"
    else:
        path = str(files("sportsdataverse.mlb.models").joinpath(STUFF_PLUS_ARTIFACT))
    booster = Booster()
    booster.load_model(path)
    return booster


@overload
def mlb_stuff_plus(
    pitches: pl.DataFrame, *, level: Literal["pitch"] = "pitch", return_as_pandas: Literal[False] = False
) -> pl.DataFrame: ...
@overload
def mlb_stuff_plus(
    pitches: pl.DataFrame, *, level: Literal["arsenal"], return_as_pandas: Literal[False] = False
) -> pl.DataFrame: ...
@overload
def mlb_stuff_plus(
    pitches: pl.DataFrame, *, level: str = "pitch", return_as_pandas: Literal[True]
) -> "pd.DataFrame": ...
def mlb_stuff_plus(
    pitches: pl.DataFrame, *, level: str = "pitch", return_as_pandas: bool = False
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Score pitches with the bundled Stuff+ (①) run-value model.

    Args:
        pitches: Output of :func:`sportsdataverse.mlb.mlb_pitch_features.pitch_features`
            (needs ``velo_z``, ``spin_z``, ``pfx_x_z``, ``pfx_z_z``,
            ``release_pos_x_z``, ``release_pos_z_z``, ``extension_z``).
        level: ``"pitch"`` (default) for per-pitch output, or ``"arsenal"``
            for a per ``(pitcher, pitch_type)`` mean.
        return_as_pandas: When ``True``, return a ``pandas.DataFrame``.

    Returns:
        ``pitcher``, ``pitch_type``, ``stuff_rv_hat``, ``stuff_plus`` — one
        row per pitch (``level="pitch"``) or per pitcher-pitchtype
        (``level="arsenal"``). Empty input returns a zero-row frame with the
        documented schema.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_pitch_features import pitch_features
            from sportsdataverse.mlb.mlb_stuff_plus import mlb_stuff_plus
            feats = pitch_features(raw_pitches)
            out = mlb_stuff_plus(feats, level="arsenal")
            print(out.sort("stuff_plus", descending=True).head())

        Pipeline next step::

            out.filter(pl.col("pitch_type") == "FF").sort("stuff_plus", descending=True)

        See Also:
            * `baseballr`_ -- companion R package for Statcast-based pitching analysis.

        .. _baseballr: https://baseballr.sportsdataverse.org
    """
    if pitches is None or pitches.height == 0:
        schema = _EMPTY_SCHEMA_ARSENAL if level == "arsenal" else _EMPTY_SCHEMA_PITCH
        out = pl.DataFrame(schema=schema)
        return out.to_pandas() if return_as_pandas else out

    have = [c for c in STUFF_FEATURES if c in pitches.columns]
    scored = pitches.filter(pl.all_horizontal([pl.col(c).is_not_null() for c in have]))

    booster = _load_stuff_booster()
    from xgboost import DMatrix

    x = scored.select(have).to_numpy()
    dmat = DMatrix(x, feature_names=have)
    rv_hat = booster.predict(dmat)
    plus = _to_plus(rv_hat, mean_rv=STUFF_LEAGUE_MEAN_RV, sd_rv=STUFF_LEAGUE_SD_RV)

    scored = scored.with_columns(
        pl.Series("stuff_rv_hat", rv_hat, dtype=pl.Float64),
        pl.Series("stuff_plus", plus, dtype=pl.Float64),
    )

    if level == "arsenal":
        out = scored.group_by("pitcher", "pitch_type").agg(
            pl.col("stuff_rv_hat").mean(),
            pl.col("stuff_plus").mean(),
        )
    else:
        out = scored.select("pitcher", "pitch_type", "stuff_rv_hat", "stuff_plus")

    return out.to_pandas() if return_as_pandas else out
