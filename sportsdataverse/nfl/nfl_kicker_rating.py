"""NFL environment-adjusted kicker rating ③ — FGOE + empirical-Bayes shrink.

Reuses the shipped ``fg_model`` (via the public
:func:`sportsdataverse.nfl.nfl_fourth_down.fg_make_probability`) as the base
make probability, applies a fitted environment logit shift
(``ENVIRONMENT_FG_COEF``: wind / temp / altitude), and rates kickers by
FG-over-expected shrunk with the split-half-fitted ``K_fg`` prior.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, List, Optional, Tuple, Union

import numpy as np
import polars as pl
from scipy.special import expit, logit

from sportsdataverse.nfl.nfl_fourth_down import fg_make_probability
from sportsdataverse.nfl.nfl_scheme_constants import (
    EB_PRIOR,
    ENVIRONMENT_FG_COEF,
    STADIUM_ALTITUDE,
    as_of_split,
)

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd

_RATING_SCHEMA: dict = {
    "season": pl.Int64,
    "kicker_player_id": pl.Utf8,
    "kicker": pl.Utf8,
    "team": pl.Utf8,
    "fg_att": pl.Int64,
    "fg_made": pl.Int64,
    "exp_made": pl.Float64,
    "fgoe": pl.Float64,
    "fgoe_per_att": pl.Float64,
    "fgoe_shrunk": pl.Float64,
    "rating": pl.Float64,
}

_ERA_COLS: List[str] = ["era0", "era1", "era2", "era3", "era4"]


def _with_era_and_roof(pbp: pl.DataFrame) -> pl.DataFrame:
    """Ensure fg_roof + era0..era4 columns exist (derive from roof/season)."""
    df = pbp
    if "fg_roof" not in df.columns:
        df = df.with_columns((pl.col("roof") == "outdoors").cast(pl.Float64).fill_null(0.0).alias("fg_roof"))
    if "era4" not in df.columns:
        s = pl.col("season")
        df = df.with_columns(
            (s <= 2001).cast(pl.Float64).alias("era0"),
            ((s > 2001) & (s <= 2005)).cast(pl.Float64).alias("era1"),
            ((s > 2005) & (s <= 2013)).cast(pl.Float64).alias("era2"),
            ((s > 2013) & (s <= 2017)).cast(pl.Float64).alias("era3"),
            (s > 2017).cast(pl.Float64).alias("era4"),
        )
    return df


def env_adjusted_make_prob(pbp: pl.DataFrame) -> pl.DataFrame:
    """Add ``base_make_prob`` + environment-adjusted ``exp_make_prob``.

    ``exp_make_prob = sigmoid(logit(base) + b_wind*wind + b_temp*(temp-baseline)
    + b_alt*altitude_kft)`` with coefficients from
    :data:`sportsdataverse.nfl.nfl_scheme_constants.ENVIRONMENT_FG_COEF` and
    altitude from ``STADIUM_ALTITUDE[home_team]``.  Dome / closed-roof kicks
    (and missing readings) are treated as neutral (wind 0, temp = baseline).

    Args:
        pbp: FG-attempt rows with ``yardline_100`` / ``roof`` / ``temp`` /
            ``wind`` / ``home_team`` (+ ``season`` or ``era0..era4`` /
            ``fg_roof``).

    Returns:
        The input plus ``base_make_prob`` and ``exp_make_prob`` (Float64).

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nfl.nfl_kicker_rating import env_adjusted_make_prob
            fg = pl.read_parquet("tests/fixtures/nfl_scheme/fg_attempts_2019_2023.parquet")
            out = env_adjusted_make_prob(fg)
            print(out.select("base_make_prob", "exp_make_prob").describe())
    """
    if pbp.height == 0:
        return pbp.with_columns(
            pl.lit(None, dtype=pl.Float64).alias("base_make_prob"),
            pl.lit(None, dtype=pl.Float64).alias("exp_make_prob"),
        )
    df = _with_era_and_roof(pbp)
    assert df.schema["home_team"] == pl.Utf8

    base = fg_make_probability(
        df["yardline_100"].to_numpy().astype(float),
        df["fg_roof"].to_numpy().astype(float),
        df.select(_ERA_COLS).to_numpy().astype(float),
    )
    if base is None:  # pragma: no cover - depends on bundling
        raise FileNotFoundError("bundled fg_model unavailable")
    base = np.clip(base, 1e-6, 1.0 - 1e-6)

    coef = ENVIRONMENT_FG_COEF
    indoor = df["roof"].is_in(["dome", "closed"]).fill_null(False).to_numpy()
    wind = np.where(indoor, 0.0, df["wind"].fill_null(0.0).to_numpy().astype(float))
    temp_raw = df["temp"].fill_null(coef["temp_baseline"]).to_numpy().astype(float)
    temp = np.where(indoor, coef["temp_baseline"], temp_raw) - coef["temp_baseline"]
    alt_kft = (
        df["home_team"].replace_strict(STADIUM_ALTITUDE, default=0.0, return_dtype=pl.Float64).to_numpy().astype(float)
        / 1000.0
    )
    z = logit(base) + coef["wind"] * wind + coef["temp"] * temp + coef["altitude_kft"] * alt_kft
    return df.with_columns(
        pl.Series("base_make_prob", base, dtype=pl.Float64),
        pl.Series("exp_make_prob", expit(z).astype(float), dtype=pl.Float64),
    )


def _kicker_rating_from(kicks: pl.DataFrame) -> pl.DataFrame:
    """Aggregate per-kick expected makes to per (season, kicker) FGOE ratings.

    ``kicks`` must carry ``season`` / ``kicker_player_id`` /
    ``kicker_player_name`` / ``posteam`` / ``exp_make_prob`` / ``made``.
    """
    if kicks.height == 0:
        return pl.DataFrame(schema=_RATING_SCHEMA)
    k_fg = EB_PRIOR["K_fg"]
    out = (
        kicks.filter(pl.col("kicker_player_id").is_not_null())
        .group_by("season", "kicker_player_id")
        .agg(
            pl.col("kicker_player_name").drop_nulls().first().alias("kicker"),
            pl.col("posteam").drop_nulls().last().alias("team"),
            pl.len().cast(pl.Int64).alias("fg_att"),
            pl.col("made").sum().cast(pl.Int64).alias("fg_made"),
            pl.col("exp_make_prob").sum().alias("exp_made"),
        )
        .with_columns((pl.col("fg_made") - pl.col("exp_made")).alias("fgoe"))
        .with_columns((pl.col("fgoe") / pl.col("fg_att")).alias("fgoe_per_att"))
        .with_columns((pl.col("fgoe_per_att") * pl.col("fg_att") / (pl.col("fg_att") + k_fg)).alias("fgoe_shrunk"))
    )
    mu = out["fgoe_shrunk"].mean()
    sd = out["fgoe_shrunk"].std()
    if sd is None or sd == 0.0:
        out = out.with_columns(pl.lit(100.0).alias("rating"))
    else:
        out = out.with_columns((100.0 + 15.0 * (pl.col("fgoe_shrunk") - mu) / sd).alias("rating"))
    return (
        out.with_columns(pl.col("season").cast(pl.Int64), pl.col("kicker_player_id").cast(pl.Utf8))
        .select(list(_RATING_SCHEMA.keys()))
        .sort("season", "rating", descending=[False, True])
    )


def nfl_kicker_rating(
    seasons: Union[int, List[int]],
    *,
    as_of: Optional[Tuple[int, int]] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Environment-adjusted kicker FG-over-expected ratings.

    Loads pbp FG attempts for ``seasons``, computes the environment-adjusted
    expected make probability per kick, and aggregates to per
    ``(season, kicker)`` FGOE (raw + EB-shrunk with the fitted ``K_fg``).

    Args:
        seasons: Season or list of seasons.
        as_of: Optional ``(season, week)``; uses only kicks strictly before
            that point (the as-of leakage boundary for mid-season ratings).
        return_as_pandas: When ``True``, return a ``pandas.DataFrame``.

    Returns:
        Per ``(season, kicker_player_id)``: ``kicker``, ``team``, ``fg_att``,
        ``fg_made``, ``exp_made``, ``fgoe``, ``fgoe_per_att``,
        ``fgoe_shrunk``, ``rating`` (100 +/- 15 z of ``fgoe_shrunk``).
        Empty seasons yield a zero-row frame with this schema.

    Example:
        Quick start::

            from sportsdataverse.nfl.nfl_kicker_rating import nfl_kicker_rating
            r = nfl_kicker_rating([2023])
            print(r.head())

        Mid-season as-of rating::

            r = nfl_kicker_rating([2023], as_of=(2023, 10))

        See Also:
            * `nflfastR`_ -- source pbp columns (kick distance, roof, weather).

        .. _nflfastR: https://www.nflfastr.com
    """
    from sportsdataverse.nfl.nfl_loaders import load_nfl_pbp

    season_list = [seasons] if isinstance(seasons, int) else list(seasons)
    if not season_list:
        out: pl.DataFrame = pl.DataFrame(schema=_RATING_SCHEMA)
        return out.to_pandas() if return_as_pandas else out
    pbp = load_nfl_pbp(season_list)
    fg = pbp.filter(pl.col("play_type") == "field_goal")
    if as_of is not None:
        fg = as_of_split(fg, season=as_of[0], week=as_of[1])
    fg = env_adjusted_make_prob(fg).with_columns((pl.col("field_goal_result") == "made").cast(pl.Int64).alias("made"))
    out = _kicker_rating_from(fg)
    return out.to_pandas() if return_as_pandas else out
