"""NHL/PWHL expected primary/secondary assists (T5.2 model 3).

Credits each goal's assisters an expected-assist share weighted by the goal
shot's xG (a playmaker who sets up high-danger goals accrues more), split
primary (``assist1``) vs secondary (``assist2``). ``assists_above_expected``
is actual assists minus that xG-weighted expectation. The on-demand shot-xG
model (:func:`~sportsdataverse.nhl.nhl_microstat_constants.fit_shot_xg`) is
fit at call time unless one is injected via ``xg_model=`` (offline tests).

Example:
    Quick start::

        from sportsdataverse.nhl.nhl_expected_assists import nhl_expected_assists

        out = nhl_expected_assists(pbp)
        print(out.sort("assists_above_expected", descending=True).head())

See Also:
    * `nhl-api-py`_ -- Python NHL API client (companion data source).

.. _nhl-api-py: https://github.com/coreyjs/nhl-api-py
"""

from __future__ import annotations

from typing import Literal, overload

import pandas as pd
import polars as pl

from sportsdataverse.nhl.nhl_microstat_constants import ShotXGModel, fit_shot_xg

GOALS_SCHEMA = {
    "game_id": pl.Utf8,
    "scoring_player_id": pl.Utf8,
    "assist1_player_id": pl.Utf8,
    "assist2_player_id": pl.Utf8,
    "goal_xg": pl.Float64,
}

VALUE_SCHEMA = {
    "player_id": pl.Utf8,
    "primary_assists": pl.Int64,
    "secondary_assists": pl.Int64,
    "x_primary_assists": pl.Float64,
    "x_secondary_assists": pl.Float64,
    "assists_above_expected": pl.Float64,
    "primary_share": pl.Float64,
}


def extract_goals_with_assists(pbp: pl.DataFrame, *, xg_model: ShotXGModel | None = None) -> pl.DataFrame:
    """Extract one row per goal with its assisters and the goal-shot xG.

    Args:
        pbp: Parsed pbp frame (Task-0.1 contract).
        xg_model: A fitted :class:`~sportsdataverse.nhl.nhl_microstat_constants.ShotXGModel`.
            When ``None``, one is fit on ``pbp`` via ``fit_shot_xg``.

    Returns:
        One row per goal: ``game_id``, ``scoring_player_id``,
        ``assist1_player_id``, ``assist2_player_id``, ``goal_xg``. Zero-row
        input (or a frame missing ``type_desc_key``) returns a zero-row
        frame with this schema.

    Example:
        Quick start::

            from sportsdataverse.nhl.nhl_expected_assists import extract_goals_with_assists

            goals = extract_goals_with_assists(pbp)
    """
    if pbp.height == 0 or "type_desc_key" not in pbp.columns:
        return pl.DataFrame(schema=GOALS_SCHEMA)

    goals = pbp.filter(pl.col("type_desc_key") == "goal")
    if goals.height == 0:
        return pl.DataFrame(schema=GOALS_SCHEMA)

    model = xg_model if xg_model is not None else fit_shot_xg(pbp)
    xg = model.predict(goals)
    goals = goals.with_columns(xg.alias("goal_xg"))
    return goals.select(
        pl.col("game_id").cast(pl.Utf8),
        pl.col("scoring_player_id").cast(pl.Utf8),
        pl.col("assist1_player_id").cast(pl.Utf8),
        pl.col("assist2_player_id").cast(pl.Utf8),
        pl.col("goal_xg").cast(pl.Float64),
    )


@overload
def nhl_expected_assists(
    pbp: pl.DataFrame,
    *,
    league: str = ...,
    xg_model: ShotXGModel | None = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...
@overload
def nhl_expected_assists(
    pbp: pl.DataFrame,
    *,
    league: str = ...,
    xg_model: ShotXGModel | None = ...,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...
def nhl_expected_assists(
    pbp: pl.DataFrame,
    *,
    league: str = "nhl",
    xg_model: ShotXGModel | None = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Per-player expected primary/secondary assists from xG-weighted goal credit.

    Each goal credits its ``assist1`` player its **relative danger**
    ``goal_xg / mean_goal_xg`` as x_primary (and ``assist2`` likewise as
    x_secondary). Normalizing to the league-mean goal xG is what makes the
    total credit **unbiased** -- ``Sum(x_primary + x_secondary) ~= Sum(actual
    assists)`` -- while still rewarding a playmaker who sets up high-danger
    goals (relative danger > 1) over one who feeds tap-ins (< 1). Crediting
    raw ``goal_xg`` (~0.1-0.2) instead would put expected assists on the xG
    scale, an order of magnitude below the assist count, and could never be
    unbiased against actual assists.
    ``assists_above_expected = (primary + secondary) - (x_primary + x_secondary)``
    (positive = the player's assisted goals were lower-danger than average, so
    they out-assisted their shot quality); ``primary_share = primary /
    (primary + secondary)``.

    Args:
        pbp: Parsed pbp frame (Task-0.1 contract).
        league: League key (unused today -- assist credit is league-agnostic;
            kept for signature parity with the other microstat models and the
            PWHL shim).
        xg_model: A fitted :class:`~sportsdataverse.nhl.nhl_microstat_constants.ShotXGModel`;
            fit on ``pbp`` when ``None``.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        Per-player frame: ``player_id``, ``primary_assists``,
        ``secondary_assists``, ``x_primary_assists``, ``x_secondary_assists``,
        ``assists_above_expected``, ``primary_share``. Zero-row input returns a
        zero-row frame with this schema.

    Example:
        Quick start::

            from sportsdataverse.nhl.nhl_expected_assists import nhl_expected_assists

            out = nhl_expected_assists(pbp)

        PWHL::

            out_pwhl = nhl_expected_assists(pwhl_pbp, league="pwhl")

    See Also:
        * `nhl-api-py`_ -- Python NHL API client (companion data source).

    .. _nhl-api-py: https://github.com/coreyjs/nhl-api-py
    """
    del league  # league-agnostic credit; parameter kept for signature parity
    goals = extract_goals_with_assists(pbp, xg_model=xg_model)
    if goals.height == 0:
        empty = pl.DataFrame(schema=VALUE_SCHEMA)
        return empty.to_pandas() if return_as_pandas else empty

    # Relative danger = goal_xg / league-mean goal xG (see docstring): the
    # normalization that makes total credit unbiased vs actual assist counts.
    mean_goal_xg = goals["goal_xg"].mean()
    if mean_goal_xg is None or mean_goal_xg == 0:
        goals = goals.with_columns(pl.lit(1.0).alias("goal_xg"))
    else:
        goals = goals.with_columns((pl.col("goal_xg") / mean_goal_xg).alias("goal_xg"))

    primary = goals.select(
        pl.col("assist1_player_id").alias("player_id"),
        pl.lit(1).alias("primary_assists"),
        pl.lit(0).alias("secondary_assists"),
        pl.col("goal_xg").alias("x_primary_assists"),
        pl.lit(0.0).alias("x_secondary_assists"),
    ).filter(pl.col("player_id").is_not_null())
    secondary = goals.select(
        pl.col("assist2_player_id").alias("player_id"),
        pl.lit(0).alias("primary_assists"),
        pl.lit(1).alias("secondary_assists"),
        pl.lit(0.0).alias("x_primary_assists"),
        pl.col("goal_xg").alias("x_secondary_assists"),
    ).filter(pl.col("player_id").is_not_null())

    combined = pl.concat([primary, secondary], how="vertical_relaxed")
    agg = combined.group_by("player_id").agg(
        pl.col("primary_assists").sum(),
        pl.col("secondary_assists").sum(),
        pl.col("x_primary_assists").sum(),
        pl.col("x_secondary_assists").sum(),
    )
    agg = agg.with_columns(
        (
            (pl.col("primary_assists") + pl.col("secondary_assists"))
            - (pl.col("x_primary_assists") + pl.col("x_secondary_assists"))
        ).alias("assists_above_expected"),
        (pl.col("primary_assists") / (pl.col("primary_assists") + pl.col("secondary_assists")).cast(pl.Float64)).alias(
            "primary_share"
        ),
    )
    out = agg.select(
        pl.col("player_id").cast(pl.Utf8),
        pl.col("primary_assists").cast(pl.Int64),
        pl.col("secondary_assists").cast(pl.Int64),
        pl.col("x_primary_assists").cast(pl.Float64),
        pl.col("x_secondary_assists").cast(pl.Float64),
        pl.col("assists_above_expected").cast(pl.Float64),
        pl.col("primary_share").cast(pl.Float64),
    )
    return out.to_pandas() if return_as_pandas else out
