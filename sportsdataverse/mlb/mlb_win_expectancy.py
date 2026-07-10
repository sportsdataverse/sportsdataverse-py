"""MLB win-expectancy / WPA / leverage index (T6.4, model ②).

Built on the same base-out-state substrate as RE24
(:func:`sportsdataverse.mlb.mlb_run_expectancy.pbp_base_out_states`):
an empirical win-expectancy table keyed on
``(inning_capped, half, base_state, outs_start, score_diff_bucket)``,
per-play win-probability-added, and the empirical (Tango) leverage
index.

See Also:
    * `baseballr`_ -- R sibling package for MLB sabermetrics.
    * Tango, Lichtman & Dolphin, *The Book* (2007) -- the empirical
      leverage-index definition this module implements.

    .. _baseballr: https://baseballr.sportsdataverse.org
"""

from __future__ import annotations

from typing import Union

import pandas as pd
import polars as pl

from sportsdataverse.mlb.mlb_run_expectancy import pbp_base_out_states

_WE_KEY = ["inning_capped", "half", "base_state", "outs_start", "score_diff_bucket"]
_WE_TABLE_SCHEMA = {
    "inning_capped": pl.Int64,
    "half": pl.Utf8,
    "base_state": pl.Utf8,
    "outs_start": pl.Int64,
    "score_diff_bucket": pl.Int64,
    "home_win_exp": pl.Float64,
    "n": pl.Int64,
}
_WE_SCHEMA = {"game_id": pl.Utf8, "at_bat_index": pl.Int64, "half": pl.Utf8, "home_win_exp": pl.Float64}
_WPA_SCHEMA = {"game_id": pl.Utf8, "at_bat_index": pl.Int64, "wpa": pl.Float64}
_LI_SCHEMA = {**{k: v for k, v in _WE_TABLE_SCHEMA.items() if k != "home_win_exp"}, "leverage_index": pl.Float64}


def _bucket(states: pl.DataFrame) -> pl.DataFrame:
    return states.with_columns(
        pl.col("inning").clip(upper_bound=9).alias("inning_capped"),
        pl.col("score_diff").clip(-6, 6).alias("score_diff_bucket"),
    )


def build_we_table(states: pl.DataFrame, results: pl.DataFrame, *, laplace: float = 1.0) -> pl.DataFrame:
    """Empirical, Laplace-smoothed home win-expectancy table.

    Args:
        states: Output of :func:`pbp_base_out_states`.
        results: Game-level results with ``game_id`` (same dtype as
            ``states``), ``home_score``, ``away_score``.
        laplace: Additive smoothing constant (default 1.0).

    Returns:
        pl.DataFrame: one row per observed state bucket.

        | Column | Type | Description |
        |---|---|---|
        | inning_capped | Int64 | Inning, capped at 9 |
        | half | Utf8 | ``"top"`` or ``"bottom"`` |
        | base_state | Utf8 | 3-char base occupancy |
        | outs_start | Int64 | Outs before the play (0-2) |
        | score_diff_bucket | Int64 | home - away score, clipped to [-6, 6] |
        | home_win_exp | Float64 | Laplace-smoothed P(home wins \\| state) |
        | n | Int64 | Plate appearances observed in this bucket |

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_run_expectancy import pbp_base_out_states
            from sportsdataverse.mlb.mlb_win_expectancy import build_we_table
            states = pbp_base_out_states(pbp)
            table = build_we_table(states, results)
    """
    if states.height == 0 or results.height == 0:
        return pl.DataFrame(schema=_WE_TABLE_SCHEMA)
    assert states.schema["game_id"] == results.schema["game_id"], (
        f"game_id dtype mismatch: states={states.schema['game_id']} results={results.schema['game_id']}"
    )
    home_won = results.select("game_id", (pl.col("home_score") > pl.col("away_score")).cast(pl.Int64).alias("home_won"))
    joined = _bucket(states).join(home_won, on="game_id", how="inner")
    assert joined.height > 0, "no states matched a result row -- check game_id join key"
    return (
        joined.group_by(_WE_KEY)
        .agg(pl.col("home_won").sum().alias("wins"), pl.len().alias("n"))
        .with_columns(((pl.col("wins") + laplace * 0.5) / (pl.col("n") + laplace)).alias("home_win_exp"))
        .select(*_WE_KEY, "home_win_exp", "n")
    )


def _sparse_fallback(score_diff_bucket: pl.Expr) -> pl.Expr:
    """Monotone-in-score-diff logistic prior for state buckets with zero observations."""
    return (0.5 + 0.5 * (score_diff_bucket.cast(pl.Float64) * 0.4).tanh()).clip(0.0, 1.0)


def _lookup_we(states: pl.DataFrame, we_table: pl.DataFrame) -> pl.DataFrame:
    """Left-join each play's state onto ``we_table``, falling back to the sparse-cell prior."""
    assert states.schema["base_state"] == we_table.schema["base_state"], (
        f"base_state dtype mismatch: states={states.schema['base_state']} we_table={we_table.schema['base_state']}"
    )
    bucketed = _bucket(states)
    joined = bucketed.join(we_table.select(*_WE_KEY, "home_win_exp"), on=_WE_KEY, how="left")
    return joined.with_columns(
        pl.when(pl.col("home_win_exp").is_null())
        .then(_sparse_fallback(pl.col("score_diff_bucket")))
        .otherwise(pl.col("home_win_exp"))
        .alias("home_win_exp")
    )


def mlb_win_expectancy(
    pbp: pl.DataFrame,
    results: pl.DataFrame,
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Per-play home win expectancy from the empirical state table.

    Args:
        pbp: Parsed ``mlb_play_by_play`` frame (see :func:`sportsdataverse.mlb.mlb_run_expectancy.pbp_base_out_states`).
        results: Game-level results (``game_id``, ``home_score``, ``away_score``).
        return_as_pandas: Return ``pandas.DataFrame`` instead of polars.

    Returns:
        pl.DataFrame: one row per plate appearance, **plus one terminal
        "game over" row per game** (``at_bat_index`` = last real PA's index
        + 1, ``home_win_exp`` pinned to the actual final outcome: 1.0 if
        home won, 0.0 otherwise). Without this anchor, the last real play's
        own WPA swing (e.g. a walk-off) would never be captured by
        :func:`mlb_win_probability_added`'s per-game diff, and the
        game-level WPA sum would not telescope to the exact +-0.5 identity.

        | Column | Type | Description |
        |---|---|---|
        | game_id | Utf8 | Game identifier |
        | at_bat_index | Int64 | Game-global sequential PA index (last row is a synthetic terminal marker) |
        | half | Utf8 | ``"top"`` or ``"bottom"`` (offense side); the terminal row repeats the last real half |
        | home_win_exp | Float64 | P(home team wins \\| state before the play); 1.0/0.0 on the terminal row |

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_win_expectancy import mlb_win_expectancy
            we = mlb_win_expectancy(pbp, results)

        Pipeline next step (one line)::

            we.filter(pl.col("game_id") == "716390").sort("at_bat_index")
    """
    states = pbp_base_out_states(pbp)
    if states.height == 0:
        out = pl.DataFrame(schema=_WE_SCHEMA)
        return out.to_pandas() if return_as_pandas else out
    table = build_we_table(states, results)
    looked = _lookup_we(states, table)
    out = looked.select("game_id", "at_bat_index", "half", "home_win_exp")

    assert out.schema["game_id"] == results.schema["game_id"], (
        f"game_id dtype mismatch: we={out.schema['game_id']} results={results.schema['game_id']}"
    )
    last = out.group_by("game_id").agg(
        pl.col("at_bat_index").max().alias("at_bat_index"), pl.col("half").last().alias("half")
    )
    terminal = last.join(
        results.select("game_id", (pl.col("home_score") > pl.col("away_score")).cast(pl.Float64).alias("home_win_exp")),
        on="game_id",
        how="inner",
    ).with_columns((pl.col("at_bat_index") + 1).alias("at_bat_index"))
    out = pl.concat([out, terminal.select("game_id", "at_bat_index", "half", "home_win_exp")], how="vertical")
    return out.to_pandas() if return_as_pandas else out


def mlb_win_probability_added(
    we: pl.DataFrame,
    *,
    perspective: str = "home",
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Per-play win-probability-added from a :func:`mlb_win_expectancy` frame.

    ``wpa_i = home_win_exp_i - home_win_exp_{i-1}`` within each game (the
    first play of a game is measured against the neutral 0.5 baseline).

    Args:
        we: Output of :func:`mlb_win_expectancy` (needs ``game_id``,
            ``at_bat_index``, ``home_win_exp``).
        perspective: ``"home"`` (default) returns home-team WPA; any
            other value (e.g. ``"away"``) returns the sign-flipped
            (away-team) WPA.
        return_as_pandas: Return ``pandas.DataFrame`` instead of polars.

    Returns:
        pl.DataFrame: one row per plate appearance.

        | Column | Type | Description |
        |---|---|---|
        | game_id | Utf8 | Game identifier |
        | at_bat_index | Int64 | Game-global sequential PA index |
        | wpa | Float64 | Win-probability added, from ``perspective`` |

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_win_expectancy import mlb_win_probability_added
            wpa = mlb_win_probability_added(we)
    """
    if we.height == 0:
        out = pl.DataFrame(schema=_WPA_SCHEMA)
        return out.to_pandas() if return_as_pandas else out
    sign = 1.0 if perspective == "home" else -1.0
    out = we.sort(["game_id", "at_bat_index"]).with_columns(
        (sign * (pl.col("home_win_exp") - pl.col("home_win_exp").shift(1, fill_value=0.5).over("game_id"))).alias("wpa")
    )
    out = out.select("game_id", "at_bat_index", "wpa")
    return out.to_pandas() if return_as_pandas else out


def leverage_index(
    states: pl.DataFrame,
    we_table: pl.DataFrame,
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Empirical (Tango) leverage index per base-out-score-inning state bucket.

    ``leverage_index(s) = E[|delta WE|] over the empirical next-state
    distribution from ``s``, divided by the league-average of that
    quantity (so the average state has ``leverage_index == 1``).

    Args:
        states: Output of :func:`sportsdataverse.mlb.mlb_run_expectancy.pbp_base_out_states`.
        we_table: Output of :func:`build_we_table`.
        return_as_pandas: Return ``pandas.DataFrame`` instead of polars.

    Returns:
        pl.DataFrame: one row per observed state bucket.

        | Column | Type | Description |
        |---|---|---|
        | inning_capped | Int64 | Inning, capped at 9 |
        | half | Utf8 | ``"top"`` or ``"bottom"`` |
        | base_state | Utf8 | 3-char base occupancy |
        | outs_start | Int64 | Outs before the play (0-2) |
        | score_diff_bucket | Int64 | home - away score, clipped to [-6, 6] |
        | leverage_index | Float64 | Mean \\|delta WE\\| from this state, normalized to a 1.0 league average |

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_win_expectancy import build_we_table, leverage_index
            table = build_we_table(states, results)
            li = leverage_index(states, table)
    """
    if states.height == 0 or we_table.height == 0:
        return pl.DataFrame(schema=_LI_SCHEMA)
    looked = _lookup_we(states, we_table).sort(["game_id", "at_bat_index"])
    looked = looked.with_columns(
        (pl.col("home_win_exp") - pl.col("home_win_exp").shift(1, fill_value=0.5).over("game_id"))
        .abs()
        .alias("abs_wpa")
    )
    global_mean = looked["abs_wpa"].mean() or 1.0
    out = (
        looked.group_by(_WE_KEY)
        .agg((pl.col("abs_wpa").mean() / global_mean).alias("leverage_index"))
        .select(*_WE_KEY, "leverage_index")
    )
    return out.to_pandas() if return_as_pandas else out
