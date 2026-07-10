"""MLB RE24 run-expectancy matrix (T6.4, model ①).

Owns the base-out-state substrate extraction (:func:`pbp_base_out_states`,
shared with the win-expectancy model in :mod:`mlb_win_expectancy`), the
empirical RE24 matrix (:func:`mlb_run_expectancy_matrix`), and the
:func:`run_value` transition helper -- the **exported run-value
denominator** the sibling Statcast spines (pitching/hitting/fielding)
import.

See Also:
    * `baseballr`_ -- R sibling package for MLB sabermetrics.
    * Tango, Lichtman & Dolphin, *The Book: Playing the Percentages in
      Baseball* (2007) -- source of the published RE24 methodology and
      reference table this module's oracle gate validates against.

    .. _baseballr: https://baseballr.sportsdataverse.org
"""

from __future__ import annotations

from typing import List, Optional, Union

import pandas as pd
import polars as pl

_STATES_SCHEMA = {
    "game_id": pl.Utf8,
    "inning": pl.Int64,
    "half": pl.Utf8,
    "at_bat_index": pl.Int64,
    "base_state": pl.Utf8,
    "outs_start": pl.Int64,
    "runs_on_play": pl.Int64,
    "runs_rest_of_inning": pl.Int64,
    "score_diff": pl.Int64,
}

_MATRIX_SCHEMA = {"base_state": pl.Utf8, "outs": pl.Int64, "re": pl.Float64, "n": pl.Int64}


def _occ(col: str) -> pl.Expr:
    return (pl.col(col).is_not_null()).cast(pl.Int8)


def pbp_base_out_states(pbp: pl.DataFrame) -> pl.DataFrame:
    """Reconstruct pre-play base-out state from statsapi play-by-play.

    Within each ``(game_id, inning, half)`` half-inning, ordered by the
    game-global ``at_bat_index``: ``base_state``/``outs_start`` before PA
    *i* are the post-occupancy / out-count of PA *i-1* (empty/0 at the
    half's first PA -- occupancy and outs both genuinely reset at every
    half-inning boundary). ``runs_on_play`` is the score delta since the
    *previous PA in the game* (``over("game_id")``, **not** reset per
    half-inning -- the score itself carries across the half-inning
    boundary even though outs/bases do not).  ``runs_rest_of_inning`` is
    the suffix-sum of ``runs_on_play`` within the half.

    Args:
        pbp: Parsed ``mlb_play_by_play`` frame (optionally concatenated
            across games), carrying ``game_id``, ``about_inning``,
            ``about_half_inning``, ``about_at_bat_index``, ``count_outs``,
            ``result_home_score``, ``result_away_score``,
            ``matchup_post_on_{first,second,third}_id``.

    Returns:
        pl.DataFrame: one row per plate appearance.

        | Column | Type | Description |
        |---|---|---|
        | game_id | Utf8 | Game identifier |
        | inning | Int64 | Inning number |
        | half | Utf8 | ``"top"`` or ``"bottom"`` |
        | at_bat_index | Int64 | Game-global sequential PA index |
        | base_state | Utf8 | 3-char occupancy before the PA (``"1_3"`` etc.) |
        | outs_start | Int64 | Outs before the PA (0-2) |
        | runs_on_play | Int64 | Runs scored on this PA |
        | runs_rest_of_inning | Int64 | Runs scored from this PA through the half's end |
        | score_diff | Int64 | home - away score at the start of the PA |

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_run_expectancy import pbp_base_out_states
            states = pbp_base_out_states(pbp)
    """
    if pbp is None or pbp.height == 0:
        return pl.DataFrame(schema=_STATES_SCHEMA)

    df = pbp.with_columns(
        pl.col("game_id").cast(pl.Utf8),
        pl.col("about_inning").cast(pl.Int64).alias("inning"),
        pl.col("about_half_inning").cast(pl.Utf8).alias("half"),
        pl.col("about_at_bat_index").cast(pl.Int64).alias("at_bat_index"),
        pl.col("count_outs").cast(pl.Int64),
        pl.col("result_home_score").cast(pl.Int64),
        pl.col("result_away_score").cast(pl.Int64),
        _occ("matchup_post_on_first_id").alias("post_1"),
        _occ("matchup_post_on_second_id").alias("post_2"),
        _occ("matchup_post_on_third_id").alias("post_3"),
    ).sort(["game_id", "at_bat_index"])

    half_grp = ["game_id", "inning", "half"]
    total = pl.col("result_home_score") + pl.col("result_away_score")
    df = df.with_columns(
        pl.col("post_1").shift(1, fill_value=0).over(half_grp).alias("pre_1"),
        pl.col("post_2").shift(1, fill_value=0).over(half_grp).alias("pre_2"),
        pl.col("post_3").shift(1, fill_value=0).over(half_grp).alias("pre_3"),
        pl.col("count_outs").shift(1, fill_value=0).over(half_grp).alias("outs_start"),
        # Score carries across half-inning boundaries -- shift over the WHOLE
        # game, not the half-inning group (unlike bases/outs, which reset).
        (total - total.shift(1, fill_value=0).over("game_id")).alias("runs_on_play"),
        (pl.col("result_home_score") - pl.col("result_away_score")).alias("score_diff"),
    )
    df = df.with_columns(
        (
            pl.col("pre_1").cast(pl.Utf8).str.replace("0", "_")
            + pl.col("pre_2").cast(pl.Utf8).str.replace("1", "2").str.replace("0", "_")
            + pl.col("pre_3").cast(pl.Utf8).str.replace("1", "3").str.replace("0", "_")
        ).alias("base_state"),
        # Suffix sum of runs_on_play within the half: total - inclusive-cumsum + self.
        (
            pl.col("runs_on_play").sum().over(half_grp)
            - pl.col("runs_on_play").cum_sum().over(half_grp)
            + pl.col("runs_on_play")
        ).alias("runs_rest_of_inning"),
    )
    return df.select(list(_STATES_SCHEMA.keys()))


def _season_game_pks(seasons: Union[int, List[int], None]) -> List[int]:
    """Regular-season ``gamePk`` list for one or more seasons via ``mlb_schedule``."""
    from sportsdataverse.mlb.mlb_api_extra import mlb_schedule

    season_list = [seasons] if isinstance(seasons, int) else list(seasons or [])
    pks: List[int] = []
    for season in season_list:
        raw = mlb_schedule(season=season, game_type="R")
        for date_entry in raw.get("dates") or []:
            for game in date_entry.get("games") or []:
                if (game.get("status") or {}).get("codedGameState") == "F":
                    pks.append(int(game["gamePk"]))
    return pks


def mlb_run_expectancy_matrix(
    seasons: Union[int, List[int], None] = None,
    *,
    pbp: Optional[pl.DataFrame] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Empirical RE24 run-expectancy matrix by base-out state.

    ``re[base_state, outs] = mean(runs_rest_of_inning)`` over all plate
    appearances starting in that state, excluding the bottom of the 9th
    inning and beyond (the standard RE24 exclusion -- those half-innings
    are only played while the home team trails or is tied, a
    score-differential selection bias that would otherwise distort the
    matrix). Computed on demand from statsapi play-by-play; **no bundled
    artifact**.

    Args:
        seasons: One season (int) or a list of seasons to collect via
            :func:`sportsdataverse.mlb.mlb_api_extra.mlb_schedule`. Ignored
            when ``pbp`` is supplied.
        pbp: Pre-collected parsed play-by-play frame (skips the network
            collector -- primarily for tests / offline reuse).
        return_as_pandas: Return ``pandas.DataFrame`` instead of polars.

    Returns:
        pl.DataFrame: up to 24 rows (base_state x outs).

        | Column | Type | Description |
        |---|---|---|
        | base_state | Utf8 | 3-char base occupancy (e.g. ``"1_3"``) |
        | outs | Int64 | Outs at the start of the state (0-2) |
        | re | Float64 | Mean runs scored through the end of the half-inning |
        | n | Int64 | Number of plate appearances observed in this state |

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_run_expectancy import mlb_run_expectancy_matrix
            matrix = mlb_run_expectancy_matrix(pbp=pbp)

        Pipeline next step (one line)::

            matrix.filter(pl.col("base_state") == "___").sort("outs")

    See Also:
        * `baseballr`_ -- R sibling package for MLB sabermetrics.
        * Tango, Lichtman & Dolphin, *The Book* (2007) -- the RE24 methodology.

        .. _baseballr: https://baseballr.sportsdataverse.org
    """
    if pbp is None:
        from sportsdataverse.mlb.mlb_game_state_constants import collect_statsapi_pbp

        pbp = collect_statsapi_pbp(_season_game_pks(seasons))
    states = pbp_base_out_states(pbp)
    if states.height == 0:
        out = pl.DataFrame(schema=_MATRIX_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    eligible = states.filter((pl.col("outs_start") < 3) & ~((pl.col("half") == "bottom") & (pl.col("inning") >= 9)))
    out = (
        eligible.group_by("base_state", "outs_start")
        .agg(pl.col("runs_rest_of_inning").mean().alias("re"), pl.len().alias("n"))
        .rename({"outs_start": "outs"})
        .sort("outs", "base_state")
        .select("base_state", "outs", "re", "n")
    )
    return out.to_pandas() if return_as_pandas else out


def run_value(
    before_state: str,
    before_outs: int,
    after_state: str,
    after_outs: int,
    runs_on_play: int,
    matrix: pl.DataFrame,
) -> float:
    """Run value of a single event: ``re[after] - re[before] + runs_on_play``.

    The **exported denominator** the sibling Statcast spines (T6.1
    pitching, T6.2 hitting, T6.3 fielding/baserunning) call to score
    individual plays against this spine's RE24 matrix.

    Args:
        before_state: 3-char base occupancy before the event.
        before_outs: Outs before the event (0-2); ``>= 3`` treated as 0 RE.
        after_state: 3-char base occupancy after the event.
        after_outs: Outs after the event; ``>= 3`` (inning over) treated as 0 RE.
        runs_on_play: Runs scored on the event.
        matrix: An :func:`mlb_run_expectancy_matrix` output (or any frame
            with matching ``base_state``/``outs``/``re`` columns).

    Returns:
        float: the run value of the event.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_run_expectancy import run_value, mlb_run_expectancy_matrix
            matrix = mlb_run_expectancy_matrix(pbp=pbp)
            rv = run_value("___", 0, "1__", 0, 0, matrix)
    """
    lut = {(r["base_state"], r["outs"]): r["re"] for r in matrix.to_dicts()}
    re_before = 0.0 if before_outs >= 3 else lut.get((before_state, before_outs), 0.0)
    re_after = 0.0 if after_outs >= 3 else lut.get((after_state, after_outs), 0.0)
    return float(re_after - re_before + runs_on_play)
