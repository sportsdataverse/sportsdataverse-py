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

import numpy as np
import pandas as pd
import polars as pl

__all__ = [
    "MLB_PA_EVENT_MAP",
    "MLB_PA_OUTCOMES",
    "mlb_base_out_transitions",
    "mlb_markov_run_expectancy",
    "mlb_run_expectancy_matrix",
    "pbp_base_out_states",
    "run_value",
]

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

_TRANSITIONS_SCHEMA = {
    "from_base_state": pl.Utf8,
    "from_outs": pl.Int64,
    "to_base_state": pl.Utf8,
    "to_outs": pl.Int64,
    "runs": pl.Int64,
    "n": pl.Int64,
    "prob": pl.Float64,
}

#: Plate-appearance outcome classes that ``mlb_base_out_transitions(..., by_event=True)`` keys on.
MLB_PA_OUTCOMES = (
    "out_inplay",
    "so",
    "bb",
    "single",
    "double",
    "triple",
    "hr",
    "reach_other",
    "other_out",
    "gidp",
    "sac_fly",
)

#: statsapi ``result_event_type`` -> :data:`MLB_PA_OUTCOMES` class. A type not listed here falls
#: to ``"out_inplay"`` when the PA made an out and to ``"reach_other"`` otherwise.
MLB_PA_EVENT_MAP = {
    "strikeout": "so",
    "strikeout_double_play": "so",
    "walk": "bb",
    "intent_walk": "bb",
    "hit_by_pitch": "bb",
    "single": "single",
    "double": "double",
    "triple": "triple",
    "home_run": "hr",
    "grounded_into_double_play": "gidp",
    "double_play": "gidp",
    "sac_fly": "sac_fly",
    "sac_fly_double_play": "gidp",
    "field_error": "reach_other",
    "catcher_interf": "reach_other",
    "fielders_choice": "reach_other",
    "caught_stealing_2b": "other_out",
    "caught_stealing_3b": "other_out",
    "caught_stealing_home": "other_out",
    "pickoff_1b": "other_out",
    "pickoff_2b": "other_out",
    "pickoff_3b": "other_out",
    "other_out": "other_out",
}

#: The 24 transient base-out states, in the empirical matrix's row order (outs, then base_state).
_STATE_KEYS = [(b, o) for o in range(3) for b in sorted(("___", "1__", "_2_", "__3", "12_", "1_3", "_23", "123"))]


def _occ(col: str) -> pl.Expr:
    return (pl.col(col).is_not_null()).cast(pl.Int8)


_POST_ON = ("matchup_post_on_first_id", "matchup_post_on_second_id", "matchup_post_on_third_id")
# ``runners`` is the parser's stringified list of runner dicts (repr or JSON quoting).
_FROM_2B = r"""["']originBase["']:\s*["']2B["']"""


def _pre_on_second(columns: List[str], half_grp: List[str]) -> pl.Expr:
    """Pre-PA occupancy of 2nd, seeding the extra-inning automatic runner (MLB, 2020+).

    He starts the half on 2nd without any PA putting him there, so the shifted
    post-occupancy reads empty. He is detected from the half's FIRST PA itself
    rather than a rule-era/inning table: after it a base is held by someone other
    than the batter, or ``runners`` shows a movement from ``originBase`` 2B (he
    scored or was put out on the play). Checked on all 425 April 2023 games: 74 of
    74 extra half-openings flagged, 0 of 7,460 regulation ones (so a pinch runner
    for the leadoff batter does not appear in that PA's post-occupancy). Frames
    without ``matchup_batter_id``/``runners`` keep the empty start; the parser emits both.
    """
    signals = []
    if "matchup_batter_id" in columns:
        batter = pl.col("matchup_batter_id").cast(pl.Int64, strict=False)
        signals += [
            pl.col(c).is_not_null() & (pl.col(c).cast(pl.Int64, strict=False) != batter).fill_null(False)
            for c in _POST_ON
        ]
    if "runners" in columns:
        signals.append(pl.col("runners").cast(pl.Utf8).str.contains(_FROM_2B).fill_null(False))
    placed = pl.any_horizontal(signals) if signals else pl.lit(False)
    first_pa = pl.col("at_bat_index") == pl.col("at_bat_index").min().over(half_grp)
    shifted = pl.col("post_2").shift(1, fill_value=0).over(half_grp)
    return pl.when(first_pa & placed).then(pl.lit(1, dtype=pl.Int8)).otherwise(shifted)


def pbp_base_out_states(pbp: pl.DataFrame) -> pl.DataFrame:
    """Reconstruct pre-play base-out state from statsapi play-by-play.

    Within each ``(game_id, inning, half)`` half-inning, ordered by the
    game-global ``at_bat_index``: ``base_state``/``outs_start`` before PA
    *i* are the post-occupancy / out-count of PA *i-1* (empty/0 at the
    half's first PA -- occupancy and outs both reset at every half-inning
    boundary), except that an extra half-inning's automatic runner starts on
    2nd (``"_2_"``; see ``_pre_on_second``). ``runs_on_play`` is the score delta since the
    *previous PA in the game* (``over("game_id")``, **not** reset per
    half-inning -- the score itself carries across the half-inning
    boundary even though outs/bases do not).  ``runs_rest_of_inning`` is
    the suffix-sum of ``runs_on_play`` within the half. A PA repeated under
    the same ``(game_id, about_at_bat_index)`` (a game collected twice)
    counts once.

    Args:
        pbp: Parsed ``mlb_play_by_play`` frame (optionally concatenated
            across games), carrying ``game_id``, ``about_inning``,
            ``about_half_inning``, ``about_at_bat_index``, ``count_outs``,
            ``result_home_score``, ``result_away_score``,
            ``matchup_post_on_{first,second,third}_id``; optionally
            ``matchup_batter_id`` and ``runners`` (extra-inning runner).

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
    # A game collected twice (listed on two schedule dates) repeats every PA key: count it once.
    pbp = pbp.unique(subset=["game_id", "about_at_bat_index"], keep="first", maintain_order=True)

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
        _pre_on_second(pbp.columns, half_grp).alias("pre_2"),
        pl.col("post_3").shift(1, fill_value=0).over(half_grp).alias("pre_3"),
        pl.col("count_outs").shift(1, fill_value=0).over(half_grp).alias("outs_start"),
        # Score carries across half-inning boundaries -- shift over the WHOLE
        # game, not the half-inning group (unlike bases/outs, which reset).
        (total - total.shift(1, fill_value=0).over("game_id")).alias("runs_on_play"),
        # score_diff is START-OF-PA (pre-play): the statsapi result_*_score are
        # POST-play, so shift them one PA back over the game (0-0 before the
        # first PA). Keeps the "before the play" contract literal -- the WE
        # table keys on all-pre-play features, and the sibling spines read it.
        (
            pl.col("result_home_score").shift(1, fill_value=0).over("game_id")
            - pl.col("result_away_score").shift(1, fill_value=0).over("game_id")
        ).alias("score_diff"),
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
    # A suspended game is listed on both its start and its resume date (e.g. 5028 on 2000-04-12/13).
    return list(dict.fromkeys(pks))


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
        matrix: An :func:`mlb_run_expectancy_matrix` or
            :func:`mlb_markov_run_expectancy` output (or any frame with
            matching ``base_state``/``outs``/``re`` columns).

    Returns:
        float: the run value of the event; NaN when the matrix has a row for
        either state with a null ``re`` (a state the Markov fit never observed).

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_run_expectancy import run_value, mlb_run_expectancy_matrix
            matrix = mlb_run_expectancy_matrix(pbp=pbp)
            rv = run_value("___", 0, "1__", 0, 0, matrix)
    """
    lut = {(r["base_state"], r["outs"]): r["re"] for r in matrix.to_dicts()}
    re_before = 0.0 if before_outs >= 3 else lut.get((before_state, before_outs), 0.0)
    re_after = 0.0 if after_outs >= 3 else lut.get((after_state, after_outs), 0.0)
    if re_before is None or re_after is None:
        return float("nan")
    return float(re_after - re_before + runs_on_play)


def _pa_transitions(pbp: pl.DataFrame, by_event: bool) -> pl.DataFrame:
    """One row per counted PA: from-state -> to-state with ``runs`` (and ``event`` when ``by_event``)."""
    if by_event and "result_event_type" not in pbp.columns:
        raise ValueError("by_event=True needs the pbp's result_event_type column")
    # A game collected twice (statsapi's schedule lists some games twice) repeats every PA key.
    pbp = pbp.unique(subset=["game_id", "about_at_bat_index"], keep="first", maintain_order=True)
    states = pbp_base_out_states(pbp)

    half_grp = ["game_id", "inning", "half"]
    event_type = [pl.col("result_event_type").cast(pl.Utf8).alias("event_type")] if by_event else []
    post = pbp.select(
        pl.col("game_id").cast(pl.Utf8),
        pl.col("about_at_bat_index").cast(pl.Int64).alias("at_bat_index"),
        pl.col("count_outs").cast(pl.Int64).alias("outs_end"),
        *event_type,
    )
    pa = (
        states.join(post, on=["game_id", "at_bat_index"], how="left", validate="1:1")
        .sort("game_id", "at_bat_index")
        .filter(~((pl.col("half") == "bottom") & (pl.col("inning") >= 9)))
        .filter((pl.col("outs_end").last().over(half_grp) == 3) & (pl.col("outs_start").max().over(half_grp) < 3))
        .with_columns(
            pl.col("base_state").shift(-1).over(half_grp).fill_null("___").alias("to_base_state"),
            pl.col("outs_start").shift(-1).over(half_grp).fill_null(3).alias("to_outs"),
        )
        .rename(
            {
                "base_state": "from_base_state",
                "outs_start": "from_outs",
                "runs_on_play": "runs",
            }
        )
    )
    if not by_event:
        return pa
    # An unmapped type (field_out, force_out, sac_bunt, ...) is an out in play when the PA made an out.
    mapped = pl.col("event_type").replace_strict(MLB_PA_EVENT_MAP, default=None, return_dtype=pl.Utf8)
    made_out = pl.col("to_outs") > pl.col("from_outs")
    fallback = pl.when(made_out).then(pl.lit("out_inplay")).otherwise(pl.lit("reach_other"))
    return pa.with_columns(pl.coalesce(mapped, fallback).alias("event"))


def mlb_base_out_transitions(pbp: pl.DataFrame, *, by_event: bool = False) -> pl.DataFrame:
    """Base-out transition counts between consecutive plate appearances.

    Each plate appearance moves its half-inning from its pre-PA state (the
    :func:`pbp_base_out_states` ``base_state``/``outs_start``, so a 2020+
    extra half-inning opens on ``"_2_"`` with the automatic runner) to the
    next PA's pre-PA state, or to the absorbing 3-out state after the half's
    last PA, scoring that PA's ``runs_on_play`` on the way. Rows are counted
    per ``(from, to, runs)`` triple.

    Two kinds of half-inning are left out, so every counted chain is a whole
    one that ends in the third out:

    * the bottom of the 9th and later -- the same exclusion
      :func:`mlb_run_expectancy_matrix` makes, which keeps the two directly
      comparable and drops every walk-off (a walk-off ends before 3 outs, so
      its truncated chain would read as a cheap inning and bias RE down);
    * any other half whose last PA does not leave 3 outs (a 7-inning 2020-21
      doubleheader walk-off, a suspended or shortened game) or whose outs
      read 3 before its last PA.

    Sparse rows are kept as observed: a triple seen once stays with ``n = 1``
    (dropping it would break each state's row sum), and no smoothing is
    applied. Each row carries ``n`` so a caller can judge the support.

    Args:
        pbp: Parsed ``mlb_play_by_play`` frame (see
            :func:`pbp_base_out_states` for the columns read; ``by_event``
            also reads ``result_event_type``).
        by_event: Also key each row on the PA's ``event``: its statsapi
            ``result_event_type`` mapped through ``MLB_PA_EVENT_MAP`` onto
            ``MLB_PA_OUTCOMES``, an unmapped type falling to ``"out_inplay"``
            when the PA made an out and ``"reach_other"`` otherwise. ``prob``
            is then normalized within each ``(from state, event)``; summing
            ``n`` over ``event`` gives the default frame's counts.

    Returns:
        pl.DataFrame: one row per observed ``(from, to, runs)`` triple.

        | Column | Type | Description |
        |---|---|---|
        | from_base_state | Utf8 | Base occupancy before the PA (``"1_3"`` etc.) |
        | from_outs | Int64 | Outs before the PA (0-2) |
        | to_base_state | Utf8 | Base occupancy before the next PA (``"___"`` for the 3-out state) |
        | to_outs | Int64 | Outs before the next PA; 3 = the half-inning ended |
        | runs | Int64 | Runs scored on the transition |
        | n | Int64 | Plate appearances observed with this transition |
        | prob | Float64 | ``n`` over all departures from the from-state (from that (state, event) cell with ``by_event``) |

        With ``by_event=True`` an ``event`` (Utf8) column follows ``from_outs``.

    Raises:
        ValueError: ``by_event=True`` and ``pbp`` has no ``result_event_type`` column.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_run_expectancy import mlb_base_out_transitions
            transitions = mlb_base_out_transitions(pbp)

        Conditioned on the PA outcome::

            by_event = mlb_base_out_transitions(pbp, by_event=True)

        Pipeline next step (one line)::

            transitions.filter((pl.col("from_base_state") == "123") & (pl.col("from_outs") == 0))
    """
    state = ["from_base_state", "from_outs", *(["event"] if by_event else [])]
    if pbp is None or pbp.height == 0:
        head = {
            "from_base_state": pl.Utf8,
            "from_outs": pl.Int64,
            **({"event": pl.Utf8} if by_event else {}),
        }
        return pl.DataFrame(schema={**head, **_TRANSITIONS_SCHEMA})
    out = (
        _pa_transitions(pbp, by_event)
        .group_by(*state, "to_base_state", "to_outs", "runs")
        .agg(pl.len().cast(pl.Int64).alias("n"))
        .with_columns((pl.col("n") / pl.col("n").sum().over(state)).alias("prob"))
        .sort("from_outs", "from_base_state", *state[2:], "to_outs", "to_base_state", "runs")
    )
    return out.select(*state, "to_base_state", "to_outs", "runs", "n", "prob")


def mlb_markov_run_expectancy(transitions: pl.DataFrame) -> pl.DataFrame:
    """Markov-chain RE24: expected runs to the end of the half-inning by base-out state.

    Treats the half-inning as an absorbing Markov chain over the 24 base-out
    states plus the 3-out state. With ``Q`` the 24x24 transient transition
    matrix and ``r`` the expected runs scored on leaving each state, the
    expected runs to absorption are ``(I - Q)^-1 r`` (the fundamental matrix
    times the reward). Probabilities are re-derived from ``n``, so a filtered,
    concatenated or ``by_event=True`` transitions frame still gives stochastic
    rows (the ``event`` key is summed out).

    A state with no observed departures gets ``re = null`` and ``n = 0``,
    never 0 runs. The output is shaped like :func:`mlb_run_expectancy_matrix`
    so the two can be joined on ``base_state``/``outs`` and compared.

    Args:
        transitions: An :func:`mlb_base_out_transitions` output (columns
            ``from_base_state``, ``from_outs``, ``to_base_state``,
            ``to_outs``, ``runs``, ``n``).

    Returns:
        pl.DataFrame: 24 rows (base_state x outs).

        | Column | Type | Description |
        |---|---|---|
        | base_state | Utf8 | 3-char base occupancy (e.g. ``"1_3"``) |
        | outs | Int64 | Outs at the start of the state (0-2) |
        | re | Float64 | Expected runs through the end of the half-inning (null if unobserved) |
        | n | Int64 | Plate appearances observed departing this state |

    Raises:
        ValueError: a row names a state outside the 24 + 3-out set, or leads
            into a state that has no observed departures (its RE is unknown).

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_run_expectancy import (
                mlb_base_out_transitions,
                mlb_markov_run_expectancy,
            )
            re24 = mlb_markov_run_expectancy(mlb_base_out_transitions(pbp))

        Pipeline next step (one line)::

            re24.join(mlb_run_expectancy_matrix(pbp=pbp), on=["base_state", "outs"], suffix="_empirical")

    See Also:
        * Tango, Lichtman & Dolphin, *The Book* (2007) -- the RE24 table.
        * Kemeny & Snell, *Finite Markov Chains* (1960) -- the fundamental matrix of an absorbing chain.
    """
    m = len(_STATE_KEYS)
    idx = {k: i for i, k in enumerate(_STATE_KEYS)}
    counts = np.zeros((m, m + 1))  # column m is the absorbing 3-out state
    run_sum = np.zeros(m)
    cols = ["from_base_state", "from_outs", "to_base_state", "to_outs", "runs", "n"]
    for row in transitions.select(cols).iter_rows():
        fb, fo, tb, to, runs, n = row
        i = idx.get((fb, fo))
        j = m if to is not None and to >= 3 else idx.get((tb, to))
        if i is None or j is None:
            raise ValueError(f"unknown base-out state in transitions row {row}")
        counts[i, j] += n
        run_sum[i] += runs * n

    n_from = counts.sum(axis=1)
    seen = n_from > 0
    if (counts[np.ix_(seen, ~np.append(seen, True))] > 0).any():
        raise ValueError("transitions lead into a base-out state with no observed departures")
    q = counts[np.ix_(seen, np.append(seen, False))] / n_from[seen, None]
    r = run_sum[seen] / n_from[seen]
    re = np.full(m, np.nan)
    re[seen] = np.linalg.solve(np.eye(int(seen.sum())) - q, r)
    return pl.DataFrame(
        {
            "base_state": [b for b, _ in _STATE_KEYS],
            "outs": [o for _, o in _STATE_KEYS],
            "re": re,
            "n": n_from.astype(np.int64),
        },
        schema=_MATRIX_SCHEMA,
    ).with_columns(pl.col("re").fill_nan(None))
