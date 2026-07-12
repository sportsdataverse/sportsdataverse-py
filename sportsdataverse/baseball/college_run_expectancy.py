"""College baseball/softball RE24 run-expectancy + WPA (T7.3, model 5).

League-agnostic core, ported by reference from
:mod:`sportsdataverse.mlb.mlb_run_expectancy` / ``mlb_win_expectancy`` (T6.4)
rather than re-implementing the RE24/WE math: the empirical-mean-runs-to-end-
of-half methodology, the 3-char base-occupancy encoding, and the win-
expectancy bucketing/sparse-cell fallback are the exact same algorithms,
fed by a college-specific base-out-state substrate built from ESPN's Core v2
``.../plays`` payload instead of MLB statsapi.

**Substrate difference from MLB (why this module owns its own state
reconstruction):** ESPN's play-by-play ships ``participants[]`` as a
variable-length list of ``{athlete, type}`` dicts (``type`` in
``{pitcher, batter, onFirst, onSecond, onThird}``) on each ``"Play Result"``
row -- the post-play base occupancy. The project's generic cross-league
parser stringifies list-valued cells so a heterogeneous-length list survives
a polars frame; that would destroy the onFirst/onSecond/onThird structure
this reconstruction needs. :func:`college_baseball_state` therefore consumes
the **raw** payload dict (``espn_college_{baseball,softball}_game_plays(...,
return_parsed=False)``) directly rather than a pre-parsed frame, avoiding a
lossy stringify/de-stringify round trip.

See Also:
    * `baseballr`_ -- R sibling package for MLB/college sabermetrics.
    * Tango, Lichtman & Dolphin, *The Book: Playing the Percentages in
      Baseball* (2007) -- source of the RE24 methodology this module reuses.

    .. _baseballr: https://baseballr.sportsdataverse.org
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import polars as pl

from sportsdataverse.baseball.college_baseball_constants import get_college_baseball_constants
from sportsdataverse.mlb.mlb_run_expectancy import run_value as _mlb_run_value
from sportsdataverse.mlb.mlb_win_expectancy import (
    _lookup_we,  # deliberate cross-module reuse of the sparse-cell fallback lookup -- see module docstring
    build_we_table,
    mlb_win_probability_added,
)

_STATE_SCHEMA = {
    "game_id": pl.Utf8,
    "inning": pl.Int64,
    "half": pl.Utf8,
    "base_state": pl.Utf8,
    "outs": pl.Int64,
    "runs_before": pl.Int64,
    "runs_after": pl.Int64,
    "batting_team_id": pl.Utf8,
    "play_seq": pl.Int64,
    "score_diff": pl.Int64,
}
_MATRIX_SCHEMA = {"base_state": pl.Utf8, "outs": pl.Int64, "run_expectancy": pl.Float64, "n": pl.Int64}
_WPA_SCHEMA = {
    "game_id": pl.Utf8,
    "play_seq": pl.Int64,
    "re_before": pl.Float64,
    "re_after": pl.Float64,
    "run_value": pl.Float64,
    "wpa": pl.Float64,
}

_EVENT_ID_RE = re.compile(r"events/(\d+)")
_TEAM_ID_RE = re.compile(r"teams/(\d+)")


def _extract_game_id(raw: Dict[str, Any]) -> Optional[str]:
    m = _EVENT_ID_RE.search((raw or {}).get("$ref") or "")
    return m.group(1) if m else None


def run_value(
    before_state: str,
    before_outs: int,
    after_state: str,
    after_outs: int,
    runs_on_play: int,
    matrix: pl.DataFrame,
) -> float:
    """Run value of a single event: ``re[after] - re[before] + runs_on_play``.

    Delegates to :func:`sportsdataverse.mlb.mlb_run_expectancy.run_value`
    (the RE24 math is reused, not re-implemented); the only added behavior
    is renaming this module's ``run_expectancy`` matrix column to the MLB
    function's expected ``re`` so a fitted :func:`college_baseball_re24`
    output works directly.

    Args:
        before_state: 3-char base occupancy before the event.
        before_outs: Outs before the event (0-2); ``>= 3`` treated as 0 RE.
        after_state: 3-char base occupancy after the event.
        after_outs: Outs after the event; ``>= 3`` (inning over) treated as 0 RE.
        runs_on_play: Runs scored on the event.
        matrix: A :func:`college_baseball_re24` output (or any frame with
            ``base_state``/``outs`` and ``run_expectancy`` or ``re`` columns).

    Returns:
        float: the run value of the event.

    Example:
        Quick start::

            from sportsdataverse.baseball.college_run_expectancy import college_baseball_re24, run_value
            matrix = college_baseball_re24(league="college_baseball", state=state)
            rv = run_value("___", 0, "1__", 0, 0, matrix)
    """
    if "run_expectancy" in matrix.columns:
        matrix = matrix.rename({"run_expectancy": "re"})
    return _mlb_run_value(before_state, before_outs, after_state, after_outs, runs_on_play, matrix)


def college_baseball_state(plays: Dict[str, Any], *, league: str) -> pl.DataFrame:
    """Reconstruct pre-play base-out state from an ESPN college game_plays payload.

    Within each ``(game_id, inning, half)`` half-inning, ordered by numeric
    ``atBatId`` (assumed monotone in true game order, as observed in the
    ESPN captures): ``base_state``/``outs`` before PA *i* are the
    post-occupancy / out-count of PA *i-1* (empty/0 outs at the half's first
    PA). ``runs_before``/``runs_after`` are the game's cumulative (both
    teams) run total immediately before/after the PA -- the score carries
    across half-inning boundaries even though outs/bases do not, same as
    MLB's ``pbp_base_out_states``. Some ``atBatId`` groups carry more than
    one ``"Play Result"`` row (e.g. a caught-stealing sub-event split from
    the batter's own result); the row with the highest ``(outs,
    sequenceNumber)`` is taken as that PA's authoritative terminal state.

    Args:
        league: ``"college_baseball"`` or ``"college_softball"`` (validates
            against :func:`sportsdataverse.baseball.college_baseball_constants.get_college_baseball_constants`).
        plays: Raw payload from ``espn_college_baseball_game_plays(event_id,
            return_parsed=False)`` / ``espn_college_softball_game_plays(...)``
            -- **not** a pre-parsed DataFrame; see the module docstring for why.

    Returns:
        pl.DataFrame: one row per plate appearance.

        | Column | Type | Description |
        |---|---|---|
        | game_id | Utf8 | ESPN event id |
        | inning | Int64 | Inning number |
        | half | Utf8 | ``"top"`` or ``"bottom"`` |
        | base_state | Utf8 | 3-char occupancy before the PA (``"1_3"`` etc.) |
        | outs | Int64 | Outs before the PA (0-2) |
        | runs_before | Int64 | Cumulative (both teams) runs before the PA |
        | runs_after | Int64 | Cumulative (both teams) runs after the PA |
        | batting_team_id | Utf8 | ESPN team id of the batting team |
        | play_seq | Int64 | Game-global sequential PA order (ordering key; not a plan-schema column, needed for correct within-half suffix sums) |
        | score_diff | Int64 | home - away cumulative score before the PA |

    Example:
        Quick start::

            from sportsdataverse.baseball.college_run_expectancy import college_baseball_state
            raw = espn_college_baseball_game_plays(event_id=401874444, return_parsed=False)
            state = college_baseball_state(raw, league="college_baseball")
    """
    get_college_baseball_constants(league)  # validates league; raises ValueError on unknown
    items: List[Dict[str, Any]] = (plays or {}).get("items") or []
    result_rows = [it for it in items if (it.get("type") or {}).get("text") == "Play Result"]
    if not result_rows:
        return pl.DataFrame(schema=_STATE_SCHEMA)

    game_id = _extract_game_id(plays)

    terminal: Dict[str, Dict[str, Any]] = {}
    terminal_key: Dict[str, tuple] = {}
    for row in result_rows:
        ab = row.get("atBatId")
        if ab is None:
            continue
        key = (int(row.get("outs") or 0), int(row.get("sequenceNumber") or 0))
        if ab not in terminal_key or key > terminal_key[ab]:
            terminal_key[ab] = key
            terminal[ab] = row

    recs = []
    for ab, row in terminal.items():
        period = row.get("period") or {}
        ptype = period.get("type")
        half = "top" if ptype == "Top" else "bottom" if ptype == "Bottom" else None
        if half is None:
            continue
        p_types = {p.get("type") for p in row.get("participants") or []}
        team_id_match = _TEAM_ID_RE.search(((row.get("team") or {}).get("$ref")) or "")
        recs.append(
            {
                "atbat_seq": int(ab),
                "inning": int(period.get("number") or 0),
                "half": half,
                "outs_after": int(row.get("outs") or 0),
                "on_first": "onFirst" in p_types,
                "on_second": "onSecond" in p_types,
                "on_third": "onThird" in p_types,
                "away_score": int(row.get("awayScore") or 0),
                "home_score": int(row.get("homeScore") or 0),
                "batting_team_id": team_id_match.group(1) if team_id_match else None,
            }
        )
    if not recs:
        return pl.DataFrame(schema=_STATE_SCHEMA)

    df = pl.DataFrame(recs).sort("atbat_seq").with_row_index("play_seq")
    half_grp = ["inning", "half"]
    df = df.with_columns(
        pl.col("outs_after").shift(1, fill_value=0).over(half_grp).alias("outs"),
        pl.col("on_first").cast(pl.Int8).shift(1, fill_value=0).over(half_grp).alias("pre_1"),
        pl.col("on_second").cast(pl.Int8).shift(1, fill_value=0).over(half_grp).alias("pre_2"),
        pl.col("on_third").cast(pl.Int8).shift(1, fill_value=0).over(half_grp).alias("pre_3"),
        (pl.col("away_score") + pl.col("home_score")).alias("runs_after"),
        (pl.col("home_score").shift(1, fill_value=0) - pl.col("away_score").shift(1, fill_value=0)).alias("score_diff"),
    )
    df = df.with_columns(
        (pl.col("away_score").shift(1, fill_value=0) + pl.col("home_score").shift(1, fill_value=0)).alias("runs_before")
    )
    df = df.with_columns(
        (
            pl.col("pre_1").cast(pl.Utf8).str.replace("0", "_")
            + pl.col("pre_2").cast(pl.Utf8).str.replace("1", "2").str.replace("0", "_")
            + pl.col("pre_3").cast(pl.Utf8).str.replace("1", "3").str.replace("0", "_")
        ).alias("base_state"),
        pl.lit(game_id).alias("game_id"),
        pl.col("play_seq").cast(pl.Int64),
    )
    return df.select(
        "game_id",
        "inning",
        "half",
        "base_state",
        "outs",
        "runs_before",
        "runs_after",
        "batting_team_id",
        "play_seq",
        "score_diff",
    ).with_columns(pl.col("game_id").cast(pl.Utf8), pl.col("batting_team_id").cast(pl.Utf8))


def college_baseball_re24(
    seasons: Union[int, List[int], None] = None,
    *,
    league: str,
    state: Optional[pl.DataFrame] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Empirical RE24 run-expectancy matrix by base-out state, for one college league.

    ``run_expectancy[base_state, outs] = mean(runs from this state through the
    end of the half-inning)`` over all plate appearances starting in that
    state, excluding the bottom of the league's final regulation inning and
    beyond (:attr:`CollegeBaseballConstants.innings` -- 9 for baseball, 7 for
    softball; same selection-bias exclusion MLB's RE24 applies at inning 9).
    Computed on demand; no bundled artifact.

    Args:
        seasons: One season (int) or a list of seasons. Currently unused --
            see the module docstring; there is no single-call ESPN season
            schedule endpoint for these two leagues the way MLB statsapi has
            one, so this parameter is accepted for interface parity with
            :func:`sportsdataverse.mlb.mlb_run_expectancy.mlb_run_expectancy_matrix`
            but real season collection requires supplying ``state`` from a
            caller-side game loop.
        league: ``"college_baseball"`` or ``"college_softball"``.
        state: Pre-built :func:`college_baseball_state` output (optionally
            ``pl.concat``-ed across many games) -- skips the (currently
            unimplemented) network path; primarily for tests / offline reuse.
        return_as_pandas: Return ``pandas.DataFrame`` instead of polars.

    Returns:
        pl.DataFrame: up to 24 rows (base_state x outs).

        | Column | Type | Description |
        |---|---|---|
        | base_state | Utf8 | 3-char base occupancy (e.g. ``"1_3"``) |
        | outs | Int64 | Outs at the start of the state (0-2) |
        | run_expectancy | Float64 | Mean runs scored through the end of the half-inning |
        | n | Int64 | Number of plate appearances observed in this state |

    Example:
        Quick start::

            from sportsdataverse.baseball.college_run_expectancy import college_baseball_state, college_baseball_re24
            state = college_baseball_state(raw, league="college_baseball")
            matrix = college_baseball_re24(league="college_baseball", state=state)

        Pipeline next step (one line)::

            matrix.filter(pl.col("base_state") == "___").sort("outs")
    """
    constants = get_college_baseball_constants(league)
    if state is None or state.height == 0:
        out = pl.DataFrame(schema=_MATRIX_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    eligible = state.filter(
        (pl.col("outs") < 3) & ~((pl.col("half") == "bottom") & (pl.col("inning") >= constants.innings))
    )
    half_grp = ["game_id", "inning", "half"]
    eligible = eligible.sort(["game_id", "play_seq"]).with_columns(
        (pl.col("runs_after") - pl.col("runs_before")).alias("runs_on_play")
    )
    eligible = eligible.with_columns(
        (
            pl.col("runs_on_play").sum().over(half_grp)
            - pl.col("runs_on_play").cum_sum().over(half_grp)
            + pl.col("runs_on_play")
        ).alias("runs_rest_of_inning")
    )
    out = (
        eligible.group_by("base_state", "outs")
        .agg(
            pl.col("runs_rest_of_inning").mean().cast(pl.Float64).alias("run_expectancy"),
            pl.len().cast(pl.Int64).alias("n"),  # pl.len() emits UInt32; pin to the documented Int64
        )
        .sort("outs", "base_state")
        .select("base_state", "outs", "run_expectancy", "n")
    )
    return out.to_pandas() if return_as_pandas else out


def college_baseball_wpa(
    seasons: Union[int, List[int], None] = None,
    *,
    league: str,
    state: Optional[pl.DataFrame] = None,
    results: Optional[pl.DataFrame] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Per-PA run value + win-probability-added for one college league.

    Reuses the T6.4 machinery by reference: :func:`college_baseball_re24` for
    ``re_before``/``re_after``/``run_value`` (``re_after - re_before +
    runs_on_play`` -- the same formula as :func:`run_value`, computed inline
    (vectorized) over the frame rather than per-event),
    and :func:`sportsdataverse.mlb.mlb_win_expectancy.build_we_table` /
    ``mlb_win_probability_added`` for ``wpa`` -- the empirical win-expectancy
    bucketing, sparse-cell logistic fallback, and per-play diff are the exact
    same algorithm as MLB, re-dimensioned to this league's
    :attr:`CollegeBaseballConstants.innings` only insofar as the RE24
    half-inning exclusion uses it (MLB's win-expectancy inning cap of 9 is
    reused as-is for softball too -- 7-inning games essentially never reach
    it, so forking that cap for a cosmetic edge case isn't worth it).

    Args:
        seasons: Unused -- see :func:`college_baseball_re24`.
        league: ``"college_baseball"`` or ``"college_softball"``.
        state: Pre-built :func:`college_baseball_state` output, primarily for
            tests / offline reuse.
        results: Game-level ``game_id``/``home_score``/``away_score`` (same
            ``game_id`` dtype as ``state``) needed to fit the win-expectancy
            table and pin the per-game terminal WPA anchor. Required
            whenever ``state`` is supplied (the combined-score state frame
            alone can't recover the per-team split needed for win/loss).
        return_as_pandas: Return ``pandas.DataFrame`` instead of polars.

    Returns:
        pl.DataFrame: one row per plate appearance.

        | Column | Type | Description |
        |---|---|---|
        | game_id | Utf8 | ESPN event id |
        | play_seq | Int64 | Game-global sequential PA order |
        | re_before | Float64 | RE24 of the base-out state before the PA |
        | re_after | Float64 | RE24 of the base-out state after the PA |
        | run_value | Float64 | ``re_after - re_before + runs_on_play`` |
        | wpa | Float64 | Home-perspective win-probability added |

    Example:
        Quick start::

            from sportsdataverse.baseball.college_run_expectancy import college_baseball_wpa
            wpa = college_baseball_wpa(league="college_baseball", state=state, results=results)
    """
    get_college_baseball_constants(league)
    if state is None or state.height == 0 or results is None or results.height == 0:
        out = pl.DataFrame(schema=_WPA_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    matrix = college_baseball_re24(league=league, state=state)
    ordered = state.sort(["game_id", "play_seq"]).with_columns(
        (pl.col("runs_after") - pl.col("runs_before")).alias("runs_on_play")
    )
    lut = {(r["base_state"], r["outs"]): r["run_expectancy"] for r in matrix.to_dicts()}

    def _re(base_state: str, outs: int) -> float:
        return 0.0 if outs >= 3 else lut.get((base_state, outs), 0.0)

    rows = ordered.to_dicts()
    re_before = [_re(r["base_state"], r["outs"]) for r in rows]
    # after-state of PA i is the before-state of PA i+1 within the same half;
    # the last PA of a half (or game) has no observed after-state -> 0 RE
    # (inning/game over), matching mlb_run_expectancy.run_value's convention.
    re_after = []
    for i, r in enumerate(rows):
        nxt = rows[i + 1] if i + 1 < len(rows) else None
        if (
            nxt is not None
            and nxt["game_id"] == r["game_id"]
            and nxt["inning"] == r["inning"]
            and nxt["half"] == r["half"]
        ):
            re_after.append(_re(nxt["base_state"], nxt["outs"]))
        else:
            re_after.append(0.0)
    run_val = [ra - rb + r["runs_on_play"] for ra, rb, r in zip(re_after, re_before, rows)]

    out = ordered.with_columns(
        pl.Series("re_before", re_before, dtype=pl.Float64),
        pl.Series("re_after", re_after, dtype=pl.Float64),
        pl.Series("run_value", run_val, dtype=pl.Float64),
    )

    # WE bucketing key names differ from this module's state schema
    # (outs -> outs_start, plus the inning_capped/score_diff_bucket the
    # shared _bucket() derives) -- rename to match build_we_table's contract
    # rather than fork its bucketing logic.
    we_input = out.rename({"outs": "outs_start"}).with_columns(pl.col("play_seq").alias("at_bat_index"))
    table = build_we_table(we_input, results)
    looked = _lookup_we(we_input, table).sort(["game_id", "at_bat_index"])

    # Terminal anchor per game so WPA telescopes to +-0.5 (mirrors
    # mlb_win_expectancy's own terminal-row logic; not reusable verbatim
    # because that function is fused to statsapi-shaped pbp parsing).
    last = looked.group_by("game_id").agg(pl.col("at_bat_index").max().alias("at_bat_index"))
    terminal = last.join(
        results.select("game_id", (pl.col("home_score") > pl.col("away_score")).cast(pl.Float64).alias("home_win_exp")),
        on="game_id",
        how="inner",
    ).with_columns((pl.col("at_bat_index") + 1).alias("at_bat_index"))
    we_full = pl.concat(
        [
            looked.select("game_id", "at_bat_index", "home_win_exp"),
            terminal.select("game_id", "at_bat_index", "home_win_exp"),
        ],
        how="vertical",
    )
    wpa = mlb_win_probability_added(we_full).sort(["game_id", "at_bat_index"])
    # The synthetic terminal row (at_bat_index = last real PA + 1) carries
    # the final jump to the actual 1.0/0.0 outcome; fold its wpa into the
    # last real PA before the play_seq join below (which only spans real
    # PAs) so the game WPA sum genuinely telescopes to final_outcome - 0.5.
    terminal_idx = pl.col("at_bat_index").max().over("game_id")
    wpa = wpa.with_columns(
        pl.when(pl.col("at_bat_index") == terminal_idx - 1)
        .then(pl.col("wpa") + pl.col("wpa").shift(-1, fill_value=0.0).over("game_id"))
        .otherwise(pl.col("wpa"))
        .alias("wpa")
    ).filter(pl.col("at_bat_index") != terminal_idx)

    result = out.join(wpa.rename({"at_bat_index": "play_seq"}), on=["game_id", "play_seq"], how="left").select(
        "game_id", "play_seq", "re_before", "re_after", "run_value", "wpa"
    )
    return result.to_pandas() if return_as_pandas else result
