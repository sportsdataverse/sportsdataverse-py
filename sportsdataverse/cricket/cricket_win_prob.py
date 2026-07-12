"""Cricket in-play win probability (T7.3 model ①).

Format-aware (T20 / ODI) probability that the batting/chasing team wins, computed
from over-level match state and backed by a resource surface + calibration
lookup fitted on real Cricsheet ball-by-ball data (male T20I + ODI, 2002-2026;
see ``dev/league_ports/fit_cricket_resource_surface.py``).

Pipeline for each state row:

1. ``resources_left = resource(fmt, overs_left, wickets_left)`` — bundled
   Duckworth/Lewis-style surface (fraction of the innings total still to come).
2. ``proj_final = runs + resources_left * par_score`` — an unbiased, par-centred
   projection of the innings total (a par-pace team lands on ``par_score``).
3. ``win_prob_raw = Phi((proj_final - benchmark) / sigma)`` — the parametric core
   (``benchmark`` is the chase ``target`` in the 2nd innings, ``par_score`` in the
   1st; ``sigma`` is the fitted per-phase probit scale).
4. ``win_prob`` — ``win_prob_raw`` mapped through the bundled per-``(fmt, phase)``
   isotonic calibration lookup so the output matches the empirical win rate
   (a scalar probit alone is ~0.07 per-decile off; the recalibration brings it
   under the 0.05 gate). Monotone non-decreasing in ``win_prob_raw``, hence in
   runs at fixed balls/wickets.

State frames come from either :func:`cricket_match_state` (the live ESPN
scoreboard/summary path) or any frame carrying the documented state schema
(``event_id, innings_number, batting_team_id, runs, wickets, balls_bowled,
balls_total, target, fmt``).
"""

from __future__ import annotations

import functools
import importlib.resources as ir
import re
from typing import TYPE_CHECKING, Any

import numpy as np
import polars as pl
from scipy.stats import norm

from sportsdataverse.cricket.cricket_model_constants import get_format

if TYPE_CHECKING:
    import pandas as pd

# Documented match-state schema (input to cricket_win_probability).
STATE_SCHEMA: dict[str, pl.DataType] = {
    "event_id": pl.Utf8,
    "innings_number": pl.Int64,
    "batting_team_id": pl.Utf8,
    "runs": pl.Int64,
    "wickets": pl.Int64,
    "balls_bowled": pl.Int64,
    "balls_total": pl.Int64,
    "target": pl.Int64,
    "fmt": pl.Utf8,
}

# ESPN cricket score strings look like ``"161/5 (18/20 ov, target 156)"`` or
# ``"88/3 (12.4/20 ov)"``. No regex lookaround (Rust/polars unsupported); the
# inline ``(?i)`` folds case. Groups: runs, wickets?, overs, partial-ball?, target?.
_SCORE_RE = re.compile(
    r"(?i)(\d+)(?:/(\d+))?\s*\(\s*(\d+)(?:\.(\d))?(?:\s*/\s*\d+)?\s*ov(?:er)?s?(?:,\s*target\s*(\d+))?\s*\)"
)


def _parse_score_string(score: object) -> tuple[int, int, int, int | None] | None:
    """Parse an ESPN cricket score string to ``(runs, wickets, balls, target)``.

    Args:
        score: A cricket score string, e.g. ``"161/5 (18/20 ov, target 156)"``.

    Returns:
        ``(runs, wickets, balls_bowled, target)`` where ``balls_bowled`` is
        ``overs * 6 + partial_ball`` and ``target`` is ``None`` in the first
        innings; ``None`` if the string does not parse.

    Example:
        Quick start::

            from sportsdataverse.cricket.cricket_win_prob import _parse_score_string
            _parse_score_string("161/5 (18/20 ov, target 156)")  # (161, 5, 108, 156)
    """
    if not isinstance(score, str):
        return None
    m = _SCORE_RE.search(score)
    if m is None:
        return None
    runs = int(m.group(1))
    wickets = int(m.group(2)) if m.group(2) is not None else 0
    overs = int(m.group(3))
    partial = int(m.group(4)) if m.group(4) is not None else 0
    target = int(m.group(5)) if m.group(5) is not None else None
    return runs, wickets, overs * 6 + partial, target


def _find_competition(summary: Any) -> dict:
    """Locate the competition dict (carrying ``competitors``) in an ESPN payload."""
    if not isinstance(summary, dict):
        return {}
    for path in (("header", "competitions"), ("competitions",)):
        node: Any = summary
        for key in path:
            node = node.get(key) if isinstance(node, dict) else None
        if isinstance(node, list) and node:
            return node[0] if isinstance(node[0], dict) else {}
    return {}


def _event_id(summary: Any, comp: dict) -> str | None:
    for candidate in (comp.get("id"), (summary or {}).get("id"), ((summary or {}).get("header") or {}).get("id")):
        if candidate is not None:
            return str(candidate)
    return None


def _empty_state(return_as_pandas: bool) -> pl.DataFrame | pd.DataFrame:
    out = pl.DataFrame(schema=STATE_SCHEMA)
    return out.to_pandas() if return_as_pandas else out


def cricket_match_state(summary: dict, *, fmt: str, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """Extract over-level match state from an ESPN cricket summary/scoreboard payload.

    One row per innings with a parseable competitor score string. The batting
    side that carries a ``target`` in its score is the second innings (chasing);
    the other is the first innings (setting).

    Args:
        summary: Raw ESPN cricket ``summary`` or ``scoreboard`` payload (dict).
        fmt: Format slug (``"t20"`` / ``"odi"``); validated via
            :func:`~sportsdataverse.cricket.cricket_model_constants.get_format`.
        return_as_pandas: When True, return a :class:`pandas.DataFrame`.

    Returns:
        A :class:`polars.DataFrame` (or pandas) with the documented state schema;
        a zero-row frame when the payload is empty/malformed.

    Raises:
        ValueError: If ``fmt`` is ``"test"`` (Test deferred) or unknown.

    Example:
        Quick start::

            from sportsdataverse.cricket import espn_cricket_summary
            from sportsdataverse.cricket.cricket_win_prob import cricket_match_state
            state = cricket_match_state(espn_cricket_summary(event="1385691", return_parsed=False), fmt="t20")
            print(state.shape)
    """
    fc = get_format(fmt)
    comp = _find_competition(summary)
    event_id = _event_id(summary, comp)
    rows: list[dict] = []
    for c in comp.get("competitors", []) or []:
        parsed = _parse_score_string(c.get("score")) if isinstance(c, dict) else None
        if parsed is None:
            continue
        runs, wickets, balls, target = parsed
        team_id = (c.get("team") or {}).get("id") if isinstance(c.get("team"), dict) else c.get("id")
        rows.append(
            {
                "event_id": None if event_id is None else str(event_id),
                "innings_number": 2 if target is not None else 1,
                "batting_team_id": None if team_id is None else str(team_id),
                "runs": runs,
                "wickets": wickets,
                "balls_bowled": balls,
                "balls_total": fc.balls_total,
                "target": target,
                "fmt": fc.name,
            }
        )
    if not rows:
        return _empty_state(return_as_pandas)
    out = pl.DataFrame(rows, schema=STATE_SCHEMA).sort("innings_number")
    return out.to_pandas() if return_as_pandas else out


@functools.lru_cache(maxsize=1)
def _load_surface() -> pl.DataFrame:
    p = ir.files("sportsdataverse.cricket.models") / "cricket_resource_surface.parquet"
    with ir.as_file(p) as fp:
        return pl.read_parquet(fp)


@functools.lru_cache(maxsize=1)
def _load_calibration() -> dict[tuple[str, str], tuple[np.ndarray, np.ndarray]]:
    p = ir.files("sportsdataverse.cricket.models") / "cricket_winprob_calibration.parquet"
    with ir.as_file(p) as fp:
        cal = pl.read_parquet(fp)
    out: dict[tuple[str, str], tuple[np.ndarray, np.ndarray]] = {}
    for (fmt, phase), grp in cal.sort("x").group_by(["fmt", "phase"]):
        out[(str(fmt), str(phase))] = (grp["x"].to_numpy(), grp["y"].to_numpy())
    return out


_WP_ADDED: dict[str, pl.DataType] = {
    "overs_left": pl.Int64,
    "wickets_left": pl.Int64,
    "resources_left": pl.Float64,
    "proj_final": pl.Float64,
    "win_prob_raw": pl.Float64,
    "win_prob": pl.Float64,
}


def cricket_win_probability(state: pl.DataFrame, *, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """In-play win probability for the batting/chasing team from match state.

    Args:
        state: Over-level match state carrying the documented state schema
            (``event_id, innings_number, batting_team_id, runs, wickets,
            balls_bowled, balls_total, target, fmt``) — e.g. the output of
            :func:`cricket_match_state`.
        return_as_pandas: When True, return a :class:`pandas.DataFrame`.

    Returns:
        The input rows plus ``overs_left:Int64``, ``wickets_left:Int64``,
        ``resources_left:Float64``, ``proj_final:Float64``,
        ``win_prob_raw:Float64`` (parametric core) and ``win_prob:Float64``
        (calibrated, the shipped estimate). A zero-row input returns the schema
        with these columns appended (all null).

    Raises:
        ValueError: If a row's ``fmt`` is ``"test"`` (deferred) or unknown.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.cricket.cricket_win_prob import cricket_win_probability
            st = pl.DataFrame([{ "event_id": "M1", "innings_number": 2,
                "batting_team_id": "A", "runs": 120, "wickets": 3,
                "balls_bowled": 90, "balls_total": 120, "target": 160, "fmt": "t20"}])
            cricket_win_probability(st).select("win_prob").item()
    """
    if state.height == 0:
        out = state.with_columns([pl.lit(None, dtype).alias(name) for name, dtype in _WP_ADDED.items()])
        return out.to_pandas() if return_as_pandas else out

    surf = _load_surface()
    calib = _load_calibration()

    # par per fmt (validates fmt for every distinct format present, incl. "test").
    fmts_present = state["fmt"].unique().to_list()
    par_map = {f: float(get_format(f).par_score) for f in fmts_present}
    par_expr = pl.col("fmt").replace_strict(par_map, default=None).cast(pl.Float64)

    df = state.with_columns(
        overs_left=((pl.col("balls_total") - pl.col("balls_bowled")) // 6).cast(pl.Int64),
        wickets_left=(pl.lit(10) - pl.col("wickets")).cast(pl.Int64),
    ).join(surf, on=["fmt", "overs_left", "wickets_left"], how="left")
    df = df.with_columns(resources_left=pl.col("resource").fill_null(0.0).cast(pl.Float64)).drop("resource")
    df = df.with_columns(proj_final=(pl.col("runs") + pl.col("resources_left") * par_expr).cast(pl.Float64))
    is_chase = (pl.col("innings_number") == 2) & pl.col("target").is_not_null()
    df = df.with_columns(
        phase=pl.when(is_chase).then(pl.lit("chase")).otherwise(pl.lit("set")),
        benchmark=pl.when(is_chase).then(pl.col("target").cast(pl.Float64)).otherwise(par_expr),
    ).with_columns(z=(pl.col("proj_final") - pl.col("benchmark")))

    fmts = df["fmt"].to_numpy()
    phases = df["phase"].to_numpy()
    z = df["z"].to_numpy().astype(float)
    raw = np.full(df.height, np.nan)
    winp = np.full(df.height, np.nan)
    for f in fmts_present:
        fc = get_format(f)
        for phase, sigma in (("set", fc.sigma_set), ("chase", fc.sigma_chase)):
            m = (fmts == f) & (phases == phase)
            if not m.any():
                continue
            r = norm.cdf(z[m] / float(sigma))
            raw[m] = r
            cx, cy = calib[(f, phase)]
            winp[m] = np.interp(r, cx, cy)
    # tiny raw tie-breaker: preserves calibration (shift <= 5e-7) but keeps
    # win_prob strictly increasing wherever win_prob_raw is (isotonic plateaus).
    winp = np.clip(winp + 1e-6 * (raw - 0.5), 0.0, 1.0)

    out = df.drop("phase", "benchmark", "z").with_columns(
        win_prob_raw=pl.Series("win_prob_raw", raw, dtype=pl.Float64),
        win_prob=pl.Series("win_prob", winp, dtype=pl.Float64),
    )
    # Terminal-state overrides — the smooth model never sees post-completion
    # states (a chase ends the instant the target is reached / resources run out),
    # so pin the certain outcomes: chase reached target => 1.0; chase all out or
    # out of balls short of the target => 0.0.
    chase = (pl.col("innings_number") == 2) & pl.col("target").is_not_null()
    reached = chase & (pl.col("runs") >= pl.col("target"))
    exhausted = (
        chase
        & (pl.col("runs") < pl.col("target"))
        & ((pl.col("wickets") >= 10) | (pl.col("balls_bowled") >= pl.col("balls_total")))
    )
    out = out.with_columns(
        win_prob=pl.when(reached).then(1.0).when(exhausted).then(0.0).otherwise(pl.col("win_prob")),
        win_prob_raw=pl.when(reached).then(1.0).when(exhausted).then(0.0).otherwise(pl.col("win_prob_raw")),
    )
    return out.to_pandas() if return_as_pandas else out
