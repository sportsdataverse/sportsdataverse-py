"""Cricket in-play win probability (T7.3 model ①).

Format-aware (T20 / ODI) probability that the batting/chasing team wins, computed
from over-level match state and backed by a resource surface + calibration
lookup fitted on real Cricsheet ball-by-ball data (male T20I + ODI, 2002-2026;
see ``tools/models/league_ports/fit_cricket_resource_surface.py``).

Pipeline for each state row:

0. ``regime = cricket_regime(fmt, match_date)`` — the ODI fielding-restriction
   regime of the match (``rules/cricket.yaml``: 2005 powerplays, the 2011 window,
   2012 two new balls / four out, 2015 batting powerplay removed; T20I is one
   regime, no date is the pooled corpus). A regime is applied whole or not at
   all: its own grid AND its fitted par/sigma (``REGIME_CONSTANTS``, via
   ``get_format(fmt, regime)``). A regime whose grid has fewer than
   :data:`MIN_REGIME_CELLS` cells with at least :data:`MIN_CELL_SUPPORT` states,
   or that has no fitted constants (the fit emits them only for 100+ matches;
   the 13-month ``odi_2011`` window has none), uses the pooled 2002-2026 surface
   and constants.
1. ``resources_left = resource(fmt, regime, overs_left, wickets_left)`` — bundled
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
balls_total, target, fmt, match_date``; ``match_date`` may be absent or null).
"""

from __future__ import annotations

import datetime as dt
import functools
import importlib.resources as ir
import re
from typing import TYPE_CHECKING, Any

import numpy as np
import polars as pl
from scipy.stats import norm

from sportsdataverse._temporal import as_date
from sportsdataverse.cricket.cricket_model_constants import (
    ODI_REGIMES,
    POOLED_REGIME,
    REGIME_CONSTANTS,
    T20_REGIME,
    get_format,
)

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
    "match_date": pl.Date,
}

#: A regime's own resource grid is used only when at least this many of its cells hold
#: :data:`MIN_CELL_SUPPORT` or more innings-states; otherwise the pooled surface is used.
#: (An ODI grid has 51 x 11 = 561 cells, a T20 grid 21 x 11 = 231.)
MIN_REGIME_CELLS = 100
#: Innings-states a cell needs to count as supported.
MIN_CELL_SUPPORT = 30

# ESPN cricket score strings look like ``"161/5 (18/20 ov, target 156)"`` or
# ``"88/3 (12.4/20 ov)"``. No regex lookaround (Rust/polars unsupported); the
# inline ``(?i)`` folds case. Groups: runs, wickets?, overs, partial-ball?,
# overs-limit? (the DLS-reduced allocation when present), target?.
_SCORE_RE = re.compile(
    r"(?i)(\d+)(?:/(\d+))?\s*\(\s*(\d+)(?:\.(\d))?(?:\s*/\s*(\d+))?\s*ov(?:er)?s?(?:,\s*target\s*(\d+))?\s*\)"
)


def _parse_score_string(score: object) -> tuple[int, int, int, int | None, int | None] | None:
    """Parse an ESPN cricket score string to ``(runs, wickets, balls, target, balls_total)``.

    Args:
        score: A cricket score string, e.g. ``"161/5 (18/20 ov, target 156)"``.

    Returns:
        ``(runs, wickets, balls_bowled, target, balls_total)`` where
        ``balls_bowled`` is ``overs * 6 + partial_ball``, ``target`` is ``None``
        in the first innings, and ``balls_total`` is the over allocation after
        the slash times six (``None`` when the string carries none). A
        DLS-reduced innings shows its REVISED allocation, e.g.
        ``"88/3 (12.4/15 ov)"`` -> 90, not the format's 120
        (rules/cricket.yaml#cricket-1999-duckworth-lewis). ``None`` if the
        string does not parse.

    Example:
        Quick start::

            from sportsdataverse.cricket.cricket_win_prob import _parse_score_string
            _parse_score_string("161/5 (18/20 ov, target 156)")  # (161, 5, 108, 156, 120)
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
    balls_total = int(m.group(5)) * 6 if m.group(5) is not None else None
    target = int(m.group(6)) if m.group(6) is not None else None
    return runs, wickets, overs * 6 + partial, target, balls_total


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


def _match_date(summary: Any, comp: dict) -> dt.date | None:
    """The match date from the competition (or header) ``date`` (``"2026-05-31T14:00Z"``)."""
    header = (summary or {}).get("header") if isinstance(summary, dict) else None
    for raw in (comp.get("date"), (summary or {}).get("date"), (header or {}).get("date")):
        if isinstance(raw, str) and len(raw) >= 10:
            try:
                return dt.date.fromisoformat(raw[:10])
            except ValueError:
                continue
    return None


def _empty_state(return_as_pandas: bool) -> pl.DataFrame | pd.DataFrame:
    out = pl.DataFrame(schema=STATE_SCHEMA)
    return out.to_pandas() if return_as_pandas else out


def cricket_match_state(summary: dict, *, fmt: str, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """Extract over-level match state from an ESPN cricket summary/scoreboard payload.

    One row per innings with a parseable competitor score string. The batting
    side that carries a ``target`` in its score is the second innings (chasing);
    the other is the first innings (setting). ``match_date`` is the competition's
    ``date`` (null when the payload has none); :func:`cricket_win_probability`
    selects the ODI fielding-restriction regime from it.

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
        runs, wickets, balls, target, balls_total = parsed
        team_id = (c.get("team") or {}).get("id") if isinstance(c.get("team"), dict) else c.get("id")
        rows.append(
            {
                "event_id": None if event_id is None else str(event_id),
                "innings_number": 2 if target is not None else 1,
                "batting_team_id": None if team_id is None else str(team_id),
                "runs": runs,
                "wickets": wickets,
                "balls_bowled": balls,
                # the score string's own allocation wins over the format default: a DLS-reduced
                # innings must not keep the full 120/300-ball budget against its revised target
                "balls_total": fc.balls_total if balls_total is None else balls_total,
                "target": target,
                "fmt": fc.name,
                "match_date": _match_date(summary, comp),
            }
        )
    if not rows:
        return _empty_state(return_as_pandas)
    out = pl.DataFrame(rows, schema=STATE_SCHEMA).sort("innings_number")
    return out.to_pandas() if return_as_pandas else out


@functools.lru_cache(maxsize=1)
def _load_surface_regimes() -> pl.DataFrame:
    """The bundled surface, every regime: ``fmt, regime, overs_left, wickets_left, resource, support``.

    ``support`` is the innings-states behind a cell (null for a regime row that is a copy of
    the pooled surface, which the lookup therefore never selects).
    """
    p = ir.files("sportsdataverse.cricket.models") / "cricket_resource_surface.parquet"
    with ir.as_file(p) as fp:
        return pl.read_parquet(fp)


@functools.lru_cache(maxsize=1)
def _load_surface() -> pl.DataFrame:
    """The pooled 2002-2026 surface: ``fmt, overs_left, wickets_left, resource``."""
    return (
        _load_surface_regimes()
        .filter(pl.col("regime") == POOLED_REGIME)
        .select("fmt", "overs_left", "wickets_left", "resource")
    )


def _usable_regimes(surface: pl.DataFrame, constants: dict[str, dict[str, float]] | None = None) -> set[str]:
    """Regimes applied whole: a thick grid (:data:`MIN_REGIME_CELLS` cells with
    :data:`MIN_CELL_SUPPORT`+ states) AND fitted constants (``constants``, default
    :data:`~sportsdataverse.cricket.cricket_model_constants.REGIME_CONSTANTS`; the T20
    regime is the pooled T20 fit and needs none). Anything else falls back to the pooled
    surface and constants, so a regime's grid and its par/sigma never come from
    different corpora."""
    fitted = REGIME_CONSTANTS if constants is None else constants
    counts = (
        surface.filter(pl.col("regime") != POOLED_REGIME)
        .group_by("regime")
        .agg((pl.col("support").fill_null(0) >= MIN_CELL_SUPPORT).sum().alias("cells"))
        .filter(pl.col("cells") >= MIN_REGIME_CELLS)
    )
    return {r for r in counts["regime"].to_list() if r == T20_REGIME or r in fitted}


def _regime_expr() -> pl.Expr:
    """``cricket_regime(fmt, match_date)`` as a polars expression."""
    odi = pl.lit(ODI_REGIMES[0][0])
    for name, start in ODI_REGIMES[1:]:
        odi = pl.when(pl.col("match_date") >= pl.lit(start)).then(pl.lit(name)).otherwise(odi)
    return (
        pl.when(pl.col("fmt") == T20_REGIME)
        .then(pl.lit(T20_REGIME))
        .when(pl.col("match_date").is_null())
        .then(pl.lit(POOLED_REGIME))
        .otherwise(odi)
    )


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
    "regime": pl.Utf8,
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
            balls_bowled, balls_total, target, fmt, match_date``) — e.g. the output
            of :func:`cricket_match_state`. ``match_date`` (Date, or a parseable
            string) selects the ODI regime; absent or null means the pooled surface.
        return_as_pandas: When True, return a :class:`pandas.DataFrame`.

    Returns:
        The input rows plus ``regime:Utf8`` (the match's regime id,
        :func:`~sportsdataverse.cricket.cricket_model_constants.cricket_regime`),
        ``overs_left:Int64``, ``wickets_left:Int64``,
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

    surf_all = _load_surface_regimes()
    calib = _load_calibration()

    # validates fmt for every distinct format present, incl. "test"
    fmts_present = state["fmt"].unique().to_list()
    for f in fmts_present:
        get_format(f)

    if "match_date" not in state.columns:
        df = state.with_columns(pl.lit(None, pl.Date).alias("match_date"))
    elif state.schema["match_date"] != pl.Date:
        df = state.with_columns(as_date(pl.col("match_date")).alias("match_date"))
    else:
        df = state
    usable = _usable_regimes(surf_all)
    df = df.with_columns(regime=_regime_expr()).with_columns(
        surface_regime=pl.when(pl.col("regime").is_in(list(usable)) if usable else pl.lit(False))
        .then(pl.col("regime"))
        .otherwise(pl.lit(POOLED_REGIME))
    )
    # par / sigma per (fmt, regime): the regime's constants only where the regime is applied whole
    consts = {
        (f, r): get_format(f, r if r in usable else None)
        for f, r in sorted(set(zip(df["fmt"].to_list(), df["regime"].to_list())))
    }
    par_expr = pl.lit(None, pl.Float64)
    for (f, r), fc in consts.items():
        par_expr = (
            pl.when((pl.col("fmt") == f) & (pl.col("regime") == r)).then(pl.lit(fc.par_score)).otherwise(par_expr)
        )
    par_expr = par_expr.cast(pl.Float64)

    surf = surf_all.rename({"regime": "surface_regime"}).select(
        "fmt", "surface_regime", "overs_left", "wickets_left", "resource"
    )
    df = df.with_columns(
        overs_left=((pl.col("balls_total") - pl.col("balls_bowled")) // 6).cast(pl.Int64),
        wickets_left=(pl.lit(10) - pl.col("wickets")).cast(pl.Int64),
    )
    keys = ["fmt", "surface_regime", "overs_left", "wickets_left"]
    for k in keys:  # join-key dtype agreement before the surface join
        assert df.schema[k] == surf.schema[k], (
            f"surface join-key dtype mismatch on {k}: {df.schema[k]} != {surf.schema[k]}"
        )
    df = df.join(surf, on=keys, how="left").drop("surface_regime")
    df = df.with_columns(resources_left=pl.col("resource").fill_null(0.0).cast(pl.Float64)).drop("resource")
    df = df.with_columns(proj_final=(pl.col("runs") + pl.col("resources_left") * par_expr).cast(pl.Float64))
    is_chase = (pl.col("innings_number") == 2) & pl.col("target").is_not_null()
    df = df.with_columns(
        phase=pl.when(is_chase).then(pl.lit("chase")).otherwise(pl.lit("set")),
        benchmark=pl.when(is_chase).then(pl.col("target").cast(pl.Float64)).otherwise(par_expr),
    ).with_columns(z=(pl.col("proj_final") - pl.col("benchmark")))

    fmts = df["fmt"].to_numpy()
    regimes = df["regime"].to_numpy()
    phases = df["phase"].to_numpy()
    z = df["z"].to_numpy().astype(float)
    raw = np.full(df.height, np.nan)
    winp = np.full(df.height, np.nan)
    for (f, rg), fc in consts.items():
        for phase, sigma in (("set", fc.sigma_set), ("chase", fc.sigma_chase)):
            m = (fmts == f) & (regimes == rg) & (phases == phase)
            if not m.any():
                continue
            r = norm.cdf(z[m] / float(sigma))
            raw[m] = r
            cx, cy = calib[(f, phase)]  # ponytail: the isotonic recalibration stays pooled per (fmt, phase)
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
