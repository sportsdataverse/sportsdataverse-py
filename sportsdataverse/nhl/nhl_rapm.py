"""Skater xG RAPM -- shift-stint builder + weighted sparse ridge (EvolvingHockey analog).

Reuses the NBA/MBB regularized-adjusted-plus-minus (RAPM) pattern: the shift **stint**
replaces the possession as the unit of observation, and xG-per-60 replaces
points-per-100. Two observations per stint (one per attacking team); response is the
attacking team's xGF per 60; features are ``+1`` indicators for each on-ice attacker
(``off_<player>``) and defender (``def_<player>``), a home-ice indicator, and an
intercept. ``lam`` is chosen by k-fold CV over ``LEAGUE_CONSTANTS[league].rapm_lambda_grid``
unless given explicitly.

Follows the published EvolvingHockey / Emmanuel-Perry RAPM methodology; no license
obligation (see ``NOTICE``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import polars as pl
import scipy.sparse as sp

from sportsdataverse.nhl.nhl_gsax import _attribute_goalie  # noqa: F401  (re-export convenience)
from sportsdataverse.nhl.nhl_player_impact_constants import get_constants, team_fullname_to_abbr, weighted_ridge
from sportsdataverse.nhl.nhl_xg import nhl_xg

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["build_stints", "build_design", "nhl_skater_rapm"]

_STINTS_SCHEMA = {
    "game_id": pl.Int64,
    "period": pl.Int64,
    "start_s": pl.Int64,
    "end_s": pl.Int64,
    "duration": pl.Int64,
    "home_ids": pl.List(pl.Int64),
    "away_ids": pl.List(pl.Int64),
    "home_goalie": pl.Int64,
    "away_goalie": pl.Int64,
    "strength_state": pl.Utf8,
    "xgf_home": pl.Float64,
    "xgf_away": pl.Float64,
}

_RAPM_SCHEMA = {
    "player_id": pl.Int64,
    "xg_rapm_off": pl.Float64,
    "xg_rapm_def": pl.Float64,
    "xg_rapm": pl.Float64,
    "toi_minutes": pl.Float64,
}


def _parse_ids(s: str | None) -> list[int]:
    """Parse a comma-space-joined id string (``"1, 2, 3"``) into an int list, dropping ``"0"``."""
    if not s:
        return []
    return [int(x) for x in s.split(", ") if x and x.strip() != "0"]


def build_stints(shifts: pl.DataFrame, scored: pl.DataFrame, *, as_of: int | None = None) -> pl.DataFrame:
    """Fold ``load_nhl_shifts`` CHANGE events into contiguous constant-personnel intervals.

    Per game: resolves each shift row's full team name (``event_team``) to home/away via
    ``team_fullname_to_abbr`` + the game's ``home_abbr``/``away_abbr`` (from ``scored``),
    then folds ``ids_on``/``ids_off`` deltas chronologically into a running on-ice set per
    side. A new interval begins at every distinct ``game_seconds`` boundary; the final
    interval is closed at the last ``scored`` event's ``game_seconds`` + 1 for that game
    (there is no explicit "end of game" CHANGE row in the shift-chart feed).

    Known simplification: shift-chart id lists do not distinguish position, so
    ``home_ids``/``away_ids`` may include the on-ice goalie's id alongside skaters;
    ``home_goalie``/``away_goalie`` are instead sourced from the overlapping ``scored``
    events' ``home_goalie_id``/``away_goalie_id`` (the modal value in the interval).

    Args:
        shifts: a ``load_nhl_shifts``-shaped frame.
        scored: an ``nhl_xg``-scored frame (for the game's ``home_abbr``/``away_abbr``
            and each interval's on-ice xG-for and goalie).
        as_of: an optional per-game ``game_seconds`` cutoff -- intervals starting at or
            after ``as_of`` are dropped. This is the leakage boundary for any
            forward-looking use: features for a game/date must use only stints strictly
            before that game's cutoff.

    Returns:
        polars.DataFrame: one row per interval, schema documented in the module's
        ``_STINTS_SCHEMA``. Empty/malformed ``shifts`` returns a zero-row frame.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nhl.nhl_xg import nhl_xg
            from sportsdataverse.nhl.nhl_rapm import build_stints
            pbp = pl.read_parquet("tests/fixtures/nhl_player_impact/pbp_sample.parquet")
            shifts = pl.read_parquet("tests/fixtures/nhl_player_impact/shifts_sample.parquet")
            scored = nhl_xg(pbp, model_dir="tests/fixtures/nhl_player_impact/xg_models")
            stints = build_stints(shifts, scored)
    """
    if shifts.height == 0:
        return pl.DataFrame(schema=_STINTS_SCHEMA)

    game_teams = (
        scored.filter(pl.col("home_abbr").is_not_null())
        .group_by("game_id")
        .agg(home_abbr=pl.col("home_abbr").first(), away_abbr=pl.col("away_abbr").first())
    )
    game_teams_map = {row["game_id"]: (row["home_abbr"], row["away_abbr"]) for row in game_teams.to_dicts()}

    shifts_work = shifts.with_columns(
        team_abbr=pl.col("event_team").map_elements(team_fullname_to_abbr, return_dtype=pl.Utf8)
    ).sort("game_id", "game_seconds")

    rows: list[dict] = []
    for game_id, sub in shifts_work.group_by("game_id", maintain_order=True):
        gid = game_id[0] if isinstance(game_id, tuple) else game_id
        home_abbr, away_abbr = game_teams_map.get(gid, (None, None))
        game_scored = scored.filter(pl.col("game_id") == gid) if scored.height > 0 else scored

        boundaries = sub["game_seconds"].unique().sort().to_list()
        if game_scored.height > 0:
            end_boundary = int(game_scored["game_seconds"].max()) + 1
            if not boundaries or end_boundary > boundaries[-1]:
                boundaries.append(end_boundary)

        running: dict[str, set[int]] = {"home": set(), "away": set()}
        period_at: dict[int, int] = {}
        snapshot_at: dict[int, tuple[set[int], set[int]]] = {}
        for t in sub["game_seconds"].unique().sort().to_list():
            at_t = sub.filter(pl.col("game_seconds") == t)
            for r in at_t.to_dicts():
                side = "home" if r["team_abbr"] == home_abbr else ("away" if r["team_abbr"] == away_abbr else None)
                if side is None:
                    continue
                running[side] -= set(_parse_ids(r["ids_off"]))
                running[side] |= set(_parse_ids(r["ids_on"]))
                period_at[t] = r["period"]
            # Snapshot the on-ice personnel *after* applying every delta at this
            # boundary -- this is the state that holds for the interval starting at t.
            snapshot_at[t] = (set(running["home"]), set(running["away"]))

        for i in range(len(boundaries) - 1):
            start_s, end_s = boundaries[i], boundaries[i + 1]
            period = period_at.get(start_s, period_at.get(boundaries[0], 1))
            home_at_start, away_at_start = snapshot_at.get(start_s, (set(), set()))
            window = (
                game_scored.filter((pl.col("game_seconds") >= start_s) & (pl.col("game_seconds") < end_s))
                if game_scored.height > 0
                else game_scored
            )
            xgf_home = xgf_away = 0.0
            home_goalie = away_goalie = None
            strength_state = None
            if window.height > 0:
                home_events = window.filter(pl.col("event_team_abbr") == home_abbr)
                away_events = window.filter(pl.col("event_team_abbr") == away_abbr)
                if home_events.height > 0:
                    xgf_home = float(home_events["xg"].sum() or 0.0)
                if away_events.height > 0:
                    xgf_away = float(away_events["xg"].sum() or 0.0)
                if "home_goalie_id" in window.columns:
                    hg = window["home_goalie_id"].drop_nulls()
                    home_goalie = int(hg.mode()[0]) if hg.len() > 0 else None
                if "away_goalie_id" in window.columns:
                    ag = window["away_goalie_id"].drop_nulls()
                    away_goalie = int(ag.mode()[0]) if ag.len() > 0 else None
                if "strength_state" in window.columns:
                    ss = window["strength_state"].drop_nulls()
                    strength_state = str(ss.mode()[0]) if ss.len() > 0 else None
            rows.append(
                {
                    "game_id": gid,
                    "period": period,
                    "start_s": start_s,
                    "end_s": end_s,
                    "duration": end_s - start_s,
                    "home_ids": sorted(home_at_start - ({home_goalie} if home_goalie else set())),
                    "away_ids": sorted(away_at_start - ({away_goalie} if away_goalie else set())),
                    "home_goalie": home_goalie,
                    "away_goalie": away_goalie,
                    "strength_state": strength_state,
                    "xgf_home": xgf_home,
                    "xgf_away": xgf_away,
                }
            )

    if not rows:
        return pl.DataFrame(schema=_STINTS_SCHEMA)

    out = pl.DataFrame(rows, schema=_STINTS_SCHEMA)
    if as_of is not None:
        out = out.filter(pl.col("start_s") < as_of)
    return out


def build_design(stints: pl.DataFrame) -> tuple["sp.csr_matrix", np.ndarray, np.ndarray, list[int]]:
    """Build the sparse RAPM design matrix -- two rows per stint (one per attacking team).

    Args:
        stints: a ``build_stints``-shaped frame.

    Returns:
        tuple: ``(X, y, w, player_index)`` where ``X`` is a ``scipy.sparse.csr_matrix``
        with columns ``off_<player>`` (all on-ice attackers), ``def_<player>`` (all on-ice
        defenders), then a trailing home-ice indicator and intercept column; ``y`` is the
        attacking team's xGF per 60; ``w`` is stint duration (seconds); ``player_index``
        maps each ``off_``/``def_`` column pair's position to a ``player_id`` (so column
        ``j`` is ``off_<player_index[j]>`` and column ``j + n_players`` is
        ``def_<player_index[j]>``).

    Example:
        Quick start::

            from sportsdataverse.nhl.nhl_rapm import build_design
            X, y, w, player_index = build_design(stints)
    """
    if stints.height == 0:
        return sp.csr_matrix((0, 0)), np.array([]), np.array([]), []

    all_players: set[int] = set()
    for col in ("home_ids", "away_ids"):
        for lst in stints[col].to_list():
            all_players.update(lst or [])
    player_index = sorted(all_players)
    pos = {p: i for i, p in enumerate(player_index)}
    n = len(player_index)

    rows_i: list[int] = []
    cols_j: list[int] = []
    data: list[float] = []
    y: list[float] = []
    w: list[float] = []
    r = 0
    for rec in stints.to_dicts():
        duration = rec["duration"]
        if duration <= 0:
            continue
        for attacker_col, defender_col, xgf, home_flag in (
            ("home_ids", "away_ids", rec["xgf_home"], 1.0),
            ("away_ids", "home_ids", rec["xgf_away"], 0.0),
        ):
            for pid in rec[attacker_col] or []:
                rows_i.append(r)
                cols_j.append(pos[pid])
                data.append(1.0)
            for pid in rec[defender_col] or []:
                rows_i.append(r)
                cols_j.append(n + pos[pid])
                data.append(1.0)
            rows_i.append(r)
            cols_j.append(2 * n)  # home-ice indicator
            data.append(home_flag)
            rows_i.append(r)
            cols_j.append(2 * n + 1)  # intercept
            data.append(1.0)
            y.append((xgf or 0.0) * 3600.0 / duration)
            w.append(float(duration))
            r += 1

    X = sp.csr_matrix((data, (rows_i, cols_j)), shape=(r, 2 * n + 2))
    return X, np.array(y), np.array(w), player_index


def nhl_skater_rapm(
    pbp: pl.DataFrame,
    shifts: pl.DataFrame,
    *,
    model_dir: "str | None" = None,
    league: str = "nhl",
    lam: float | None = None,
    as_of: int | None = None,
    strength_states: list[str] | None = None,
    return_as_pandas: bool = False,
    _stints: pl.DataFrame | None = None,
) -> "pl.DataFrame | pd.DataFrame":
    """Per-skater xG-based Regularized Adjusted Plus-Minus (RAPM), per 60 minutes.

    Builds shift stints (``build_stints``), the sparse off/def design matrix
    (``build_design``), and solves the weighted ridge (``weighted_ridge``). Offensive
    rating is the ``off_<player>`` coefficient; defensive rating is the **negated**
    ``def_<player>`` coefficient (suppressing xG-against is positive value) --
    ``xg_rapm = xg_rapm_off + xg_rapm_def``.

    Args:
        pbp: a ``load_nhl_pbp_full``-shaped frame.
        shifts: a ``load_nhl_shifts``-shaped frame.
        model_dir: passed through to ``nhl_xg``.
        league: ``"nhl"`` or ``"pwhl"`` -- selects the ridge lambda-grid via
            ``LEAGUE_CONSTANTS`` when ``lam`` is not given.
        lam: an explicit ridge penalty; ``None`` selects via k-fold CV over
            ``LEAGUE_CONSTANTS[league].rapm_lambda_grid``.
        as_of: forwarded to ``build_stints`` -- the leakage-boundary cutoff.
        strength_states: restrict the design matrix to these ``strength_state`` values
            (e.g. ``["5v5"]`` for an even-strength-only fit, as used by
            ``nhl_skater_war``'s ``ev_off``/``ev_def`` components so they don't overlap
            with ``nhl_special_teams_value``'s PP/PK components). ``None`` (default)
            uses every strength state, matching the general-purpose all-situations RAPM.
        return_as_pandas: return a pandas DataFrame instead of polars.
        _stints: internal test hook -- inject a pre-built stints frame, bypassing
            ``pbp``/``shifts``/scoring (not part of the public contract).

    Returns:
        polars.DataFrame: ``player_id:Int64, xg_rapm_off:Float64, xg_rapm_def:Float64,
        xg_rapm:Float64, toi_minutes:Float64``. Empty input returns a zero-row frame
        with this schema.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nhl.nhl_rapm import nhl_skater_rapm
            pbp = pl.read_parquet("tests/fixtures/nhl_player_impact/pbp_sample.parquet")
            shifts = pl.read_parquet("tests/fixtures/nhl_player_impact/shifts_sample.parquet")
            rapm = nhl_skater_rapm(pbp, shifts, model_dir="tests/fixtures/nhl_player_impact/xg_models")
            print(rapm.sort("xg_rapm", descending=True).head(10))

    See Also:
        * `EvolvingHockey`_ -- concurrent-validity oracle (data-blocked as of 2026-07-08;
          see the fixture README).

    .. _EvolvingHockey: https://evolving-hockey.com
    """
    if _stints is not None:
        stints = _stints
    else:
        if pbp.height == 0 or shifts.height == 0:
            return pl.DataFrame(schema=_RAPM_SCHEMA)
        scored = nhl_xg(pbp, model_dir=model_dir, league=league)
        stints = build_stints(shifts, scored, as_of=as_of)

    if strength_states is not None and stints.height > 0:
        stints = stints.filter(pl.col("strength_state").is_in(strength_states))

    if stints.height == 0:
        return pl.DataFrame(schema=_RAPM_SCHEMA)

    X, y, w, player_index = build_design(stints)
    if not player_index:
        return pl.DataFrame(schema=_RAPM_SCHEMA)

    cfg = get_constants(league)
    if lam is not None:
        best_lam = lam
    else:
        best_lam = _cv_select_lambda(X, y, w, cfg.rapm_lambda_grid)

    beta = weighted_ridge(X, y, w, best_lam)
    n = len(player_index)
    off_coef = beta[:n]
    def_coef = beta[n : 2 * n]

    toi_seconds = {p: 0.0 for p in player_index}
    for rec in stints.to_dicts():
        for pid in (rec["home_ids"] or []) + (rec["away_ids"] or []):
            if pid in toi_seconds:
                toi_seconds[pid] += float(rec["duration"])

    out = pl.DataFrame(
        {
            "player_id": player_index,
            "xg_rapm_off": off_coef.tolist(),
            "xg_rapm_def": (-def_coef).tolist(),
            "toi_minutes": [toi_seconds[p] / 60.0 for p in player_index],
        }
    ).with_columns(xg_rapm=(pl.col("xg_rapm_off") + pl.col("xg_rapm_def")))
    out = out.select("player_id", "xg_rapm_off", "xg_rapm_def", "xg_rapm", "toi_minutes")
    return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out


def _cv_select_lambda(X: "sp.csr_matrix", y: np.ndarray, w: np.ndarray, grid: list[float]) -> float:
    """k-fold (k=min(5, n)) CV over ``grid``, minimizing weighted out-of-fold MSE."""
    n = len(y)
    if n < 2:
        return grid[0]
    k = min(5, n)
    rng = np.random.default_rng(0)
    idx = rng.permutation(n)
    folds = np.array_split(idx, k)

    best_lam, best_mse = grid[0], np.inf
    for lam in grid:
        mse_total, w_total = 0.0, 0.0
        for i in range(k):
            test_idx = folds[i]
            train_idx = np.concatenate([folds[j] for j in range(k) if j != i])
            if len(train_idx) == 0 or len(test_idx) == 0:
                continue
            beta = weighted_ridge(X[train_idx], y[train_idx], w[train_idx], lam)
            pred = X[test_idx] @ beta
            resid2 = (y[test_idx] - pred) ** 2
            mse_total += float(np.sum(resid2 * w[test_idx]))
            w_total += float(np.sum(w[test_idx]))
        mse = mse_total / w_total if w_total > 0 else np.inf
        if mse < best_mse:
            best_mse, best_lam = mse, lam
    return best_lam
