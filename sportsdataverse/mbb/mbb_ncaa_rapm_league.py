"""League-wide NCAA RAPM: one joint O/D ridge per (league, season).

This is "Path B" of the NCAA RAPM program. The published
``ncaa_{lg}_rapm_within_team`` datasets estimate each player RELATIVE TO
TEAMMATES (the hoop-explorer engine solves one team at a time); this module
estimates every Division-I player on a COMMON league scale by regressing all
possessions jointly, which is what makes a team-aggregate external gate
(Torvik AdjEM Spearman) meaningful.

Like :mod:`sportsdataverse.mbb.mbb_ncaa_rapm_input`, this module is
league-blind -- frames in, frames out. WBB passes its own frames; there is
deliberately no ``wbb_ncaa_rapm_league`` twin.

The pipeline:

1. :func:`mbb_ncaa_rapm_input.resolve_possessions` attaches a ``player_id``
   to each of the ten on-floor slots (identity is the hard part and lives
   there, not here).
2. :func:`aggregate_stints` collapses those possessions to matchup stints --
   one row per unique (offense five, defense five, home/away offense). A
   possession-count-weighted ridge on stints is mathematically identical to
   the per-possession ridge (same ``X'WX`` / ``X'Wy``) while keeping the
   design matrix ~300k sparse rows instead of ~870k.
3. :func:`solve_rapm_league` runs the weighted sparse joint O/D ridge.

The model, on the per-100-possession scale::

    pts_per_100 = mu + sum(orapm_j, offense five) - sum(drapm_k, defense five)
                  + hca * side          (side = +1 home offense, -1 away)

``mu`` is the possession-weighted league mean, removed by centering (so it is
unpenalized); the player coefficients and ``hca`` are L2-penalized. With the
minus sign on the defense block, POSITIVE ``drapm`` means good defense. The
``hca`` coefficient is half the home-minus-away offensive gap; its shrinkage
under the shared penalty is negligible because every possession loads it.

A null slot id (an unresolved player, or the ``TEAM`` pseudo-slot) marks the
possession unusable and it is DROPPED -- "not a rated player" must never be
imputed. Callers should report the usable fraction.
"""

from __future__ import annotations

import numpy as np
import polars as pl

__all__ = [
    "DEFAULT_RIDGE_LAMBDA",
    "STINT_SCHEMA",
    "aggregate_stints",
    "solve_rapm_league",
    "team_aggregate",
]

#: Ridge penalty on the possession-weighted per-100 scale. Fitted by
#: game-grouped 5-fold CV (``dev/ncaa_rapm/fit_lambda_league.py``) on the
#: real 2024 seasons: BOTH leagues minimize weighted OOS MSE at 1000
#: (mbb 5145.33 over a 50..5000 grid; wbb 4910.74). Refit with that script;
#: do not tune ad hoc.
DEFAULT_RIDGE_LAMBDA = 1000.0

#: The ten id slots emitted by ``resolve_possessions`` (``"<slot>_id"``).
_SLOT_IDS = [f"{side}_{i}_id" for side in ("home", "away") for i in range(1, 6)]
_HOME_IDS = _SLOT_IDS[:5]
_AWAY_IDS = _SLOT_IDS[5:]

#: Documented output schema of :func:`aggregate_stints`.
STINT_SCHEMA: dict[str, pl.DataType] = {
    "off_ids": pl.List(pl.Utf8),
    "def_ids": pl.List(pl.Utf8),
    "off_team": pl.Utf8,
    "def_team": pl.Utf8,
    "is_home_offense": pl.Boolean,
    "n_poss": pl.Int64,
    "pts": pl.Int64,
}

_PLAYERS_SCHEMA: dict[str, pl.DataType] = {
    "player_id": pl.Utf8,
    "orapm": pl.Float64,
    "drapm": pl.Float64,
    "rapm_net": pl.Float64,
    "off_poss": pl.Int64,
    "def_poss": pl.Int64,
}


def aggregate_stints(resolved: pl.DataFrame) -> pl.DataFrame:
    """Collapse id-resolved possessions into matchup stints.

    Args:
        resolved: Output of ``resolve_possessions`` -- needs ``home``,
            ``away``, ``poss_team``, ``pts`` and the ten ``{slot}_id``
            columns. Extra columns are ignored.

    Returns:
        One row per unique ``(offense five, defense five, home/away
        offense)`` with ``n_poss`` and total ``pts`` (schema
        :data:`STINT_SCHEMA`). Possessions with ANY null slot id are dropped
        (unresolved player or ``TEAM`` pseudo-slot -- never imputed), as are
        possessions whose ``poss_team`` matches neither side.

    Raises:
        ValueError: A required column is missing.

    Example:
        Season pipeline::

            from sportsdataverse.mbb.mbb_ncaa_rapm_input import (
                build_player_xwalk, resolve_possessions,
            )
            from sportsdataverse.mbb.mbb_ncaa_rapm_league import aggregate_stints

            resolved = resolve_possessions(possessions, build_player_xwalk(rosters))
            stints = aggregate_stints(resolved)
    """
    required = ["home", "away", "poss_team", "pts", *_SLOT_IDS]
    missing = [c for c in required if c not in resolved.columns]
    if missing:
        raise ValueError(f"aggregate_stints: missing required columns {missing}")

    usable = resolved.filter(
        pl.all_horizontal([pl.col(c).is_not_null() for c in _SLOT_IDS])
        & ((pl.col("poss_team") == pl.col("home")) | (pl.col("poss_team") == pl.col("away")))
    )
    if usable.height == 0:
        return pl.DataFrame(schema=STINT_SCHEMA)

    is_home = pl.col("poss_team") == pl.col("home")
    home_ids = pl.concat_list([pl.col(c) for c in _HOME_IDS]).list.sort()
    away_ids = pl.concat_list([pl.col(c) for c in _AWAY_IDS]).list.sort()
    shaped = usable.with_columns(
        pl.when(is_home).then(home_ids).otherwise(away_ids).alias("off_ids"),
        pl.when(is_home).then(away_ids).otherwise(home_ids).alias("def_ids"),
        pl.when(is_home).then(pl.col("away")).otherwise(pl.col("home")).alias("def_team"),
        pl.col("poss_team").alias("off_team"),
        is_home.alias("is_home_offense"),
    )
    return (
        shaped.with_columns(
            pl.col("off_ids").list.join("|").alias("_ok"),
            pl.col("def_ids").list.join("|").alias("_dk"),
        )
        .group_by(["_ok", "_dk", "is_home_offense"], maintain_order=True)
        .agg(
            pl.col("off_ids").first(),
            pl.col("def_ids").first(),
            pl.col("off_team").first(),
            pl.col("def_team").first(),
            pl.len().cast(pl.Int64).alias("n_poss"),
            pl.col("pts").cast(pl.Int64).sum().alias("pts"),
        )
        .select(list(STINT_SCHEMA))
    )


_TEAM_AGG_SCHEMA: dict[str, pl.DataType] = {
    "team": pl.Utf8,
    "team_orapm": pl.Float64,
    "team_drapm": pl.Float64,
    "team_net": pl.Float64,
    "off_poss": pl.Int64,
    "def_poss": pl.Int64,
}


def _side_aggregate(
    stints: pl.DataFrame, players: pl.DataFrame, ids: str, team: str, coef: str, out: str
) -> pl.DataFrame:
    per_stint = (
        stints.with_row_index("_r")
        .select("_r", pl.col(team).alias("team"), "n_poss", pl.col(ids).alias("player_id"))
        .explode("player_id", empty_as_null=False)
        .join(players.select("player_id", coef), on="player_id", how="left")
        .group_by("_r", "team", "n_poss")
        .agg(pl.col(coef).fill_null(0.0).sum().alias("_sum"))
    )
    poss_col = "off_poss" if ids == "off_ids" else "def_poss"
    return per_stint.group_by("team").agg(
        ((pl.col("_sum") * pl.col("n_poss")).sum() / pl.col("n_poss").sum()).alias(out),
        pl.col("n_poss").sum().cast(pl.Int64).alias(poss_col),
    )


def team_aggregate(stints: pl.DataFrame, players: pl.DataFrame) -> pl.DataFrame:
    """Model-implied team ratings from player coefficients.

    For each team: ``team_orapm`` is the possession-weighted mean over its
    offensive stints of the on-floor five's ``orapm`` sum; ``team_drapm``
    likewise with ``drapm`` over its defensive stints; ``team_net`` is their
    sum. This is exactly what the fitted model predicts for the team's
    average possession (excluding opponent terms and home-court), so it is
    the faithful aggregate to hold against an external team rating (Torvik
    AdjEM) in the oracle gate.

    Args:
        stints: Output of :func:`aggregate_stints`.
        players: Players frame from :func:`solve_rapm_league` (needs
            ``player_id``, ``orapm``, ``drapm``). A player absent from it
            contributes 0 -- the model's own convention for an unrated
            column.

    Returns:
        One row per team: ``team``, ``team_orapm``, ``team_drapm``,
        ``team_net``, ``off_poss``, ``def_poss``.

    Example:
        Gate a season against Torvik::

            teams = team_aggregate(stints, players)
            joined = teams.join(torvik, on="team", how="inner")
    """
    if stints.height == 0:
        return pl.DataFrame(schema=_TEAM_AGG_SCHEMA)
    off = _side_aggregate(stints, players, "off_ids", "off_team", "orapm", "team_orapm")
    dfn = _side_aggregate(stints, players, "def_ids", "def_team", "drapm", "team_drapm")
    return (
        off.join(dfn, on="team", how="full", coalesce=True)
        .with_columns(
            pl.col("team_orapm").fill_null(0.0),
            pl.col("team_drapm").fill_null(0.0),
            pl.col("off_poss").fill_null(0).cast(pl.Int64),
            pl.col("def_poss").fill_null(0).cast(pl.Int64),
        )
        .with_columns((pl.col("team_orapm") + pl.col("team_drapm")).alias("team_net"))
        .select(list(_TEAM_AGG_SCHEMA))
        .sort("team")
    )


def solve_rapm_league(
    stints: pl.DataFrame,
    *,
    ridge_lambda: float = DEFAULT_RIDGE_LAMBDA,
    return_as_pandas: bool = False,
) -> "tuple[pl.DataFrame, dict[str, float]]":
    """Weighted sparse joint O/D ridge over matchup stints.

    Args:
        stints: Output of :func:`aggregate_stints`.
        ridge_lambda: L2 penalty on the possession-weighted normal equations.
            Because rows are weighted by possessions, the same ``lambda`` is
            exactly equivalent to a per-possession ridge with that penalty.
        return_as_pandas: Return the players frame as pandas.

    Returns:
        ``(players, info)``. ``players`` has one row per rated player:
        ``player_id``, ``orapm``, ``drapm`` (positive = good defense),
        ``rapm_net = orapm + drapm`` (all per 100 possessions), ``off_poss``,
        ``def_poss``. ``info`` carries ``intercept`` (possession-weighted
        league mean pts/100), ``hca`` (half the home-minus-away offensive
        gap), ``ridge_lambda``, ``n_stints``, ``n_poss``.

    Example:
        Solve a season::

            from sportsdataverse.mbb.mbb_ncaa_rapm_league import (
                aggregate_stints, solve_rapm_league,
            )

            players, info = solve_rapm_league(aggregate_stints(resolved))
            players.sort("rapm_net", descending=True).head()

    See Also:
        * `hoopR <https://hoopR.sportsdataverse.org>`_ -- men's college
          basketball companion (R)
        * `wehoop <https://wehoop.sportsdataverse.org>`_ -- women's college
          basketball companion (R)
    """
    from scipy.sparse import coo_matrix
    from scipy.sparse.linalg import lsqr

    empty = pl.DataFrame(schema=_PLAYERS_SCHEMA)
    if stints.height == 0:
        info = {
            "intercept": 0.0,
            "hca": 0.0,
            "ridge_lambda": float(ridge_lambda),
            "n_stints": 0,
            "n_poss": 0,
        }
        return (empty.to_pandas() if return_as_pandas else empty), info

    s = stints.with_row_index("_row")
    # empty_as_null=False is the polars-2.0 default; the lists are always
    # exactly five ids so the choice is behavior-neutral here.
    off = s.select("_row", "n_poss", pl.col("off_ids").alias("pid")).explode("pid", empty_as_null=False)
    dfn = s.select("_row", "n_poss", pl.col("def_ids").alias("pid")).explode("pid", empty_as_null=False)
    players = sorted(set(off["pid"].to_list()) | set(dfn["pid"].to_list()))
    idx = {p: i for i, p in enumerate(players)}
    n_players = len(players)
    n_stints = s.height

    w = s["n_poss"].to_numpy().astype(np.float64)
    sw = np.sqrt(w)
    y = 100.0 * s["pts"].to_numpy().astype(np.float64) / w
    mu = float(np.average(y, weights=w))

    # Sparse design: offense block [0, P), defense block [P, 2P) with -1
    # entries (so positive drapm = fewer points allowed), hca column at 2P.
    off_rows = off["_row"].to_numpy().astype(np.int64)
    dfn_rows = dfn["_row"].to_numpy().astype(np.int64)
    off_cols: "np.ndarray" = np.fromiter((idx[p] for p in off["pid"].to_list()), np.int64, len(off_rows))
    dfn_cols: "np.ndarray" = np.fromiter((n_players + idx[p] for p in dfn["pid"].to_list()), np.int64, len(dfn_rows))
    side = np.where(s["is_home_offense"].to_numpy(), 1.0, -1.0)

    rows = np.concatenate([off_rows, dfn_rows, np.arange(n_stints)])
    cols = np.concatenate([off_cols, dfn_cols, np.full(n_stints, 2 * n_players)])
    vals = np.concatenate([sw[off_rows], -sw[dfn_rows], side * sw])
    x = coo_matrix((vals, (rows, cols)), shape=(n_stints, 2 * n_players + 1)).tocsr()

    beta = lsqr(x, (y - mu) * sw, damp=float(np.sqrt(ridge_lambda)))[0]
    orapm = beta[:n_players]
    drapm = beta[n_players : 2 * n_players]
    hca = float(beta[2 * n_players])

    exposure = (
        pl.concat(
            [
                off.select("pid", pl.col("n_poss").alias("off_poss")),
                dfn.select("pid", pl.col("n_poss").alias("def_poss")),
            ],
            how="diagonal",
        )
        .group_by("pid")
        .agg(
            pl.col("off_poss").sum().fill_null(0).cast(pl.Int64),
            pl.col("def_poss").sum().fill_null(0).cast(pl.Int64),
        )
    )
    out = (
        pl.DataFrame(
            {
                "player_id": pl.Series(players, dtype=pl.Utf8),
                "orapm": orapm.astype(np.float64),
                "drapm": drapm.astype(np.float64),
            }
        )
        .with_columns((pl.col("orapm") + pl.col("drapm")).alias("rapm_net"))
        .join(exposure.rename({"pid": "player_id"}), on="player_id", how="left")
        .with_columns(
            pl.col("off_poss").fill_null(0).cast(pl.Int64),
            pl.col("def_poss").fill_null(0).cast(pl.Int64),
        )
        .select(list(_PLAYERS_SCHEMA))
        .sort("player_id")
    )
    info = {
        "intercept": mu,
        "hca": hca,
        "ridge_lambda": float(ridge_lambda),
        "n_stints": n_stints,
        "n_poss": int(w.sum()),
    }
    return (out.to_pandas() if return_as_pandas else out), info
