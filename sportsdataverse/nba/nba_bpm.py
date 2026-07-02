"""Faithful BPM 2.0 (Box Plus/Minus) — published-coefficient box-score player value."""

from __future__ import annotations

from typing import Dict, Tuple

import polars as pl

from sportsdataverse.nba.nba_box_logs import box_features

# Published BPM 2.0 coefficients (Basketball-Reference "About BPM 2.0", Daniel Myers 2020).
# position-varying cols are (pos1_PG, pos5_C); *_role cols are (role1_Creator, role5_Receiver).
BPM2_COEFFICIENTS: Dict[str, Dict[str, Tuple[float, float]]] = {
    "base": {
        "pts": (0.860, 0.860),
        "fg3m": (0.389, 0.389),
        "ast": (0.580, 1.034),
        "tov": (-0.964, -0.964),
        "orb": (0.613, 0.181),
        "drb": (0.116, 0.181),
        "stl": (1.369, 1.008),
        "blk": (1.327, 0.703),
        "pf": (-0.367, -0.367),
        "fga_role": (-0.560, -0.780),
        "fta_role": (-0.246, -0.343),
    },
    "offense": {
        "pts": (0.605, 0.605),
        "fg3m": (0.477, 0.477),
        "ast": (0.476, 0.476),
        "tov": (-0.579, -0.882),
        "orb": (0.606, 0.422),
        "drb": (-0.112, 0.103),
        "stl": (0.177, 0.294),
        "blk": (0.725, 0.097),
        "pf": (-0.439, -0.439),
        "fga_role": (-0.330, -0.472),
        "fta_role": (-0.145, -0.208),
    },
}
# position regression on % of team stats (min-weighted); blended with 50 min listed position
BPM2_POSITION_REG: Dict[str, float] = {
    "intercept": 2.130,
    "pct_trb": 8.668,
    "pct_stl": -2.486,
    "pct_pf": 0.992,
    "pct_ast": -3.536,
    "pct_blk": 1.667,
}
# offensive-role regression on % of team AST + % of team threshold-points
BPM2_ROLE_REG: Dict[str, float] = {"intercept": 6.00, "pct_ast": -6.642, "pct_threshold_pts": -8.544}

# baseline pts per adjusted (true) shot attempt used by the shooting-context step
# (calibrated to reproduce the B-Ref 2016-17 LeBron worked example 34.9 -> 30.4)
SHOOTING_BASELINE: float = 1.00
# shooting-efficiency threshold: 0.33 pts/TSA below team average (offensive-role "threshold points")
THRESHOLD_MARGIN: float = 0.33
_LISTED_BLEND_MIN: float = 50.0  # 50 minutes of listed position blended into the estimate


def _clamp(expr: pl.Expr, lo: float = 1.0, hi: float = 5.0) -> pl.Expr:
    return expr.clip(lo, hi)


def _estimate_position(shares: pl.DataFrame, listed: pl.DataFrame) -> pl.DataFrame:
    """Estimate each player's position (1-5) from % of team stats, blended with 50 min of
    listed position, then recursively shifted so the minute-weighted team mean is 3.0, clamped.

    Args:
        shares: player_id, team_id, min, and pct_trb/pct_stl/pct_pf/pct_ast/pct_blk (min-weighted
            % of team totals).
        listed: player_id, position_num (from ``nba_player_positions``).

    Returns:
        Frame player_id, position_num (Float64, in [1,5]).
    """
    c = BPM2_POSITION_REG
    raw = (
        shares.join(listed, on="player_id", how="left")
        .with_columns(pl.col("position_num").fill_null(3.0))
        .with_columns(
            (
                c["intercept"]
                + c["pct_trb"] * pl.col("pct_trb")
                + c["pct_stl"] * pl.col("pct_stl")
                + c["pct_pf"] * pl.col("pct_pf")
                + c["pct_ast"] * pl.col("pct_ast")
                + c["pct_blk"] * pl.col("pct_blk")
            ).alias("reg_pos")
        )
        .with_columns(
            # blend regression with 50 min of listed position (min-weighted)
            (
                (pl.col("reg_pos") * pl.col("min") + pl.col("position_num") * _LISTED_BLEND_MIN)
                / (pl.col("min") + _LISTED_BLEND_MIN)
            ).alias("blended")
        )
    )
    # recursive team-sum-to-3.0 with clamping (iterate: shift by team-mean deviation, clamp)
    return _recursive_team_center(raw, "blended", "position_num", target=3.0)


def _recursive_team_center(df: pl.DataFrame, col: str, out: str, *, target: float, iters: int = 100) -> pl.DataFrame:
    """Add a per-team constant so the min-weighted team mean of ``col`` equals ``target``,
    re-clamping to [1,5] each pass (recursive because clamping perturbs the mean).

    Exits early when the max per-team absolute deviation of the min-weighted mean from ``target``
    is below 1e-9.  If the roster geometry makes the constraint infeasible (e.g. four of five
    players are true 5.0 centers), the iteration converges to the closest feasible point without
    raising — this mirrors B-Ref's own algorithm behaviour.
    """
    work = df.select(["player_id", "team_id", "min", col]).rename({col: "_v"})
    for _ in range(iters):
        team_mean = work.group_by("team_id").agg(
            ((pl.col("_v") * pl.col("min")).sum() / pl.col("min").sum()).alias("_m")
        )
        # early-exit: if every team's weighted mean is already at target, stop before mutating
        max_dev = (team_mean["_m"] - target).abs().max()
        if max_dev is not None and max_dev < 1e-9:
            break
        work = (
            work.join(team_mean, on="team_id").with_columns(_clamp(pl.col("_v") + (target - pl.col("_m")))).drop("_m")
        )
    return work.select("player_id", pl.col("_v").alias(out))


def _estimate_role(shares: pl.DataFrame) -> pl.DataFrame:
    """Estimate offensive role (1 Creator .. 5 Receiver) from % of team AST + threshold points, clamped.

    Args:
        shares: player_id, pct_ast, pct_threshold_pts.

    Returns:
        Frame player_id, role_num (Float64, in [1,5]).
    """
    c = BPM2_ROLE_REG
    return shares.select(
        "player_id",
        _clamp(
            c["intercept"] + c["pct_ast"] * pl.col("pct_ast") + c["pct_threshold_pts"] * pl.col("pct_threshold_pts")
        ).alias("role_num"),
    )


# ---------------------------------------------------------------------------
# Raw BPM computation (Task 3) — position/role-interpolated coefficients
# ---------------------------------------------------------------------------

_POS_STATS = ["pts", "fg3m", "ast", "tov", "orb", "drb", "stl", "blk", "pf"]


def _interp(pair: Tuple[float, float], scale: float) -> float:
    """Interpolate a (value_at_1, value_at_5) coefficient pair at ``scale`` in [1,5].

    Args:
        pair: ``(lo, hi)`` — coefficient value at position/role 1 and 5 respectively.
        scale: Player's position (1–5) or role (1–5) value at which to interpolate.

    Returns:
        Linearly interpolated coefficient.
    """
    lo, hi = pair
    return lo + (hi - lo) * (scale - 1.0) / 4.0


def _bpm_from_table(
    feats: pl.DataFrame,
    table: Dict[str, Tuple[float, float]],
    positions: pl.DataFrame,
    roles: pl.DataFrame,
    out: str,
) -> pl.DataFrame:
    """Compute one BPM component (offense or base total) for each player.

    Each position-varying stat coefficient is interpolated at the player's ``position_num``;
    the role-varying ``fga_role``/``fta_role`` coefficients are interpolated at ``role_num``.

    Args:
        feats: per-player per-100 stats (must include ``player_id``, all ``_POS_STATS``,
            ``fga``, ``fta``; ``pts`` must be shooting-context-adjusted already).
        table: one of the ``BPM2_COEFFICIENTS`` sub-dicts (``"base"`` or ``"offense"``).
        positions: player_id, position_num.
        roles: player_id, role_num.
        out: name of the output column.

    Returns:
        Frame with ``player_id`` and ``out`` (Float64).
    """
    j = (
        feats.join(positions, on="player_id", how="left")
        .join(roles, on="player_id", how="left")
        .with_columns(
            pl.col("position_num").fill_null(3.0),
            pl.col("role_num").fill_null(3.0),
        )
    )

    def _contrib(row_pos: float, row_role: float, r: Dict[str, object]) -> float:
        total = 0.0
        for s in _POS_STATS:
            total += _interp(table[s], row_pos) * float(r[s])  # type: ignore[arg-type]
        total += _interp(table["fga_role"], row_role) * float(r["fga"])  # type: ignore[arg-type]
        total += _interp(table["fta_role"], row_role) * float(r["fta"])  # type: ignore[arg-type]
        return total

    vals = [_contrib(float(row["position_num"]), float(row["role_num"]), row) for row in j.iter_rows(named=True)]
    return j.select("player_id").with_columns(pl.Series(out, vals, dtype=pl.Float64))


def _raw_bpm(feats: pl.DataFrame, positions: pl.DataFrame, roles: pl.DataFrame) -> pl.DataFrame:
    """Raw (pre-team-adjustment) OBPM + total BPM from per-100 features + position/role.

    ``feats`` must carry shooting-context-adjusted ``pts`` and the other per-100 stats in
    ``_POS_STATS`` + ``fga``/``fta``. Returns ``player_id``, ``raw_obpm``, ``raw_bpm``.

    Args:
        feats: per-player per-100 stats (``player_id``, ``pts`` already adjusted, ``fg3m``,
            ``ast``, ``tov``, ``orb``, ``drb``, ``stl``, ``blk``, ``pf``, ``fga``, ``fta``).
        positions: player_id, position_num (Float64, [1,5]) from ``_estimate_position``.
        roles: player_id, role_num (Float64, [1,5]) from ``_estimate_role``.

    Returns:
        Frame with ``player_id``, ``raw_obpm`` (Float64), ``raw_bpm`` (Float64).

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nba.nba_bpm import _raw_bpm

            feats = pl.DataFrame({
                "player_id": [23], "pts": [30.4], "fg3m": [2.2], "ast": [11.5],
                "tov": [5.4], "orb": [1.7], "drb": [9.7], "stl": [1.6],
                "blk": [0.8], "pf": [2.4], "fga": [24.0], "fta": [9.5],
            })
            positions = pl.DataFrame({"player_id": [23], "position_num": [2.30]})
            roles = pl.DataFrame({"player_id": [23], "role_num": [1.0]})
            out = _raw_bpm(feats, positions, roles)
            print(out["raw_bpm"][0])   # ≈ 18.7

        See Also:
            * `Basketball-Reference BPM 2.0`_ — published coefficient table and methodology
            * `hoopR`_ — R companion package

        .. _Basketball-Reference BPM 2.0: https://www.basketball-reference.com/about/bpm2.html
        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    total = _bpm_from_table(feats, BPM2_COEFFICIENTS["base"], positions, roles, "raw_bpm")
    off = _bpm_from_table(feats, BPM2_COEFFICIENTS["offense"], positions, roles, "raw_obpm")
    return total.join(off, on="player_id")


# ---------------------------------------------------------------------------
# Task 4: team shares, team adjustment, shooting-context, and public nba_bpm
# ---------------------------------------------------------------------------


def _team_shares(player_logs: pl.DataFrame, team_logs: pl.DataFrame) -> pl.DataFrame:
    """Per-player % of team TRB/STL/PF/AST/BLK + threshold-points share (min-weighted totals).

    Args:
        player_logs: Per-player-per-game box lines with ``player_id``, ``team_id``, ``min``,
            ``reb``, ``stl``, ``pf``, ``ast``, ``blk``, ``pts``, ``fga``, ``fta``.
        team_logs: Per-team-per-game lines with ``team_id``, ``reb``, ``stl``, ``pf``,
            ``ast``, ``blk``, ``pts``, ``fga``, ``fta``.

    Returns:
        Frame with ``player_id``, ``team_id``, ``min``, ``pct_trb``, ``pct_stl``,
        ``pct_pf``, ``pct_ast``, ``pct_blk``, ``pct_threshold_pts``.
    """
    pl_agg = player_logs.group_by("player_id").agg(
        pl.col("team_id").first(),
        pl.col("min").sum().alias("min"),
        *[pl.col(s).sum().alias(s) for s in ["reb", "stl", "pf", "ast", "blk", "pts", "fga", "fta"]],
    )
    team_tot = team_logs.group_by("team_id").agg(
        *[pl.col(s).sum().alias(f"team_{s}") for s in ["reb", "stl", "pf", "ast", "blk", "pts", "fga", "fta"]]
    )
    j = pl_agg.join(team_tot, on="team_id", how="left")
    tsa = pl.col("fga") + 0.44 * pl.col("fta")
    team_tsa = pl.col("team_fga") + 0.44 * pl.col("team_fta")
    team_pps = pl.col("team_pts") / team_tsa  # team avg pts / true-shot-attempt
    thr_pts = pl.col("pts") - (team_pps - THRESHOLD_MARGIN) * tsa  # points above threshold
    return j.with_columns(
        (pl.col("reb") / pl.col("team_reb")).alias("pct_trb"),
        (pl.col("stl") / pl.col("team_stl")).alias("pct_stl"),
        (pl.col("pf") / pl.col("team_pf")).alias("pct_pf"),
        (pl.col("ast") / pl.col("team_ast")).alias("pct_ast"),
        (pl.col("blk") / pl.col("team_blk")).alias("pct_blk"),
        (thr_pts / pl.col("team_pts")).alias("pct_threshold_pts"),
    ).select(["player_id", "team_id", "min", "pct_trb", "pct_stl", "pct_pf", "pct_ast", "pct_blk", "pct_threshold_pts"])


def _team_adjust(raw: pl.DataFrame, team_margin: pl.DataFrame, ptm: pl.DataFrame) -> pl.DataFrame:
    """Add a per-team constant so minute-weighted team raw_bpm == the team's efficiency margin.

    Args:
        raw: Frame with ``player_id``, ``raw_bpm``, ``raw_obpm``.
        team_margin: Frame with ``team_id``, ``margin`` (efficiency margin per 100 possessions).
        ptm: Frame with ``player_id``, ``team_id``, ``min`` (total minutes per player).

    Returns:
        Frame with ``player_id``, ``obpm``, ``bpm`` (team-adjusted).
    """
    j = raw.join(ptm, on="player_id")  # ptm: player_id, team_id, min
    cur = (
        j.group_by("team_id")
        .agg(((pl.col("raw_bpm") * pl.col("min")).sum() / pl.col("min").sum()).alias("cur"))
        .join(team_margin, on="team_id")
    )
    const = cur.with_columns((pl.col("margin") - pl.col("cur")).alias("k")).select(["team_id", "k"])
    return (
        j.join(const, on="team_id")
        .with_columns(
            (pl.col("raw_bpm") + pl.col("k")).alias("bpm"),
            (pl.col("raw_obpm") + pl.col("k")).alias("obpm"),
        )
        .select(["player_id", "obpm", "bpm"])
    )


def nba_bpm(
    player_logs: pl.DataFrame,
    team_logs: pl.DataFrame,
    positions: pl.DataFrame,
    *,
    team_adjust: bool = True,
    return_as_pandas: bool = False,
) -> pl.DataFrame:
    """Faithful BPM 2.0 per player over the given logs (a season).

    Args:
        player_logs: per-player-per-game box lines (``nba_box_logs``'s ``player``).
        team_logs: per-team-per-game lines incl. ``plus_minus`` (``nba_box_logs``'s ``team``).
        positions: listed positions (``nba_player_positions``): player_id, position_num.
        team_adjust: apply the team adjustment (True) or return raw box-BPM (False).
        return_as_pandas: return pandas instead of polars.

    Returns:
        Frame with ``player_id``, ``obpm``, ``dbpm``, ``bpm``, ``min``, ``gp``
        (Int64 player_id/gp, Float64 obpm/dbpm/bpm/min).

    Example:
        Season BPM (residential IP)::

            from sportsdataverse.nba import nba_bpm, nba_box_logs, nba_player_positions
            logs = nba_box_logs("2023-24"); pos = nba_player_positions("2023-24")
            bpm = nba_bpm(logs["player"], logs["team"], pos)
            print(bpm.sort("bpm", descending=True).head())

        Raw (no team adjustment)::

            bpm_raw = nba_bpm(logs["player"], logs["team"], pos, team_adjust=False)

        Pandas output::

            bpm_pd = nba_bpm(logs["player"], logs["team"], pos, return_as_pandas=True)

        See Also:
            * `Basketball-Reference BPM 2.0`_ — published coefficient table and methodology
            * `hoopR`_ — R companion package

        .. _Basketball-Reference BPM 2.0: https://www.basketball-reference.com/about/bpm2.html
        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    # box_features returns oreb/dreb; _raw_bpm expects orb/drb — rename at the boundary
    feats_raw = box_features(player_logs, team_logs)
    feats_renamed = feats_raw.rename({"oreb": "orb", "dreb": "drb"})

    shares = _team_shares(player_logs, team_logs)
    positions_est = _estimate_position(shares, positions)
    roles = _estimate_role(shares)

    # shooting-context: adjust per-100 pts toward the baseline given team shooting environment
    tl = (
        team_logs.group_by("team_id")
        .agg(
            pl.col("pts").sum().alias("tp"),
            (pl.col("fga").sum() + 0.44 * pl.col("fta").sum()).alias("ttsa"),
            pl.col("plus_minus").sum().alias("pm"),
            (pl.col("fga").sum() - pl.col("oreb").sum() + pl.col("tov").sum() + 0.44 * pl.col("fta").sum()).alias(
                "poss"
            ),
        )
        .with_columns(
            (SHOOTING_BASELINE - pl.col("tp") / pl.col("ttsa")).alias("pps_delta"),
            (pl.col("pm") / pl.col("poss") * 100).alias("margin"),
        )
    )
    feats_adj = (
        feats_renamed.join(shares.select(["player_id", "team_id"]), on="player_id")
        .join(tl.select(["team_id", "pps_delta"]), on="team_id")
        .with_columns((pl.col("pts") + pl.col("pps_delta") * (pl.col("fga") + 0.44 * pl.col("fta"))).alias("pts"))
    )

    raw = _raw_bpm(feats_adj, positions_est, roles)
    ptm = shares.select(["player_id", "team_id", "min"])

    if team_adjust:
        adj = _team_adjust(raw, tl.select(["team_id", "margin"]), ptm)
    else:
        adj = raw.select("player_id", pl.col("raw_obpm").alias("obpm"), pl.col("raw_bpm").alias("bpm"))

    out = (
        adj.join(feats_raw.select(["player_id", "min", "gp"]), on="player_id")
        .with_columns((pl.col("bpm") - pl.col("obpm")).alias("dbpm"))
        .select(
            pl.col("player_id").cast(pl.Int64),
            pl.col("obpm").cast(pl.Float64),
            pl.col("dbpm").cast(pl.Float64),
            pl.col("bpm").cast(pl.Float64),
            pl.col("min").cast(pl.Float64),
            pl.col("gp").cast(pl.Int64),
        )
    )
    return out.to_pandas() if return_as_pandas else out
