"""Special-teams (PP/PK) per-skater value -- on-ice rate above/below league baseline.

Reuses ``build_stints`` filtered to power-play / penalty-kill strength states. A
stint's ``strength_state`` (``"{home_skaters}v{away_skaters}"``) determines which side
is on the power play (more skaters) and which is shorthanded; every on-ice skater on the
PP side accrues ``pp_value`` off their unit's xGF rate above ``league_xg_rate_pp``, and
every on-ice skater on the PK side accrues ``pk_value`` off their unit's xGA rate
suppressed below ``league_xg_rate_pk`` (so *suppression* is positive value, matching
the EV defense-RAPM sign convention).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from sportsdataverse.nhl.nhl_player_impact_constants import get_constants
from sportsdataverse.nhl.nhl_rapm import build_stints
from sportsdataverse.nhl.nhl_xg import nhl_xg

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["nhl_special_teams_value"]

_ST_SCHEMA = {
    "player_id": pl.Int64,
    "pp_toi_minutes": pl.Float64,
    "pk_toi_minutes": pl.Float64,
    "pp_value": pl.Float64,
    "pk_value": pl.Float64,
}


def _parse_strength_state(s: str | None) -> tuple[int, int] | None:
    if not s or "v" not in s:
        return None
    a, b = s.split("v", 1)
    try:
        return int(a), int(b)
    except ValueError:
        return None


def nhl_special_teams_value(
    pbp: pl.DataFrame,
    shifts: pl.DataFrame,
    *,
    model_dir: "str | None" = None,
    league: str = "nhl",
    return_as_pandas: bool = False,
    _stints: pl.DataFrame | None = None,
) -> "pl.DataFrame | pd.DataFrame":
    """Per-skater power-play/penalty-kill value (goals) above/below league baseline.

    Args:
        pbp: a ``load_nhl_pbp_full``-shaped frame.
        shifts: a ``load_nhl_shifts``-shaped frame.
        model_dir: passed through to ``nhl_xg``.
        league: ``"nhl"`` or ``"pwhl"`` -- selects ``league_xg_rate_pp``/``_pk`` via
            ``LEAGUE_CONSTANTS``.
        return_as_pandas: return a pandas DataFrame instead of polars.
        _stints: internal test hook -- inject a pre-built stints frame.

    Returns:
        polars.DataFrame: ``player_id:Int64, pp_toi_minutes:Float64,
        pk_toi_minutes:Float64, pp_value:Float64, pk_value:Float64``. Empty input
        returns a zero-row frame with this schema.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nhl.nhl_special_teams import nhl_special_teams_value
            pbp = pl.read_parquet("tests/fixtures/nhl_player_impact/pbp_sample.parquet")
            shifts = pl.read_parquet("tests/fixtures/nhl_player_impact/shifts_sample.parquet")
            st = nhl_special_teams_value(pbp, shifts, model_dir="tests/fixtures/nhl_player_impact/xg_models")
            print(st.sort("pp_value", descending=True).head(10))
    """
    if _stints is not None:
        stints = _stints
    else:
        if pbp.height == 0 or shifts.height == 0:
            return pl.DataFrame(schema=_ST_SCHEMA)
        scored = nhl_xg(pbp, model_dir=model_dir, league=league)
        stints = build_stints(shifts, scored)

    if stints.height == 0:
        return pl.DataFrame(schema=_ST_SCHEMA)

    cfg = get_constants(league)
    pp_stats: dict[int, dict[str, float]] = {}
    pk_stats: dict[int, dict[str, float]] = {}

    for rec in stints.to_dicts():
        counts = _parse_strength_state(rec.get("strength_state"))
        if counts is None:
            continue
        home_skaters, away_skaters = counts
        if home_skaters == away_skaters:
            continue  # even strength -- not a special-teams stint
        duration = rec["duration"]
        if duration <= 0:
            continue

        # The shots taken by the team with the man-advantage are simultaneously "xGF
        # for the PP side" and "xGA against the PK side" -- one xgf value serves both.
        if home_skaters > away_skaters:
            pp_side, pk_side, xgf = "home_ids", "away_ids", rec["xgf_home"]
        else:
            pp_side, pk_side, xgf = "away_ids", "home_ids", rec["xgf_away"]

        for pid in rec[pp_side] or []:
            slot = pp_stats.setdefault(pid, {"toi": 0.0, "xgf": 0.0})
            slot["toi"] += duration
            slot["xgf"] += float(xgf or 0.0)
        for pid in rec[pk_side] or []:
            slot = pk_stats.setdefault(pid, {"toi": 0.0, "xga": 0.0})
            slot["toi"] += duration
            slot["xga"] += float(xgf or 0.0)

    all_players = set(pp_stats) | set(pk_stats)
    if not all_players:
        return pl.DataFrame(schema=_ST_SCHEMA)

    rows = []
    for pid in all_players:
        pp = pp_stats.get(pid, {"toi": 0.0, "xgf": 0.0})
        pk = pk_stats.get(pid, {"toi": 0.0, "xga": 0.0})
        pp_toi_minutes = pp["toi"] / 60.0
        pk_toi_minutes = pk["toi"] / 60.0
        pp_rate = pp["xgf"] * 3600.0 / pp["toi"] if pp["toi"] > 0 else cfg.league_xg_rate_pp
        pk_rate = pk["xga"] * 3600.0 / pk["toi"] if pk["toi"] > 0 else cfg.league_xg_rate_pk
        pp_value = (pp_rate - cfg.league_xg_rate_pp) * pp_toi_minutes / 60.0
        pk_value = (cfg.league_xg_rate_pk - pk_rate) * pk_toi_minutes / 60.0
        rows.append(
            {
                "player_id": pid,
                "pp_toi_minutes": pp_toi_minutes,
                "pk_toi_minutes": pk_toi_minutes,
                "pp_value": pp_value if pp["toi"] > 0 else 0.0,
                "pk_value": pk_value if pk["toi"] > 0 else 0.0,
            }
        )

    out = pl.DataFrame(rows, schema=_ST_SCHEMA).sort("pp_value", descending=True)
    return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out
