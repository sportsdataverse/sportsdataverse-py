"""Special-teams (PP/PK) per-skater value -- on-ice rate above/below league baseline.

Reuses ``build_stints`` filtered to power-play / penalty-kill strength states. A
stint's ``strength_state`` (``"{home_skaters}v{away_skaters}"``) determines which side
is on the power play (more skaters) and which is shorthanded. A unit's value is its xGF
above ``league_xg_rate_pp`` (PP) or its xGA suppressed below ``league_xg_rate_pk`` (PK, so
*suppression* is positive value, matching the EV defense-RAPM sign convention), and each
skater on the ice takes an equal share of it: a fifth on a 5v4 power play, a quarter on
the 4v5 kill. The shares add up to the unit's value, so ``nhl_skater_war`` counts each
special-teams goal once, as the 5v5 RAPM does (``ImpactConfig.replacement_rapm``).

The baseline is the league average, not a replacement level: special-teams minutes go to
players a coach already rates above replacement, and no PP/PK replacement rate is fitted.
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
    xg_scored: bool = False,
    _stints: pl.DataFrame | None = None,
) -> "pl.DataFrame | pd.DataFrame":
    """Per-skater share of power-play/penalty-kill value (goals) above/below the league rate.

    Args:
        pbp: a ``load_nhl_pbp_full``-shaped frame.
        shifts: a ``load_nhl_shifts``-shaped frame.
        model_dir: passed through to ``nhl_xg``.
        league: ``"nhl"`` or ``"pwhl"`` -- selects ``league_xg_rate_pp``/``_pk`` via
            ``LEAGUE_CONSTANTS``.
        return_as_pandas: return a pandas DataFrame instead of polars.
        xg_scored: ``pbp`` already carries the shot ``xg`` to use (another league's own xG
            model); skip re-scoring with the NHL boosters, which would replace it.
        _stints: internal test hook -- inject a pre-built stints frame.

    Returns:
        polars.DataFrame: ``player_id:Int64, pp_toi_minutes:Float64,
        pk_toi_minutes:Float64, pp_value:Float64, pk_value:Float64``. ``*_toi_minutes`` is
        the skater's own time on the ice; ``pp_value``/``pk_value`` are goals above the league
        rate, each skater carrying an equal share of their unit's (a fifth on a 5v4 power
        play), so a unit's skaters sum to its value. Empty input returns a zero-row frame with
        this schema.

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
        scored = pbp if xg_scored else nhl_xg(pbp, model_dir=model_dir, league=league)
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

        # Each skater takes 1/n of the unit's seconds and xG, so the unit's value is split, not
        # copied to all n (which counted every PP goal about 5x and PK goal about 4x in GAR).
        for side, stats in ((pp_side, pp_stats), (pk_side, pk_stats)):
            ids = rec[side] or []
            for pid in ids:
                slot = stats.setdefault(pid, {"toi": 0.0, "share_s": 0.0, "share_xg": 0.0})
                slot["toi"] += duration
                slot["share_s"] += duration / len(ids)
                slot["share_xg"] += float(xgf or 0.0) / len(ids)

    all_players = set(pp_stats) | set(pk_stats)
    if not all_players:
        return pl.DataFrame(schema=_ST_SCHEMA)

    rows = []
    for pid in all_players:
        empty = {"toi": 0.0, "share_s": 0.0, "share_xg": 0.0}
        pp = pp_stats.get(pid, empty)
        pk = pk_stats.get(pid, empty)
        rows.append(
            {
                "player_id": pid,
                "pp_toi_minutes": pp["toi"] / 60.0,
                "pk_toi_minutes": pk["toi"] / 60.0,
                "pp_value": pp["share_xg"] - cfg.league_xg_rate_pp * pp["share_s"] / 3600.0,
                "pk_value": cfg.league_xg_rate_pk * pk["share_s"] / 3600.0 - pk["share_xg"],
            }
        )

    out = pl.DataFrame(rows, schema=_ST_SCHEMA).sort("pp_value", descending=True)
    return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out
