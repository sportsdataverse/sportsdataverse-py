"""On-ice line/pair ratings -- summed-member RAPM + observed on-ice xG (Decision D4).

Reuses ``build_stints`` + ``nhl_skater_rapm`` (Phase 3) rather than fitting a second,
standalone ridge with unit-level indicators: unit indicators are collinear with their
member indicators, so a second ridge would be degenerate (Decision D4 in the design
spec). A unit's ``unit_value`` is an empirical-Bayes shrinkage blend of its members'
summed skater-RAPM toward the team mean, weighted by the unit's time-on-ice-together.

Data-availability caveat: the sdv-py NHL loader surface does not currently carry a
per-shift player-position (forward/defenseman) label wired into the stint builder, so
``unit_type`` selects a **co-occurrence group size** (3 for ``"forward_line"``, 2 for
``"defense_pair"``) over the *observed* on-ice skater combinations, rather than a
position-verified forward trio / defense pair. This is the standard
"who-plays-together-most" definition used when position labels aren't available and is
documented here rather than silently assumed accurate; wiring in
``load_nhl_skater_boxscores``' ``position``/``position_code`` column is a follow-up.
"""

from __future__ import annotations

from itertools import combinations
from math import comb
from typing import TYPE_CHECKING

import polars as pl

from sportsdataverse.nhl.nhl_rapm import build_stints, nhl_skater_rapm
from sportsdataverse.nhl.nhl_xg import nhl_xg

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["nhl_unit_ratings"]

_UNIT_SCHEMA = {
    "team": pl.Utf8,
    "unit_ids": pl.Utf8,
    "unit_players": pl.Utf8,
    "toi_minutes": pl.Float64,
    "on_ice_xgf": pl.Float64,
    "on_ice_xga": pl.Float64,
    "on_ice_xgf_pct": pl.Float64,
    "summed_rapm": pl.Float64,
    "unit_value": pl.Float64,
}

# Empirical-Bayes shrinkage constant (minutes) -- a unit with `toi_minutes == K` gets
# blended 50/50 with the team mean; more TOI-together shrinks less. Chosen as a round
# number in the same order of magnitude as a full-season top-line's TOI-together
# (fitting this from data is a documented follow-up, not yet backed by a `dev/` script).
_SHRINKAGE_K_MINUTES = 30.0


def _team_label_map(scored: pl.DataFrame) -> dict[int, tuple[str, str]]:
    if scored.height == 0 or "home_abbr" not in scored.columns:
        return {}
    game_teams = (
        scored.filter(pl.col("home_abbr").is_not_null())
        .group_by("game_id")
        .agg(home_abbr=pl.col("home_abbr").first(), away_abbr=pl.col("away_abbr").first())
    )
    return {row["game_id"]: (row["home_abbr"], row["away_abbr"]) for row in game_teams.to_dicts()}


def nhl_unit_ratings(
    pbp: pl.DataFrame,
    shifts: pl.DataFrame,
    *,
    model_dir: "str | None" = None,
    league: str = "nhl",
    unit_type: str = "forward_line",
    min_toi: float = 20.0,
    return_as_pandas: bool = False,
    _stints: pl.DataFrame | None = None,
    _rapm: pl.DataFrame | None = None,
) -> "pl.DataFrame | pd.DataFrame":
    """Per on-ice skater combination: observed xGF/xGA + shrinkage-blended summed RAPM.

    Args:
        pbp: a ``load_nhl_pbp_full``-shaped frame.
        shifts: a ``load_nhl_shifts``-shaped frame.
        model_dir: passed through to ``nhl_xg``/``nhl_skater_rapm``.
        league: ``"nhl"`` or ``"pwhl"``.
        unit_type: ``"forward_line"`` (3-skater combinations) or ``"defense_pair"``
            (2-skater combinations) -- see the module's data-availability caveat.
        min_toi: minimum minutes-together for a unit to be reported.
        return_as_pandas: return a pandas DataFrame instead of polars.
        _stints: internal test hook -- inject a pre-built stints frame.
        _rapm: internal test hook -- inject a pre-built skater-RAPM frame (paired with
            ``_stints``; both must be given together to bypass real computation).

    Returns:
        polars.DataFrame: ``team:Utf8, unit_ids:Utf8 (sorted "id-id-id"),
        unit_players:Utf8, toi_minutes:Float64, on_ice_xgf:Float64, on_ice_xga:Float64,
        on_ice_xgf_pct:Float64, summed_rapm:Float64, unit_value:Float64``. Empty input
        returns a zero-row frame with this schema.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nhl.nhl_unit_ratings import nhl_unit_ratings
            pbp = pl.read_parquet("tests/fixtures/nhl_player_impact/pbp_sample.parquet")
            shifts = pl.read_parquet("tests/fixtures/nhl_player_impact/shifts_sample.parquet")
            units = nhl_unit_ratings(pbp, shifts, model_dir="tests/fixtures/nhl_player_impact/xg_models")
            print(units.sort("unit_value", descending=True).head(10))
    """
    size = 3 if unit_type == "forward_line" else 2

    if _stints is not None and _rapm is not None:
        stints, rapm, team_labels = _stints, _rapm, {}
    else:
        if pbp.height == 0 or shifts.height == 0:
            return pl.DataFrame(schema=_UNIT_SCHEMA)
        scored = nhl_xg(pbp, model_dir=model_dir, league=league)
        stints = build_stints(shifts, scored)
        rapm = nhl_skater_rapm(pbp, shifts, model_dir=model_dir, league=league, _stints=stints)
        team_labels = _team_label_map(scored)

    if stints.height == 0 or rapm.height == 0:
        return pl.DataFrame(schema=_UNIT_SCHEMA)

    rapm_map = dict(zip(rapm["player_id"].to_list(), rapm["xg_rapm"].to_list()))

    agg: dict[tuple[str, tuple[int, ...]], dict[str, float]] = {}
    for rec in stints.to_dicts():
        home_abbr, away_abbr = team_labels.get(rec["game_id"], ("home", "away"))
        duration = rec["duration"]
        if duration <= 0:
            continue
        for side_ids_col, xgf_key, xga_key, team_label in (
            ("home_ids", "xgf_home", "xgf_away", home_abbr),
            ("away_ids", "xgf_away", "xgf_home", away_abbr),
        ):
            ids = sorted(rec[side_ids_col] or [])
            if len(ids) < size:
                continue
            xgf = float(rec[xgf_key] or 0.0)
            xga = float(rec[xga_key] or 0.0)
            n_combos = comb(len(ids), size)
            for combo in combinations(ids, size):
                key = (team_label, combo)
                slot = agg.setdefault(key, {"toi": 0.0, "xgf": 0.0, "xga": 0.0})
                # Split credit evenly across every combo active in the same stint so a
                # 5-skater on-ice group's xG isn't multiply-counted across its C(5,size) subsets.
                slot["toi"] += duration
                slot["xgf"] += xgf / n_combos
                slot["xga"] += xga / n_combos

    if not agg:
        return pl.DataFrame(schema=_UNIT_SCHEMA)

    rows = []
    for (team, combo), slot in agg.items():
        toi_minutes = slot["toi"] / 60.0
        if toi_minutes < min_toi:
            continue
        summed_rapm = sum(rapm_map.get(pid, 0.0) for pid in combo)
        denom = slot["xgf"] + slot["xga"]
        xgf_pct = slot["xgf"] / denom if denom > 0 else 0.5
        rows.append(
            {
                "team": team,
                "unit_ids": "-".join(str(p) for p in combo),
                "unit_players": "-".join(str(p) for p in combo),
                "toi_minutes": toi_minutes,
                "on_ice_xgf": slot["xgf"],
                "on_ice_xga": slot["xga"],
                "on_ice_xgf_pct": xgf_pct,
                "summed_rapm": summed_rapm,
            }
        )

    if not rows:
        return pl.DataFrame(schema=_UNIT_SCHEMA)

    out = pl.DataFrame(rows)
    team_mean = out.group_by("team").agg(team_mean_rapm=pl.col("summed_rapm").mean())
    out = out.join(team_mean, on="team", how="left")
    shrink_w = pl.col("toi_minutes") / (pl.col("toi_minutes") + _SHRINKAGE_K_MINUTES)
    out = out.with_columns(
        unit_value=(shrink_w * pl.col("summed_rapm") + (1 - shrink_w) * pl.col("team_mean_rapm"))
    ).drop("team_mean_rapm")
    out = out.select(list(_UNIT_SCHEMA.keys())).sort("unit_value", descending=True)
    return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out
