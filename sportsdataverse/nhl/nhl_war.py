"""GAR / WAR composite -- assembles per-skater components (in goals) into WAR.

``gar = Sigma(component - replacement)`` across EV offense/defense (rate x EV TOI,
via ``nhl_skater_rapm`` restricted to 5v5 stints so it doesn't overlap with the PP/PK
components), special teams (``nhl_special_teams_value``), faceoffs, and penalties
(drawn - taken); ``war = gar / goals_per_win``. Follows the published EvolvingHockey
GAR/WAR methodology; no license obligation (see ``NOTICE``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from sportsdataverse.nhl.nhl_player_impact_constants import get_constants
from sportsdataverse.nhl.nhl_rapm import nhl_skater_rapm
from sportsdataverse.nhl.nhl_special_teams import nhl_special_teams_value

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["nhl_skater_war"]

_WAR_SCHEMA = {
    "player_id": pl.Int64,
    "ev_off": pl.Float64,
    "ev_def": pl.Float64,
    "pp": pl.Float64,
    "pk": pl.Float64,
    "pens": pl.Float64,
    "faceoffs": pl.Float64,
    "gar": pl.Float64,
    "war": pl.Float64,
}


def _faceoff_penalty_components(pbp: pl.DataFrame, *, league: str = "nhl") -> pl.DataFrame:
    """Faceoff-win-share and penalty-differential GAR components (in goals).

    Args:
        pbp: a ``load_nhl_pbp_full``-shaped frame carrying ``FACEOFF``/``PENALTY``
            events with ``event_player_1_id``/``event_player_1_type`` (``"Winner"``/
            ``"PenaltyOn"``) and ``event_player_2_id``/``event_player_2_type``
            (``"Loser"``/``"DrewBy"``).
        league: ``"nhl"`` or ``"pwhl"`` -- selects ``faceoff_goal_weight`` /
            ``penalty_goal_weight`` via ``LEAGUE_CONSTANTS``.

    Returns:
        polars.DataFrame: ``player_id:Int64, faceoffs_goals:Float64,
        pens_goals:Float64``. ``faceoffs_goals = (fo_won - 0.5*fo_total) *
        faceoff_goal_weight``; ``pens_goals = (pens_drawn - pens_taken) *
        penalty_goal_weight``. Empty input returns a zero-row frame with this schema.

    Example:
        Quick start::

            from sportsdataverse.nhl.nhl_war import _faceoff_penalty_components
            comp = _faceoff_penalty_components(pbp)
    """
    schema = {"player_id": pl.Int64, "faceoffs_goals": pl.Float64, "pens_goals": pl.Float64}
    if pbp.height == 0:
        return pl.DataFrame(schema=schema)

    cfg = get_constants(league)
    pbp = pbp.with_columns(pl.col("event_player_1_id").cast(pl.Int64), pl.col("event_player_2_id").cast(pl.Int64))

    # pl.len() aggregates as an unsigned integer type -- subtracting two unsigned
    # counts (e.g. pens_drawn - pens_taken) BEFORE casting wraps around to a huge
    # positive number whenever the minuend is smaller (unsigned underflow), rather
    # than raising. Cast every count to a signed Int64 immediately after the
    # group_by, before any subtraction.
    fo = pbp.filter(pl.col("event_type") == "FACEOFF")
    won = (
        fo.group_by("event_player_1_id").agg(fo_won=pl.len().cast(pl.Int64)).rename({"event_player_1_id": "player_id"})
    )
    lost = (
        fo.group_by("event_player_2_id").agg(fo_lost=pl.len().cast(pl.Int64)).rename({"event_player_2_id": "player_id"})
    )
    fo_tab = won.join(lost, on="player_id", how="full", coalesce=True).fill_null(0)
    fo_tab = fo_tab.with_columns(fo_total=(pl.col("fo_won") + pl.col("fo_lost")))
    fo_tab = fo_tab.with_columns(
        faceoffs_goals=((pl.col("fo_won") - 0.5 * pl.col("fo_total")) * cfg.faceoff_goal_weight)
    ).select("player_id", "faceoffs_goals")

    pen = pbp.filter(pl.col("event_type") == "PENALTY")
    taken = (
        pen.group_by("event_player_1_id")
        .agg(pens_taken=pl.len().cast(pl.Int64))
        .rename({"event_player_1_id": "player_id"})
    )
    drawn = (
        pen.group_by("event_player_2_id")
        .agg(pens_drawn=pl.len().cast(pl.Int64))
        .rename({"event_player_2_id": "player_id"})
    )
    pen_tab = taken.join(drawn, on="player_id", how="full", coalesce=True).fill_null(0)
    pen_tab = pen_tab.with_columns(
        pens_goals=((pl.col("pens_drawn") - pl.col("pens_taken")) * cfg.penalty_goal_weight)
    ).select("player_id", "pens_goals")

    out = fo_tab.join(pen_tab, on="player_id", how="full", coalesce=True).fill_null(0.0)
    # A `full` join of two possibly-empty group-bys can leave `player_id` as Float64
    # (polars' inferred type for an empty numeric column) -- pin it back to Int64
    # before returning so downstream joins (e.g. nhl_skater_war's) don't dtype-mismatch.
    out = out.with_columns(pl.col("player_id").cast(pl.Int64))
    return out.select("player_id", "faceoffs_goals", "pens_goals")


def nhl_skater_war(
    pbp: pl.DataFrame,
    shifts: pl.DataFrame,
    *,
    model_dir: "str | None" = None,
    league: str = "nhl",
    return_as_pandas: bool = False,
) -> "pl.DataFrame | pd.DataFrame":
    """Per-skater GAR/WAR composite -- EV + special-teams + faceoffs + penalties.

    Args:
        pbp: a ``load_nhl_pbp_full``-shaped frame.
        shifts: a ``load_nhl_shifts``-shaped frame.
        model_dir: passed through to ``nhl_xg``/``nhl_skater_rapm``/
            ``nhl_special_teams_value``.
        league: ``"nhl"`` or ``"pwhl"``.
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame: ``player_id:Int64, ev_off:Float64, ev_def:Float64, pp:Float64,
        pk:Float64, pens:Float64, faceoffs:Float64, gar:Float64, war:Float64``.
        ``ev_off``/``ev_def`` are ``(5v5-only RAPM rate - replacement level) * EV
        TOI/60``; ``gar`` sums every component; ``war = gar / goals_per_win``. Empty
        input returns a zero-row frame with this schema.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nhl.nhl_war import nhl_skater_war
            pbp = pl.read_parquet("tests/fixtures/nhl_player_impact/pbp_sample.parquet")
            shifts = pl.read_parquet("tests/fixtures/nhl_player_impact/shifts_sample.parquet")
            war = nhl_skater_war(pbp, shifts, model_dir="tests/fixtures/nhl_player_impact/xg_models")
            print(war.sort("war", descending=True).head(10))

    See Also:
        * `EvolvingHockey`_ -- concurrent-validity oracle (data-blocked as of 2026-07-08;
          see the fixture README).

    .. _EvolvingHockey: https://evolving-hockey.com
    """
    if pbp.height == 0 or shifts.height == 0:
        return pl.DataFrame(schema=_WAR_SCHEMA)

    cfg = get_constants(league)
    ev_rapm = nhl_skater_rapm(pbp, shifts, model_dir=model_dir, league=league, strength_states=["5v5"])
    st = nhl_special_teams_value(pbp, shifts, model_dir=model_dir, league=league)
    fp = _faceoff_penalty_components(pbp, league=league)

    if ev_rapm.height == 0:
        return pl.DataFrame(schema=_WAR_SCHEMA)

    out = ev_rapm.select("player_id", "xg_rapm_off", "xg_rapm_def", "toi_minutes")
    for other, name in ((st, "nhl_special_teams_value"), (fp, "_faceoff_penalty_components")):
        assert out.schema["player_id"] == other.schema["player_id"], (
            f"player_id dtype mismatch joining {name}: {out.schema['player_id']} vs {other.schema['player_id']}"
        )
    out = out.join(st, on="player_id", how="full", coalesce=True)
    out = out.join(fp, on="player_id", how="full", coalesce=True)
    out = out.with_columns(
        pl.col("xg_rapm_off").fill_null(0.0),
        pl.col("xg_rapm_def").fill_null(0.0),
        pl.col("toi_minutes").fill_null(0.0),
        pl.col("pp_value").fill_null(0.0),
        pl.col("pk_value").fill_null(0.0),
        pl.col("faceoffs_goals").fill_null(0.0),
        pl.col("pens_goals").fill_null(0.0),
    )
    out = out.with_columns(
        ev_off=((pl.col("xg_rapm_off") - cfg.replacement_ev_off) * pl.col("toi_minutes") / 60.0),
        ev_def=((pl.col("xg_rapm_def") - cfg.replacement_ev_def) * pl.col("toi_minutes") / 60.0),
        pp=pl.col("pp_value"),
        pk=pl.col("pk_value"),
        pens=pl.col("pens_goals"),
        faceoffs=pl.col("faceoffs_goals"),
    )
    out = out.with_columns(
        gar=(pl.col("ev_off") + pl.col("ev_def") + pl.col("pp") + pl.col("pk") + pl.col("pens") + pl.col("faceoffs"))
    )
    out = out.with_columns(war=(pl.col("gar") / cfg.goals_per_win))
    out = out.select(list(_WAR_SCHEMA.keys())).sort("war", descending=True)
    return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out
