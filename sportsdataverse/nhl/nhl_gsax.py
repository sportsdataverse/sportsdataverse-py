"""Goalie GSAx (goals saved above expected) -- a closed-form aggregation off native xG.

For every unblocked shot on goal, ``gsax = xga - ga`` where ``xga = sum(xg)`` (expected
goals against) and ``ga = count(goals)`` (actual goals against), attributed to the
defending team's on-ice goalie. League-wide ``sum(gsax) ~= 0`` at large sample: the
algebraic identity is ``sum(gsax) == sum(xga) - sum(goals)``, which is exactly zero
only under perfect league-wide xG calibration (``sum(xg) == goals``), so the gate in
``test_nhl_player_impact_oracle.py`` treats it as approximate within a documented
tolerance -- still a real check, because an attribution bug that double-counts or drops
shots pushes it far past that band.

Follows the published GSAx methodology (EvolvingHockey, MoneyPuck); no license
obligation (see ``NOTICE``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from sportsdataverse.nhl.nhl_xg import nhl_xg

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["nhl_goalie_gsax"]

_GSAX_SCHEMA = {
    "player_id": pl.Int64,
    "goalie": pl.Utf8,
    "shots": pl.Int64,
    "xga": pl.Float64,
    "ga": pl.Int64,
    "gsax": pl.Float64,
    "gsax_per_60": pl.Float64,
}


def _attribute_goalie(scored: pl.DataFrame) -> pl.DataFrame:
    """Attribute each shot to the *defending* team's on-ice goalie.

    Prefers ``load_nhl_pbp_full``'s own ``home_goalie_id``/``away_goalie_id`` columns
    (the on-ice goalie at that event) over deriving goalie identity from shift-chart
    personnel, per the design spec's Open Item ("prefer a pbp goalie column if
    ``load_nhl_pbp`` carries one").

    Args:
        scored: an ``nhl_xg``-scored frame carrying ``event_team_abbr``, ``home_abbr``,
            ``away_abbr``, ``home_goalie_id``/``away_goalie_id``,
            ``home_goalie``/``away_goalie``.

    Returns:
        ``scored`` with ``defending_goalie_id:Int64`` and ``defending_goalie_name:Utf8``
        appended -- the shooting team's opponent's goalie.

    Example:
        Quick start::

            from sportsdataverse.nhl.nhl_gsax import _attribute_goalie
            out = _attribute_goalie(scored)
    """
    is_home_shooting = pl.col("event_team_abbr") == pl.col("home_abbr")
    return scored.with_columns(
        defending_goalie_id=pl.when(is_home_shooting)
        .then(pl.col("away_goalie_id"))
        .otherwise(pl.col("home_goalie_id"))
        .cast(pl.Int64),
        defending_goalie_name=pl.when(is_home_shooting).then(pl.col("away_goalie")).otherwise(pl.col("home_goalie")),
    )


def _aggregate_gsax(scored: pl.DataFrame) -> pl.DataFrame:
    """Group attributed, xG-scored shots into per-goalie ``xga``/``ga``/``gsax``.

    Args:
        scored: an ``nhl_xg``-scored, ``_attribute_goalie``-attributed frame.

    Returns:
        polars.DataFrame: ``player_id:Int64, goalie:Utf8, shots:Int64, xga:Float64,
        ga:Int64, gsax:Float64`` (``gsax_per_60`` is appended by the public
        ``nhl_goalie_gsax`` once TOI is available).
    """
    attributed = _attribute_goalie(scored)
    unblocked = attributed.filter(
        pl.col("event_type").is_in(["SHOT", "MISSED_SHOT", "GOAL"])
        & pl.col("xg").is_not_null()
        & pl.col("defending_goalie_id").is_not_null()
    )
    if unblocked.height == 0:
        return pl.DataFrame(
            schema={"player_id": pl.Int64, "goalie": pl.Utf8, "shots": pl.Int64, "xga": pl.Float64, "ga": pl.Int64}
        ).with_columns(gsax=pl.lit(None, dtype=pl.Float64))
    return (
        unblocked.group_by("defending_goalie_id", "defending_goalie_name")
        .agg(
            shots=pl.len(),
            xga=pl.col("xg").sum(),
            ga=(pl.col("event_type") == "GOAL").sum().cast(pl.Int64),
        )
        .rename({"defending_goalie_id": "player_id", "defending_goalie_name": "goalie"})
        .with_columns(gsax=(pl.col("xga") - pl.col("ga")))
    )


def _toi_seconds_by_goalie(pbp: pl.DataFrame) -> pl.DataFrame:
    """Approximate each goalie's on-ice seconds from the pbp event span they're credited on.

    For every ``(game_id, side)`` group, TOI is proxied as ``max(game_seconds) -
    min(game_seconds)`` over rows crediting that goalie -- an approximation (it misses
    the small window before the first / after the last logged event) rather than an
    exact shift-derived TOI, documented here rather than silently assumed exact.

    Args:
        pbp: a ``load_nhl_pbp_full``-shaped frame.

    Returns:
        polars.DataFrame: ``player_id:Int64, toi_seconds:Float64``.
    """
    parts = []
    for side in ("home", "away"):
        col = f"{side}_goalie_id"
        if col not in pbp.columns:
            continue
        parts.append(
            pbp.filter(pl.col(col).is_not_null())
            .group_by("game_id", col)
            .agg(
                span=(pl.col("game_seconds").max() - pl.col("game_seconds").min()).cast(pl.Float64),
            )
            .rename({col: "player_id"})
        )
    if not parts:
        return pl.DataFrame(schema={"player_id": pl.Int64, "toi_seconds": pl.Float64})
    return (
        pl.concat(parts)
        .with_columns(pl.col("player_id").cast(pl.Int64))
        .group_by("player_id")
        .agg(toi_seconds=pl.col("span").sum())
    )


def nhl_goalie_gsax(
    pbp: pl.DataFrame,
    shifts: pl.DataFrame,
    *,
    model_dir: "str | None" = None,
    league: str = "nhl",
    return_as_pandas: bool = False,
) -> "pl.DataFrame | pd.DataFrame":
    """Per-goalie goals-saved-above-expected (GSAx) for the games in ``pbp``.

    Scores every unblocked shot via ``nhl_xg``, attributes each shot to the defending
    goalie (``_attribute_goalie``), and aggregates ``xga = sum(xg)``, ``ga =
    count(goals)``, ``gsax = xga - ga``. ``gsax_per_60`` uses an on-ice-seconds proxy
    derived from the pbp event span each goalie is credited on (see
    ``_toi_seconds_by_goalie``) -- ``shifts`` is accepted for interface parity with the
    rest of the player-impact spine but is not currently required for TOI.

    Args:
        pbp: a ``load_nhl_pbp_full``-shaped frame (or an already ``nhl_xg``-scored one --
            re-scoring is idempotent since the prior ``xg`` column is dropped first).
        shifts: a ``load_nhl_shifts``-shaped frame (currently unused; accepted for
            interface parity -- see the module docstring).
        model_dir: passed through to ``nhl_xg`` (booster directory).
        league: ``"nhl"`` or ``"pwhl"``.
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame: ``player_id:Int64, goalie:Utf8, shots:Int64, xga:Float64,
        ga:Int64, gsax:Float64, gsax_per_60:Float64``. League-wide ``sum(gsax) ==
        sum(xga) - sum(goals)``, which is ``~= 0`` at large sample and exactly zero only
        under perfect league-wide xG calibration. Empty/malformed input returns a
        zero-row frame with this schema -- never raises.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nhl.nhl_gsax import nhl_goalie_gsax
            pbp = pl.read_parquet("tests/fixtures/nhl_player_impact/pbp_sample.parquet")
            gsax = nhl_goalie_gsax(pbp, pl.DataFrame(), model_dir="tests/fixtures/nhl_player_impact/xg_models")
            print(gsax.sort("gsax", descending=True))

        Pipeline next step::

            gsax.filter(pl.col("shots") >= 10).sort("gsax_per_60", descending=True).head()

    See Also:
        * `MoneyPuck`_ -- concurrent-validity oracle for GSAx (data-blocked as of
          2026-07-08; see the fixture README).

    .. _MoneyPuck: https://moneypuck.com
    """
    if pbp.height == 0:
        return pl.DataFrame(schema=_GSAX_SCHEMA)

    scored = nhl_xg(pbp, model_dir=model_dir, league=league)
    agg = _aggregate_gsax(scored)
    if agg.height == 0:
        return pl.DataFrame(schema=_GSAX_SCHEMA)

    toi = _toi_seconds_by_goalie(pbp)
    assert agg.schema["player_id"] == toi.schema["player_id"], (
        f"player_id dtype mismatch: gsax={agg.schema['player_id']} vs toi={toi.schema['player_id']}"
    )
    out = (
        agg.join(toi, on="player_id", how="left")
        .with_columns(
            gsax_per_60=pl.when(pl.col("toi_seconds") > 0)
            .then(pl.col("gsax") * 3600.0 / pl.col("toi_seconds"))
            .otherwise(None)
        )
        .drop("toi_seconds")
    )
    out = out.select(list(_GSAX_SCHEMA.keys())).sort("gsax", descending=True)
    return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out
