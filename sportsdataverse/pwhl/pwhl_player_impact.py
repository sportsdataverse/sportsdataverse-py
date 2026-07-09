"""PWHL by-reference shims over the NHL player-impact spine (Phase 7, Decision D6).

The player-impact **math** (xG feature recipe, weighted-ridge RAPM, GSAx, GAR/WAR
assembly) is league-agnostic; only the constants differ (``LEAGUE_CONSTANTS["pwhl"]``
in ``nhl_player_impact_constants.py``). Mirrors how ``sportsdataverse.wbb`` re-exports
``sportsdataverse.mbb`` by reference.

Coverage caveats (see the design spec's Decision D6 + Open Items):

- **Borrowed xG boosters.** The PWHL has no PWHL-trained xG model; ``pwhl_xg`` scores
  PWHL shots with the same published ``nhl_xg_models`` boosters as the NHL (documented
  approximation -- ``LEAGUE_CONSTANTS["pwhl"].xg_booster_league == "nhl"``).
- **~2 seasons of PWHL history** as of 2026 -- thin sample for any rate-based constant.
- **Thin/absent PWHL shift-chart coverage.** ``sportsdataverse.pwhl`` currently ships
  only per-game live shift/TOI/corsi scrapers (``pwhl_game_shifts`` et al. in
  ``pwhl_analytics.py``), not a season-scale ``load_pwhl_shifts``-equivalent dataset
  loader. The four stint-dependent models (RAPM, line/pair, special teams, GAR/WAR)
  therefore guard on ``shifts.height`` and return a documented empty frame (with a
  ``cli_warn``) rather than fitting a degenerate ridge on insufficient personnel data --
  wiring in a real PWHL shift dataset is the tracked follow-up.
"""

from __future__ import annotations

import functools
from typing import TYPE_CHECKING, Any

import polars as pl

from sportsdataverse._codegen_runtime import cli_warn
from sportsdataverse.nhl.nhl_gsax import nhl_goalie_gsax
from sportsdataverse.nhl.nhl_rapm import nhl_skater_rapm
from sportsdataverse.nhl.nhl_special_teams import nhl_special_teams_value
from sportsdataverse.nhl.nhl_unit_ratings import nhl_unit_ratings
from sportsdataverse.nhl.nhl_war import nhl_skater_war
from sportsdataverse.nhl.nhl_xg import nhl_xg

if TYPE_CHECKING:
    import pandas as pd

__all__ = [
    "pwhl_xg",
    "pwhl_goalie_gsax",
    "pwhl_skater_rapm",
    "pwhl_unit_ratings",
    "pwhl_special_teams_value",
    "pwhl_skater_war",
]

# Placeholder minimum shift-chart row count before a stint-dependent model is considered
# to have "sufficient" PWHL personnel coverage. Revisit once a real PWHL season-scale
# shift dataset is wired in (see the module docstring); until then this just distinguishes
# "empty/near-empty" from "some coverage exists" rather than encoding a fitted threshold.
_MIN_PWHL_SHIFTS_ROWS = 10

#: ① native xG scoring, borrowing the NHL's published boosters (Decision D6).
pwhl_xg = functools.partial(nhl_xg, league="pwhl")
pwhl_xg.__doc__ = """PWHL shim over :func:`sportsdataverse.nhl.nhl_xg.nhl_xg` with ``league='pwhl'``.

Scores PWHL shots with the NHL's published ``nhl_xg_models`` boosters (the PWHL has no
PWHL-trained xG model -- see the module docstring's borrowed-booster caveat).

Args:
    pbp: a PWHL play-by-play frame shaped like ``load_nhl_pbp_full`` (same column
        contract -- ``x``, ``x_fixed``, ``strength_state``, ``home_skaters``/
        ``away_skaters``, ``game_seconds``, ``event_id``, ``secondary_type``,
        ``event_team_abbr``, ``home_abbr``/``away_abbr``, ``season``, ``empty_net``).
    model_dir: booster directory; ``None`` downloads-and-caches on first use.
    return_as_pandas: return a pandas DataFrame instead of polars.

Returns:
    Same schema as :func:`nhl_xg`: the input frame with ``xg:Float64``,
    ``distance_to_net:Float64``, ``shot_angle:Float64``, ``shot_danger:Utf8`` appended.

Example:
    Quick start::

        from sportsdataverse.pwhl.pwhl_player_impact import pwhl_xg
        scored = pwhl_xg(pwhl_pbp, model_dir="tests/fixtures/nhl_player_impact/xg_models")
"""

#: ④ goalie GSAx, closed-form off the borrowed-booster xG.
pwhl_goalie_gsax = functools.partial(nhl_goalie_gsax, league="pwhl")
pwhl_goalie_gsax.__doc__ = """PWHL shim over :func:`sportsdataverse.nhl.nhl_gsax.nhl_goalie_gsax` with ``league='pwhl'``.

Args:
    pbp: a PWHL play-by-play frame shaped like ``load_nhl_pbp_full``.
    shifts: a PWHL shift-chart frame shaped like ``load_nhl_shifts`` (currently unused --
        see :func:`nhl_goalie_gsax`'s TOI-proxy note).
    model_dir: booster directory passed through to the borrowed NHL xG boosters.
    return_as_pandas: return a pandas DataFrame instead of polars.

Returns:
    Same schema as :func:`nhl_goalie_gsax`: ``player_id:Int64, goalie:Utf8, shots:Int64,
    xga:Float64, ga:Int64, gsax:Float64, gsax_per_60:Float64``.

Example:
    Quick start::

        from sportsdataverse.pwhl.pwhl_player_impact import pwhl_goalie_gsax
        gsax = pwhl_goalie_gsax(pwhl_pbp, pwhl_shifts)
"""


def _insufficient_pwhl_shifts(shifts: pl.DataFrame, *, fn_name: str) -> bool:
    insufficient = shifts.height < _MIN_PWHL_SHIFTS_ROWS
    if insufficient:
        cli_warn(
            f"{fn_name}: insufficient PWHL shift-chart coverage "
            f"({shifts.height} rows < {_MIN_PWHL_SHIFTS_ROWS}) -- returning an empty "
            "frame rather than fitting a degenerate ridge/aggregation. See "
            "sportsdataverse.pwhl.pwhl_player_impact's module docstring."
        )
    return insufficient


def pwhl_skater_rapm(
    pbp: pl.DataFrame, shifts: pl.DataFrame, *, model_dir: "str | None" = None, **kwargs: Any
) -> "pl.DataFrame | pd.DataFrame":
    """② PWHL skater xG RAPM -- shim over :func:`nhl_skater_rapm` with ``league='pwhl'``.

    Guards on PWHL shift-chart coverage (see the module docstring); returns a documented
    empty frame + ``cli_warn`` rather than fitting a degenerate ridge when ``shifts`` is
    too thin.

    Args:
        pbp: a PWHL play-by-play frame shaped like ``load_nhl_pbp_full``.
        shifts: a PWHL shift-chart frame shaped like ``load_nhl_shifts``.
        model_dir: booster directory passed through to the borrowed NHL xG boosters.
        **kwargs: forwarded to :func:`nhl_skater_rapm` (e.g. ``lam``, ``as_of``,
            ``strength_states``, ``return_as_pandas``).

    Returns:
        Same schema as :func:`nhl_skater_rapm`: ``player_id:Int64,
        xg_rapm_off:Float64, xg_rapm_def:Float64, xg_rapm:Float64,
        toi_minutes:Float64``. A zero-row frame with this schema (+ a ``cli_warn``) when
        PWHL shift coverage is insufficient.

    Example:
        Quick start::

            from sportsdataverse.pwhl.pwhl_player_impact import pwhl_skater_rapm
            rapm = pwhl_skater_rapm(pbp, shifts)
    """
    if _insufficient_pwhl_shifts(shifts, fn_name="pwhl_skater_rapm"):
        return nhl_skater_rapm(pl.DataFrame(), pl.DataFrame())
    return nhl_skater_rapm(pbp, shifts, model_dir=model_dir, league="pwhl", **kwargs)


def pwhl_unit_ratings(
    pbp: pl.DataFrame, shifts: pl.DataFrame, *, model_dir: "str | None" = None, **kwargs: Any
) -> "pl.DataFrame | pd.DataFrame":
    """⑤ PWHL line/pair ratings -- shim over :func:`nhl_unit_ratings` with ``league='pwhl'``.

    Guards on PWHL shift-chart coverage (see the module docstring).

    Args:
        pbp: a PWHL play-by-play frame shaped like ``load_nhl_pbp_full``.
        shifts: a PWHL shift-chart frame shaped like ``load_nhl_shifts``.
        model_dir: booster directory passed through to the borrowed NHL xG boosters.
        **kwargs: forwarded to :func:`nhl_unit_ratings` (e.g. ``unit_type``,
            ``min_toi``, ``return_as_pandas``).

    Returns:
        Same schema as :func:`nhl_unit_ratings`: ``team:Utf8, unit_ids:Utf8,
        unit_players:Utf8, toi_minutes:Float64, on_ice_xgf:Float64, on_ice_xga:Float64,
        on_ice_xgf_pct:Float64, summed_rapm:Float64, unit_value:Float64``. A zero-row
        frame with this schema (+ a ``cli_warn``) when PWHL shift coverage is
        insufficient.

    Example:
        Quick start::

            from sportsdataverse.pwhl.pwhl_player_impact import pwhl_unit_ratings
            units = pwhl_unit_ratings(pbp, shifts)
    """
    if _insufficient_pwhl_shifts(shifts, fn_name="pwhl_unit_ratings"):
        return nhl_unit_ratings(pl.DataFrame(), pl.DataFrame())
    return nhl_unit_ratings(pbp, shifts, model_dir=model_dir, league="pwhl", **kwargs)


def pwhl_special_teams_value(
    pbp: pl.DataFrame, shifts: pl.DataFrame, *, model_dir: "str | None" = None, **kwargs: Any
) -> "pl.DataFrame | pd.DataFrame":
    """⑥ PWHL special-teams value -- shim over :func:`nhl_special_teams_value` with ``league='pwhl'``.

    Guards on PWHL shift-chart coverage (see the module docstring).

    Args:
        pbp: a PWHL play-by-play frame shaped like ``load_nhl_pbp_full``.
        shifts: a PWHL shift-chart frame shaped like ``load_nhl_shifts``.
        model_dir: booster directory passed through to the borrowed NHL xG boosters.
        **kwargs: forwarded to :func:`nhl_special_teams_value` (e.g.
            ``return_as_pandas``).

    Returns:
        Same schema as :func:`nhl_special_teams_value`: ``player_id:Int64,
        pp_toi_minutes:Float64, pk_toi_minutes:Float64, pp_value:Float64,
        pk_value:Float64``. A zero-row frame with this schema (+ a ``cli_warn``) when
        PWHL shift coverage is insufficient.

    Example:
        Quick start::

            from sportsdataverse.pwhl.pwhl_player_impact import pwhl_special_teams_value
            st = pwhl_special_teams_value(pbp, shifts)
    """
    if _insufficient_pwhl_shifts(shifts, fn_name="pwhl_special_teams_value"):
        return nhl_special_teams_value(pl.DataFrame(), pl.DataFrame())
    return nhl_special_teams_value(pbp, shifts, model_dir=model_dir, league="pwhl", **kwargs)


def pwhl_skater_war(
    pbp: pl.DataFrame, shifts: pl.DataFrame, *, model_dir: "str | None" = None, **kwargs: Any
) -> "pl.DataFrame | pd.DataFrame":
    """③ PWHL GAR/WAR composite -- shim over :func:`nhl_skater_war` with ``league='pwhl'``.

    Guards on PWHL shift-chart coverage (see the module docstring).

    Args:
        pbp: a PWHL play-by-play frame shaped like ``load_nhl_pbp_full``.
        shifts: a PWHL shift-chart frame shaped like ``load_nhl_shifts``.
        model_dir: booster directory passed through to the borrowed NHL xG boosters.
        **kwargs: forwarded to :func:`nhl_skater_war` (e.g. ``return_as_pandas``).

    Returns:
        Same schema as :func:`nhl_skater_war`: ``player_id:Int64, ev_off:Float64,
        ev_def:Float64, pp:Float64, pk:Float64, pens:Float64, faceoffs:Float64,
        gar:Float64, war:Float64``. A zero-row frame with this schema (+ a
        ``cli_warn``) when PWHL shift coverage is insufficient.

    Example:
        Quick start::

            from sportsdataverse.pwhl.pwhl_player_impact import pwhl_skater_war
            war = pwhl_skater_war(pbp, shifts)
    """
    if _insufficient_pwhl_shifts(shifts, fn_name="pwhl_skater_war"):
        return nhl_skater_war(pl.DataFrame(), pl.DataFrame())
    return nhl_skater_war(pbp, shifts, model_dir=model_dir, league="pwhl", **kwargs)
