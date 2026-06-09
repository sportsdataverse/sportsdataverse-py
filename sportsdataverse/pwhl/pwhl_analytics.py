"""PWHL game analytics: shifts, time-on-ice, and player-level on-ice Corsi.

``hockeytech_api`` is imported at module level so tests can monkeypatch it
via ``monkeypatch.setattr(an, "hockeytech_api", ...)``.

Note on Corsi/Fenwick: the HockeyTech feed has no missed-shot event, so both
metrics are proxies — every output row carries ``corsi_includes_missed = False``.
"""

from __future__ import annotations

from typing import Any

import polars as pl

from sportsdataverse.hockeytech import hockeytech_api
from sportsdataverse.hockeytech._analytics import corsi_fenwick_on_ice, per60, player_toi
from sportsdataverse.hockeytech._parsers import parse_shifts

__all__ = ["pwhl_game_shifts", "pwhl_player_toi", "pwhl_game_corsi"]

_LG = "pwhl"


def pwhl_game_shifts(game_id: Any, return_as_pandas: bool = False) -> Any:
    """Parsed shift stints for a single PWHL game.

    Calls the HockeyTech ``modulekit/gameshifts`` endpoint and returns one
    row per player-shift stint via :func:`~sportsdataverse.hockeytech._parsers.parse_shifts`.

    Parameters
    ----------
    game_id:
        HockeyTech game identifier (integer or string).
    return_as_pandas:
        If ``True``, return a :class:`pandas.DataFrame` instead of a
        :class:`polars.DataFrame`.

    Returns
    -------
    polars.DataFrame or pandas.DataFrame
        Columns include ``player_id``, ``first_name``, ``last_name``,
        ``home``, ``period``, ``start_time``, ``end_time``, ``start_s``,
        ``end_s``, ``goal_on_shift``, ``penalty_on_shift``.
    """
    payload = hockeytech_api(_LG, "modulekit", "gameshifts", {"game_id": game_id})
    shifts = parse_shifts(payload, game_id=game_id)
    if return_as_pandas:
        return shifts.to_pandas()
    return shifts


def pwhl_player_toi(game_id: Any, return_as_pandas: bool = False) -> Any:
    """Per-player time-on-ice totals for a single PWHL game.

    Fetches shifts via :func:`pwhl_game_shifts` then aggregates via
    :func:`~sportsdataverse.hockeytech._analytics.player_toi`.

    Parameters
    ----------
    game_id:
        HockeyTech game identifier (integer or string).
    return_as_pandas:
        If ``True``, return a :class:`pandas.DataFrame` instead of a
        :class:`polars.DataFrame`.

    Returns
    -------
    polars.DataFrame or pandas.DataFrame
        One row per player with ``player_id``, ``first_name``, ``last_name``,
        ``toi_seconds``, ``num_shifts``, ``avg_shift_s``, sorted by
        ``toi_seconds`` descending.
    """
    shifts = pwhl_game_shifts(game_id)
    toi = player_toi(shifts)
    if return_as_pandas:
        return toi.to_pandas()
    return toi


def pwhl_game_corsi(game_id: Any, return_as_pandas: bool = False) -> Any:
    """Player-level on-ice Corsi and Fenwick for a single PWHL game.

    Computes shot-attempt counts for every player found on ice during a
    shot/blocked_shot/goal event, then joins their time-on-ice so per-60
    rates are available.

    **Corsi/Fenwick note**: the HockeyTech feed has no missed-shot event,
    so both metrics are proxies that count only shot + blocked_shot + goal.
    Every output row carries ``corsi_includes_missed = False``.

    Parameters
    ----------
    game_id:
        HockeyTech game identifier (integer or string).
    return_as_pandas:
        If ``True``, return a :class:`pandas.DataFrame` instead of a
        :class:`polars.DataFrame`.

    Returns
    -------
    polars.DataFrame or pandas.DataFrame
        One row per on-ice player with columns:

        - ``player_id`` (Utf8)
        - ``corsi_for``, ``corsi_against`` (Int64)
        - ``corsi_for_pct`` (Float64)
        - ``fenwick_for``, ``fenwick_against`` (Int64)
        - ``fenwick_for_pct`` (Float64)
        - ``toi_seconds`` (Int64, from shifts; null if player not in shift data)
        - ``corsi_for_per60`` (Float64)
        - ``corsi_includes_missed`` (Boolean, always False)
    """
    from sportsdataverse.pwhl.pwhl_api import pwhl_pbp  # noqa: PLC0415 (local import avoids circular at module init)

    pbp = pwhl_pbp(game_id)
    corsi = corsi_fenwick_on_ice(pbp)

    # Fetch TOI (reuses hockeytech_api via pwhl_game_shifts under the hood)
    toi = pwhl_player_toi(game_id)

    # LEFT-JOIN toi_seconds onto corsi so every on-ice player is present
    # even if they have no shift data (null toi_seconds).
    toi_sel = toi.select(
        pl.col("player_id").cast(pl.Utf8),
        pl.col("toi_seconds"),
    )
    out = corsi.join(toi_sel, on="player_id", how="left")

    # Add corsi_for_per60 (null when toi_seconds is null or zero)
    out = out.with_columns(
        pl.when(pl.col("toi_seconds").is_not_null() & (pl.col("toi_seconds") > 0))
        .then(per60("corsi_for"))
        .otherwise(pl.lit(None, dtype=pl.Float64))
        .alias("corsi_for_per60")
    )

    if return_as_pandas:
        return out.to_pandas()
    return out
