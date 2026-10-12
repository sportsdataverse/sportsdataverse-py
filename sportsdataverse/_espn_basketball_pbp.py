"""Pieces shared by the ESPN basketball pbp producers (nba / wnba / mbb / wbb)."""

from __future__ import annotations

import re
from typing import Any

import pandas as pd
import polars as pl

# ESPN play types for a timeout a team called (NCAA 578/579, NBA/WNBA 15/16/17/283).
# Official / TV timeouts (580, 581, 19) belong to no team and are left out. The
# timeouts map built from these holds timeouts CALLED as ESPN logs them, not
# timeouts CHARGED: a coach's challenge outcome is not applied, because ESPN logs
# the challenge's own timeout too inconsistently to correct for (a team timeout
# precedes 90% of charged and 56% of retained NBA 2020-26 challenges, 84% / 22% in
# the WNBA 2023-26, about 5% in NCAA 2025-26). Challenge plays are not timeouts.
TEAM_TIMEOUT_TYPES = ["RegularTimeOut", "ShortTimeOut", "Full Timeout", "Short Timeout", "No Timeout", "Reset Timeout"]


def pickcenter_odds(pickcenter: Any, default_over_under: float) -> dict[str, Any]:
    """Spread, over/under and home favorite from an ESPN summary ``pickcenter`` array.

    Providers are read in ``str(provider.id)`` order, which puts teamrankings
    ("1002") ahead of consensus ("1004") and of Caesars ("45"). That order is kept
    on purpose: where teamrankings and consensus disagree, teamrankings matches the
    winner more often (MBB 23 of 37 games, NBA 39 of 60), and an integer sort would
    move whole seasons (MBB 2021-22) to Caesars' line. The spread and the home
    favorite come from the same row, the first with a spread (a record-only
    teamrankings row has none, and its favorite flag is False for both teams).
    The over/under is the first non-null one. One provider is enough; with no
    spread anywhere the defaults (2.5, home favored, unavailable) stand.
    """
    pc = pd.json_normalize(pickcenter or [])
    if "spread" not in pc.columns or not pc["spread"].notnull().any():
        return {"gameSpread": 2.5, "overUnder": default_over_under, "homeFavorite": True, "gameSpreadAvailable": False}
    if "provider.id" in pc.columns:
        pc = pc.sort_values(by="provider.id", key=lambda ids: ids.astype(str), kind="stable")
    row = pc[pc["spread"].notnull()].iloc[0]
    spread = float(row["spread"])
    # ESPN's spread is the home team's line, so a negative spread is a home favorite
    favorite = row.get("homeTeamOdds.favorite")
    home_favorite = spread < 0 if spread != 0 else (True if pd.isna(favorite) else bool(favorite))
    over_unders = pc["overUnder"].dropna() if "overUnder" in pc.columns else pd.Series(dtype=float)
    return {
        "gameSpread": spread,
        "overUnder": float(over_unders.iloc[0]) if len(over_unders) else default_over_under,
        "homeFavorite": home_favorite,
        "gameSpreadAvailable": True,
    }


def team_timeout_called(columns: list[str], init: dict[str, Any], side: str) -> pl.Expr:
    """True on a team-timeout play the ``side`` ("home" / "away") team called.

    The play's own ``team.id`` decides. Only a play without one falls back to the
    team's abbreviation / location / mascot appearing as a whole word in the text
    (a bare substring test credits "Memphis" to PHI and "timeout" to ME).
    """
    team_id = pl.col("team.id").cast(pl.Int64, strict=False) if "team.id" in columns else pl.lit(None, dtype=pl.Int64)
    names = {str(init[f"{side}Team{part}"]) for part in ("Abbrev", "Name", "Mascot", "NameAlt")} - {"", "None"}
    if names:
        alternation = "|".join(re.escape(n) for n in sorted(names))
        name_match = pl.col("text").str.contains(rf"(?i)(?:^|\W)(?:{alternation})(?:\W|$)")
    else:
        name_match = pl.lit(False)
    called = pl.col("type.text").is_in(TEAM_TIMEOUT_TYPES) & pl.coalesce(team_id == init[f"{side}TeamId"], name_match)
    return called.fill_null(False)


def period_end_seconds(period_scope: str, scopes: tuple[str, ...]) -> list[pl.Expr]:
    """``end.<scope>_seconds_remaining`` with the period boundary on the row that ends the period.

    ``end.*`` is the next row's ``start.*``. The last row of a period (by sequence: the
    next row's ``period.number`` differs) has its next row in a period whose clock
    starts over, so it ends where its own period ends, at 0:00 on the period clock:
    ``start.<scope>`` less the ``start.<period_scope>`` seconds the period still had.
    Every other row, a period's first included, ends where the next row starts. The
    game's last row has no next row and stays null.

    The boundary used to be filled on a period's FIRST row instead, setting its end to
    the full period length, so a first play logged after the opening second ended later
    than it started (WNBA 400927398 Q2 at 9:41: start 1781, end 1800).

    Args:
        period_scope: The scope counting down one period ("period" for MBB, "quarter"
            for the others, where it is the period clock in a halves era too).
        scopes: The wider scopes ("half", "game") filled the same way.

    Returns:
        One expression per scope, ``period_scope`` first, each aliased to its
        ``end.<scope>_seconds_remaining`` column.
    """
    ends_period = pl.col("period.number") != pl.col("period.number").shift(-1)
    left_in_period = pl.col(f"start.{period_scope}_seconds_remaining")
    return [
        pl.when(ends_period)
        .then(pl.col(f"start.{s}_seconds_remaining") - left_in_period)
        .otherwise(pl.col(f"end.{s}_seconds_remaining"))
        .alias(f"end.{s}_seconds_remaining")
        for s in (period_scope, *scopes)
    ]
