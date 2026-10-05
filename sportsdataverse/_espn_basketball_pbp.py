"""Pieces shared by the ESPN basketball pbp producers (nba / wnba / mbb / wbb)."""

from __future__ import annotations

from typing import Any

import pandas as pd


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
