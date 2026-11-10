"""Men's college official season profiles (BB-D34): ``mbb.official_seasons``.

Thin wrapper over :mod:`sportsdataverse._common.official_seasons` on the ESPN
``load_mbb_officials``, ``load_mbb_team_boxscore`` and ``load_mbb_schedule`` releases.
"""

from __future__ import annotations

import pandas as pd
import polars as pl

from sportsdataverse._common.official_seasons import league_official_seasons

__all__ = ["mbb_official_seasons"]


def mbb_official_seasons(
    seasons: int | list[int],
    *,
    season_type: int | None = 2,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Men's college official season profiles: games, fouls and FTA per game, pace and home splits.

    One row per official and season over the completed games of ``season_type`` the official
    worked. ``fouls_per_game`` and ``fta_per_game`` are both teams' totals from ESPN's team box;
    ``pace`` is possessions (FGA - ORB + TOV + 0.44 FTA, averaged over the two teams) per 40
    minutes, over the games' minutes including overtime; ``home_win_pct`` and
    ``home_minus_away_fouls`` (home fouls less away fouls, per game) leave out neutral-site
    games. Every rate has ``<m>_n``, the games it is computed over. The release keys officials by
    names only, so ``official_id`` is null.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26).
        season_type: ESPN's season type: ``2`` regular season (default), ``3`` postseason;
            ``None`` keeps every type.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, official)``: ``season``, ``official_id`` (Int64), ``official_name``,
        ``games``, ``fouls_per_game``, ``fta_per_game``, ``pace``, ``home_win_pct`` and
        ``home_minus_away_fouls``, each rate with its ``_n``. A season without an officials
        release returns that schema with zero rows.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_official_seasons import mbb_official_seasons
            refs = mbb_official_seasons(2026)
            refs.sort("fouls_per_game", descending=True).select("official_name", "games", "fouls_per_game").head()
    """
    out = league_official_seasons("mbb", seasons, season_type=season_type)
    return out.to_pandas() if return_as_pandas else out
