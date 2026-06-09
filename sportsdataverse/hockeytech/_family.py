"""Generic HockeyTech league-family factory.

``build_family(league)`` returns a dict of callables named with the league
prefix (e.g. ``ahl_schedule``, ``most_recent_ahl_season``). Import
``hockeytech_api`` at module level so tests can monkeypatch it.

Usage (per-league __init__.py)::

    from sportsdataverse.hockeytech._family import build_family
    _family = build_family("ahl")
    globals().update(_family)
    __all__ = list(_family)
"""

from __future__ import annotations

from typing import Any, Optional

from sportsdataverse.hockeytech import hockeytech_api, resolve_season_id
from sportsdataverse.hockeytech import _parsers as P
from sportsdataverse.hockeytech._analytics import (
    corsi_fenwick_on_ice,
    enrich_pbp,
    per60,
    player_toi,
)

import polars as pl


def build_family(league: str) -> dict[str, Any]:
    """Return a dict of public callables for *league*.

    All callables are fully independent closures over the single ``league``
    string; none share mutable state.  The dict is ready to be spread into
    a module namespace via ``globals().update(...)``.

    Parameters
    ----------
    league:
        HockeyTech league code: ``"ahl"``, ``"ohl"``, ``"whl"``, or
        ``"qmjhl"``.

    Returns
    -------
    dict[str, callable]
        Keys are the public function names (e.g. ``"ahl_schedule"``).
    """
    from sportsdataverse.hockeytech._leagues import LEAGUES  # lazy to avoid circulars

    cfg = LEAGUES[league]
    lg = league  # captured in closures

    # ------------------------------------------------------------------
    # Season helpers
    # ------------------------------------------------------------------

    def _season_id(return_as_pandas: bool = False) -> Any:
        """All seasons with end-year + game-type labels."""
        return P.parse_seasons(hockeytech_api(lg, "modulekit", "seasons", {}), return_as_pandas)

    _season_id.__name__ = f"{lg}_season_id"
    _season_id.__qualname__ = f"{lg}_season_id"
    _season_id.__doc__ = f"All {cfg.name} seasons with end-year + game-type labels."

    def _most_recent_season() -> int:
        """Most-recent season as an end-year integer (max ``season_yr``), or 0."""
        df = _season_id()
        return int(df["season_yr"].max()) if df.height else 0

    _most_recent_season.__name__ = f"most_recent_{lg}_season"
    _most_recent_season.__qualname__ = f"most_recent_{lg}_season"
    _most_recent_season.__doc__ = f"Most-recent {cfg.name} season as an end-year integer (max ``season_yr``), or 0."

    # ------------------------------------------------------------------
    # Schedule
    # ------------------------------------------------------------------

    def _schedule(
        season: Optional[int] = None,
        season_id: Optional[int] = None,
        return_as_pandas: bool = False,
    ) -> Any:
        """Schedule — one row per game."""
        params: dict = {
            "numberofdaysback": 10000,
            "numberofdaysahead": 10000,
            "limit": 10000,
            "league_id": cfg.league_id,
        }
        if season is not None or season_id is not None:
            params["season_id"] = resolve_season_id(lg, season=season, season_id=season_id)
        return P.parse_schedule(hockeytech_api(lg, "modulekit", "scorebar", params), return_as_pandas)

    _schedule.__name__ = f"{lg}_schedule"
    _schedule.__qualname__ = f"{lg}_schedule"
    _schedule.__doc__ = f"{cfg.name} schedule — one row per game."

    # ------------------------------------------------------------------
    # PBP
    # ------------------------------------------------------------------

    def _pbp(game_id: int, return_as_pandas: bool = False) -> Any:
        """Play-by-play — one row per event, fully enriched."""
        payload = hockeytech_api(
            lg,
            "statviewfeed",
            "gameCenterPlayByPlay",
            {"game_id": game_id, "league_id": ""},
        )
        df = P.parse_pbp(payload, pbp_style=cfg.pbp_style, game_id=game_id)
        meta_payload = hockeytech_api(lg, "gc", "gamesummary", {"game_id": game_id})
        shifts_payload = hockeytech_api(lg, "modulekit", "gameshifts", {"game_id": game_id})
        return enrich_pbp(
            df,
            lg,
            game_id,
            meta_payload=meta_payload,
            shifts_payload=shifts_payload,
            return_as_pandas=return_as_pandas,
        )

    _pbp.__name__ = f"{lg}_pbp"
    _pbp.__qualname__ = f"{lg}_pbp"
    _pbp.__doc__ = f"{cfg.name} play-by-play — one row per event, fully enriched."

    # ------------------------------------------------------------------
    # Standings
    # ------------------------------------------------------------------

    def _standings(
        season: Optional[int] = None,
        season_id: Optional[int] = None,
        return_as_pandas: bool = False,
    ) -> Any:
        """Standings — one row per team."""
        sid = resolve_season_id(
            lg,
            season=season if season is not None else _most_recent_season(),
            season_id=season_id,
        )
        payload = hockeytech_api(
            lg,
            "statviewfeed",
            "teams",
            {
                "groupTeamsBy": "division",
                "context": "overall",
                "special": "false",
                "league_id": cfg.league_id,
                "sort": "points",
                "season": sid,
            },
        )
        return P.parse_standings(payload, return_as_pandas)

    _standings.__name__ = f"{lg}_standings"
    _standings.__qualname__ = f"{lg}_standings"
    _standings.__doc__ = f"{cfg.name} standings — one row per team."

    # ------------------------------------------------------------------
    # Teams
    # ------------------------------------------------------------------

    def _teams(
        season: Optional[int] = None,
        season_id: Optional[int] = None,
        return_as_pandas: bool = False,
    ) -> Any:
        """Teams for a given season."""
        sid = resolve_season_id(
            lg,
            season=season if season is not None else _most_recent_season(),
            season_id=season_id,
        )
        return P.parse_teams(
            hockeytech_api(lg, "modulekit", "teamsbyseason", {"season": sid}),
            return_as_pandas,
        )

    _teams.__name__ = f"{lg}_teams"
    _teams.__qualname__ = f"{lg}_teams"
    _teams.__doc__ = f"{cfg.name} teams for a given season."

    # ------------------------------------------------------------------
    # Team roster
    # ------------------------------------------------------------------

    def _team_roster(
        team_id: int,
        season: Optional[int] = None,
        season_id: Optional[int] = None,
        return_as_pandas: bool = False,
    ) -> Any:
        """Team roster for a given team + season."""
        sid = resolve_season_id(
            lg,
            season=season if season is not None else _most_recent_season(),
            season_id=season_id,
        )
        return P.parse_roster(
            hockeytech_api(lg, "modulekit", "roster", {"team_id": team_id, "season_id": sid}),
            return_as_pandas,
        )

    _team_roster.__name__ = f"{lg}_team_roster"
    _team_roster.__qualname__ = f"{lg}_team_roster"
    _team_roster.__doc__ = f"{cfg.name} team roster for a given team + season."

    # ------------------------------------------------------------------
    # Player stats
    # ------------------------------------------------------------------

    def _player_stats(player_id: int, return_as_pandas: bool = False) -> Any:
        """Player season stats across all seasons."""
        return P.parse_player_stats(
            hockeytech_api(
                lg,
                "modulekit",
                "player",
                {"player_id": player_id, "category": "seasonstats"},
            ),
            return_as_pandas,
        )

    _player_stats.__name__ = f"{lg}_player_stats"
    _player_stats.__qualname__ = f"{lg}_player_stats"
    _player_stats.__doc__ = f"{cfg.name} player season stats across all seasons."

    # ------------------------------------------------------------------
    # Leaders
    # ------------------------------------------------------------------

    def _leaders(
        season: Optional[int] = None,
        season_id: Optional[int] = None,
        return_as_pandas: bool = False,
    ) -> Any:
        """Statistical leaders for a given season."""
        sid = resolve_season_id(
            lg,
            season=season if season is not None else _most_recent_season(),
            season_id=season_id,
        )
        payload = hockeytech_api(
            lg,
            "statviewfeed",
            "leadersExtended",
            {
                "season_id": sid,
                "team_id": 0,
                "playerTypes": "skaters",
                "skaterStatTypes": "points,goals",
                "activeOnly": 0,
            },
        )
        return P.parse_leaders(payload, return_as_pandas)

    _leaders.__name__ = f"{lg}_leaders"
    _leaders.__qualname__ = f"{lg}_leaders"
    _leaders.__doc__ = f"{cfg.name} statistical leaders for a given season."

    # ------------------------------------------------------------------
    # Game summary
    # ------------------------------------------------------------------

    def _game_summary(game_id: int) -> dict:
        """Game summary — dict of frames (game/goals/penalties/shots_by_period/three_stars)."""
        return P.parse_game_summary(
            hockeytech_api(lg, "gc", "gamesummary", {"game_id": game_id}),
            game_id=game_id,
        )

    _game_summary.__name__ = f"{lg}_game_summary"
    _game_summary.__qualname__ = f"{lg}_game_summary"
    _game_summary.__doc__ = (
        f"{cfg.name} game summary — dict of frames (game/goals/penalties/shots_by_period/three_stars)."
    )

    # ------------------------------------------------------------------
    # Game shifts
    # ------------------------------------------------------------------

    def _game_shifts(game_id: int, return_as_pandas: bool = False) -> Any:
        """Parsed shift stints for a single game."""
        shifts = P.parse_shifts(
            hockeytech_api(lg, "modulekit", "gameshifts", {"game_id": game_id}),
            game_id=game_id,
        )
        if return_as_pandas:
            return shifts.to_pandas()
        return shifts

    _game_shifts.__name__ = f"{lg}_game_shifts"
    _game_shifts.__qualname__ = f"{lg}_game_shifts"
    _game_shifts.__doc__ = f"Parsed shift stints for a single {cfg.name} game."

    # ------------------------------------------------------------------
    # Player TOI
    # ------------------------------------------------------------------

    def _player_toi(game_id: int, return_as_pandas: bool = False) -> Any:
        """Per-player time-on-ice totals for a single game."""
        shifts = _game_shifts(game_id)
        toi = player_toi(shifts)
        if return_as_pandas:
            return toi.to_pandas()
        return toi

    _player_toi.__name__ = f"{lg}_player_toi"
    _player_toi.__qualname__ = f"{lg}_player_toi"
    _player_toi.__doc__ = f"Per-player time-on-ice totals for a single {cfg.name} game."

    # ------------------------------------------------------------------
    # Game Corsi
    # ------------------------------------------------------------------

    def _game_corsi(game_id: int, return_as_pandas: bool = False) -> Any:
        """Player-level on-ice Corsi and Fenwick for a single game."""
        pbp = _pbp(game_id)
        corsi = corsi_fenwick_on_ice(pbp)

        toi = _player_toi(game_id)
        toi_sel = toi.select(
            pl.col("player_id").cast(pl.Utf8),
            pl.col("toi_seconds"),
        )
        out = corsi.join(toi_sel, on="player_id", how="left")
        out = out.with_columns(
            pl.when(pl.col("toi_seconds").is_not_null() & (pl.col("toi_seconds") > 0))
            .then(per60("corsi_for"))
            .otherwise(pl.lit(None, dtype=pl.Float64))
            .alias("corsi_for_per60")
        )
        if return_as_pandas:
            return out.to_pandas()
        return out

    _game_corsi.__name__ = f"{lg}_game_corsi"
    _game_corsi.__qualname__ = f"{lg}_game_corsi"
    _game_corsi.__doc__ = f"Player-level on-ice Corsi and Fenwick for a single {cfg.name} game."

    # ------------------------------------------------------------------
    # Assemble and return the family dict
    # ------------------------------------------------------------------
    return {
        f"{lg}_season_id": _season_id,
        f"most_recent_{lg}_season": _most_recent_season,
        f"{lg}_schedule": _schedule,
        f"{lg}_pbp": _pbp,
        f"{lg}_standings": _standings,
        f"{lg}_teams": _teams,
        f"{lg}_team_roster": _team_roster,
        f"{lg}_player_stats": _player_stats,
        f"{lg}_leaders": _leaders,
        f"{lg}_game_summary": _game_summary,
        f"{lg}_game_shifts": _game_shifts,
        f"{lg}_player_toi": _player_toi,
        f"{lg}_game_corsi": _game_corsi,
    }
