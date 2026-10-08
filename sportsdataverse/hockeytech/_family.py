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
from sportsdataverse.hockeytech._leagues import most_recent_season_yr
from sportsdataverse.hockeytech._analytics import (
    corsi_fenwick_on_ice,
    enrich_pbp,
    per60,
    player_toi,
)

import polars as pl

# Shared docstring tails for the minted callables (every one goes through hockeytech_api).
_RAISES = (
    "\n\nRaises:\n"
    "    NoDataError: The feed answered 404.\n"
    "    AssetFetchError: The fetch failed: a non-2xx status, an empty or unparseable body, or an\n"
    "        ``Undefined Tab`` / ``InvalidView`` sentinel."
)
_PANDAS = "A pandas DataFrame when ``return_as_pandas`` is True."
_SEASON_ARGS = (
    "Args:\n"
    "    season: Season as an END year (2026 = the 2025-26 season). Defaults to the newest regular\n"
    "        season when neither ``season`` nor ``season_id`` is given.\n"
    "    season_id: The HockeyTech season id, when it is already known.\n"
    "    return_as_pandas: Return a pandas DataFrame instead of polars.\n\n"
)
_GAME_ARGS = (
    "Args:\n"
    "    game_id: The HockeyTech game id.\n"
    "    return_as_pandas: Return a pandas DataFrame instead of polars.\n\n"
)


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
    _season_id.__doc__ = (
        f"All {cfg.name} seasons with end-year + game-type labels.\n\n"
        "Args:\n"
        "    return_as_pandas: Return a pandas DataFrame instead of polars.\n\n"
        "Returns:\n"
        "    polars.DataFrame: One row per season: ``season_id`` (Int64), ``season_name``,\n"
        "        ``season_short``, ``career``, ``playoff``, ``start_date``, ``end_date``, ``season_yr``\n"
        f"        (Int64, the END year) and ``game_type_label``. {_PANDAS}" + _RAISES
    )

    def _most_recent_season() -> int:
        """Newest regular season as an end-year integer."""
        return most_recent_season_yr(_season_id(), lg)

    _most_recent_season.__name__ = f"most_recent_{lg}_season"
    _most_recent_season.__qualname__ = f"most_recent_{lg}_season"
    _most_recent_season.__doc__ = (
        f"Newest {cfg.name} regular season as an end-year integer: the highest ``season_yr`` of a "
        "regular season that is not a one-off event, so a preseason listed first is not a default.\n\n"
        "Returns:\n"
        "    int: The newest regular season's END year (2026 = the 2025-26 season).\n\n"
        "Raises:\n"
        "    NoDataError: The seasons feed lists no regular season.\n"
        "    AssetFetchError: The seasons feed failed."
    )

    def _season_or_latest(season: Optional[int], season_id: Optional[int]) -> Optional[int]:
        """The caller's season; the newest only when neither season nor season_id is given."""
        return season if season is not None or season_id is not None else _most_recent_season()

    # ------------------------------------------------------------------
    # Schedule
    # ------------------------------------------------------------------

    def _schedule(
        season: Optional[int] = None,
        season_id: Optional[int] = None,
        return_as_pandas: bool = False,
    ) -> Any:
        """Schedule — one row per game."""
        sid = resolve_season_id(lg, season=_season_or_latest(season, season_id), season_id=season_id)
        payload = hockeytech_api(lg, "modulekit", "schedule", {"season_id": sid})
        return P.parse_schedule(payload, return_as_pandas, season_id=sid)

    _schedule.__name__ = f"{lg}_schedule"
    _schedule.__qualname__ = f"{lg}_schedule"
    _schedule.__doc__ = (
        f"{cfg.name} schedule — one row per game of one season.\n\n" + _SEASON_ARGS + "Returns:\n"
        "    polars.DataFrame: One row per game: ``game_id``, ``game_date``, ``game_status``,\n"
        "        ``home_team`` / ``home_team_id`` / ``home_score``, ``away_team`` / ``away_team_id`` /\n"
        "        ``away_score``, ``venue``, ``season_id`` and ``game_type`` (all String). Only the\n"
        "        requested season's games: a regular season, its playoffs and its preseason are\n"
        f"        separate season ids. {_PANDAS}" + _RAISES
    )

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
    _pbp.__doc__ = (
        f"{cfg.name} play-by-play — one row per event, fully enriched.\n\n" + _GAME_ARGS + "Returns:\n"
        "    polars.DataFrame: One row per event (shot, goal, penalty, faceoff, hit, goalie change ...):\n"
        "        ``game_id``, ``event``, ``team_id``, ``period_of_game``, ``time_of_period``, rink\n"
        "        ``x_coord`` / ``y_coord`` (Float64, raw 600×300 coordinates), the primary / second / third\n"
        "        player and goalie ids and names, the plus / minus skaters on a goal, game metadata from\n"
        "        the game summary, and derived ``shot_distance`` / ``shot_angle`` / ``scoring_chance`` and\n"
        "        the ``on_ice_home`` / ``on_ice_away`` skaters from the shift feed. Player ids are Float64\n"
        "        here. Some leagues (USHL, MJHL) publish only goals, penalties and goalie changes, with no\n"
        f"        coordinates. {_PANDAS}" + _RAISES
    )

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
            season=_season_or_latest(season, season_id),
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
    _standings.__doc__ = (
        f"{cfg.name} standings — one row per team.\n\n" + _SEASON_ARGS + "Returns:\n"
        "    polars.DataFrame: One row per team: ``team``, ``team_code``, ``team_rank`` and ``wins``\n"
        "        (Int64), and ``games_played``, ``losses``, ``regulation_wins``, ``non_reg_wins``,\n"
        "        ``non_reg_losses``, ``points``, ``goals_for``, ``goals_against``, ``games_remaining``,\n"
        f"        ``percentage`` and ``overall_rank`` (String, as the feed ships them). {_PANDAS}" + _RAISES
    )

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
            season=_season_or_latest(season, season_id),
            season_id=season_id,
        )
        return P.parse_teams(
            hockeytech_api(lg, "modulekit", "teamsbyseason", {"season": sid}),
            return_as_pandas,
        )

    _teams.__name__ = f"{lg}_teams"
    _teams.__qualname__ = f"{lg}_teams"
    _teams.__doc__ = (
        f"{cfg.name} teams for a given season.\n\n" + _SEASON_ARGS + "Returns:\n"
        "    polars.DataFrame: One row per team: ``team_id``, ``team_name``, ``team_code``,\n"
        f"        ``team_nickname``, ``team_label``, ``division`` and ``team_logo`` (String). {_PANDAS}" + _RAISES
    )

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
            season=_season_or_latest(season, season_id),
            season_id=season_id,
        )
        return P.parse_roster(
            hockeytech_api(lg, "modulekit", "roster", {"team_id": team_id, "season_id": sid}),
            return_as_pandas,
        )

    _team_roster.__name__ = f"{lg}_team_roster"
    _team_roster.__qualname__ = f"{lg}_team_roster"
    _team_roster.__doc__ = (
        f"{cfg.name} team roster for a given team + season.\n\n"
        "Args:\n"
        "    team_id: The HockeyTech team id.\n" + _SEASON_ARGS.split("Args:\n", 1)[1] + "Returns:\n"
        "    polars.DataFrame: One row per rostered player: ``player_id``, ``person_id``, names\n"
        "        (``first_name``, ``last_name``, ``display_name``), ``position``, ``tp_jersey_number``,\n"
        "        ``shoots`` / ``catches``, ``height`` / ``weight``, ``birthdate``, home and birth places,\n"
        "        ``rookie``, ``veteran_status``, ``draft_status`` and ``player_image`` (String, as the feed\n"
        f"        ships them). {_PANDAS}" + _RAISES
    )

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
    _player_stats.__doc__ = (
        f"{cfg.name} player season stats across all seasons.\n\n"
        "Args:\n"
        "    player_id: The HockeyTech player id.\n"
        "    return_as_pandas: Return a pandas DataFrame instead of polars.\n\n"
        "Returns:\n"
        "    polars.DataFrame: One row per season (and team) the player played: ``season_id``,\n"
        "        ``season_name``, ``playoff``, ``team_id`` / ``team_name`` / ``team_code``,\n"
        "        ``games_played``, ``goals``, ``assists``, ``points``, ``plus_minus``,\n"
        "        ``penalty_minutes``, power-play / short-handed / shootout splits, ``shots``,\n"
        "        ``faceoff_wins`` / ``faceoff_attempts``, ``ice_time`` and ``stat_type`` (String, as the\n"
        f"        feed ships them). {_PANDAS}" + _RAISES
    )

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
            season=_season_or_latest(season, season_id),
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
    _leaders.__doc__ = (
        f"{cfg.name} statistical leaders for a given season.\n\n" + _SEASON_ARGS + "Returns:\n"
        "    polars.DataFrame: One row per ranked skater (points and goals leaders): ``rank`` (Int64),\n"
        "        ``player_id``, ``name``, ``jersey_number``, ``position``, ``team_id`` / ``team_name`` /\n"
        "        ``team_code``, ``stat_formatted`` and ``type_formatted`` (String), plus photo and logo\n"
        f"        URLs. {_PANDAS}" + _RAISES
    )

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
        f"{cfg.name} game summary — dict of frames (game/goals/penalties/shots_by_period/three_stars).\n\n"
        "Args:\n"
        "    game_id: The HockeyTech game id.\n\n"
        "Returns:\n"
        "    dict[str, polars.DataFrame]: ``game`` (one row: ``game_id``, ``date``, ``status``,\n"
        "        ``venue``, ``attendance``, both teams and scores), ``goals`` (one row per goal with the\n"
        "        scorer, both assists and the plus / minus skaters), ``penalties`` (one row per penalty),\n"
        "        ``shots_by_period`` (``side``, ``period``, ``shots``) and ``three_stars``. When the league\n"
        "        denies the summary view, the event frames are empty and ``game`` is a ``game_id`` stub row." + _RAISES
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
    _game_shifts.__doc__ = (
        f"Parsed shift stints for a single {cfg.name} game.\n\n" + _GAME_ARGS + "Returns:\n"
        "    polars.DataFrame: One row per shift: ``game_id`` and ``player_id`` (Int64),\n"
        "        ``first_name``, ``last_name``, ``jersey_number``, ``home`` (Int64, 1 = home),\n"
        "        ``period``, ``start_time`` / ``end_time`` / ``length`` (clock strings), ``start_s`` /\n"
        "        ``end_s`` (Int64 seconds) and the ``goal_on_shift`` / ``penalty_on_shift`` flags.\n"
        f"        {_PANDAS}" + _RAISES
    )

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
    _player_toi.__doc__ = (
        f"Per-player time-on-ice totals for a single {cfg.name} game.\n\n" + _GAME_ARGS + "Returns:\n"
        "    polars.DataFrame: One row per player: ``player_id`` (Int64), ``first_name``,\n"
        "        ``last_name``, ``toi_seconds`` (Int64), ``num_shifts`` and ``avg_shift_s`` (Float64).\n"
        f"        {_PANDAS}" + _RAISES
    )

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
    _game_corsi.__doc__ = (
        f"Player-level on-ice Corsi and Fenwick for a single {cfg.name} game.\n\n" + _GAME_ARGS + "Returns:\n"
        "    polars.DataFrame: One row per player on ice for a shot attempt: ``player_id`` (String),\n"
        "        ``corsi_for`` / ``corsi_against`` / ``corsi_for_pct``, ``fenwick_for`` /\n"
        "        ``fenwick_against`` / ``fenwick_for_pct``, ``corsi_includes_missed`` (Boolean),\n"
        "        ``toi_seconds`` (Int64) and ``corsi_for_per60`` (Float64, null without time on ice).\n"
        f"        {_PANDAS}" + _RAISES
    )

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
