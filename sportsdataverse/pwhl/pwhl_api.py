"""Live PWHL HockeyTech wrappers — full output parity with fastRhockey (R).

Season arguments use the **end year** (e.g. ``2026`` for 2025-26), matching
fastRhockey; they are resolved to the integer HockeyTech ``season_id``.

``hockeytech_api`` is imported at module level so tests can monkeypatch it via
``monkeypatch.setattr(api, "hockeytech_api", ...)``.
"""

from __future__ import annotations

from typing import Any, Optional

import polars as pl

from sportsdataverse.hockeytech import hockeytech_api, resolve_season_id
from sportsdataverse.hockeytech import _parsers as P
from sportsdataverse.hockeytech._analytics import (
    add_clock_columns,
    add_coord_transforms,
    add_shot_distance_angle,
    build_on_ice,
    scoring_chances,
)

__all__ = [
    "pwhl_schedule",
    "pwhl_scorebar",
    "pwhl_game_info",
    "pwhl_game_summary",
    "pwhl_pbp",
    "pwhl_player_box",
    "pwhl_teams",
    "pwhl_team_roster",
    "pwhl_standings",
    "pwhl_player_info",
    "pwhl_player_stats",
    "pwhl_player_game_log",
    "pwhl_player_search",
    "pwhl_stats",
    "pwhl_leaders",
    "pwhl_streaks",
    "pwhl_transactions",
    "pwhl_playoff_bracket",
    "pwhl_season_id",
    "most_recent_pwhl_season",
]

_LG = "pwhl"


def pwhl_season_id(return_as_pandas: bool = False) -> Any:
    """All PWHL seasons with end-year + game-type labels (HockeyTech ``seasons``)."""
    return P.parse_seasons(hockeytech_api(_LG, "modulekit", "seasons", {}), return_as_pandas)


def most_recent_pwhl_season() -> int:
    """Most-recent PWHL season as an end-year integer (max ``season_yr``)."""
    df = pwhl_season_id()
    return int(df["season_yr"].max()) if df.height else 2026


def pwhl_schedule(
    season: Optional[int] = None,
    season_id: Optional[int] = None,
    return_as_pandas: bool = False,
) -> Any:
    """PWHL schedule — one row per game (matches fastRhockey ``pwhl_schedule``)."""
    params: dict = {
        "numberofdaysback": 10000,
        "numberofdaysahead": 10000,
        "limit": 10000,
        "league_id": 1,
    }
    if season is not None or season_id is not None:
        params["season_id"] = resolve_season_id(_LG, season=season, season_id=season_id)
    return P.parse_schedule(hockeytech_api(_LG, "modulekit", "scorebar", params), return_as_pandas)


def _enrich_pwhl_pbp(
    df: pl.DataFrame,
    game_id: int,
    *,
    meta: Optional[Any] = None,
    shifts: Optional[pl.DataFrame] = None,
) -> pl.DataFrame:
    """Enrich a parsed PWHL PBP frame with coord transforms, clock columns,
    shot geometry, game-meta join, and on-ice player tracking.

    This is an internal helper to keep ``pwhl_pbp`` readable and to allow
    offline unit tests to inject fixtures for ``meta`` and ``shifts`` directly
    (bypassing the network).

    Parameters
    ----------
    df:
        Raw frame produced by ``parse_pbp``.
    game_id:
        Numeric game identifier — used to fetch meta/shifts when not provided.
    meta:
        Optional pre-fetched ``gc/gamesummary`` JSON payload (the full dict as
        returned by ``hockeytech_api``).  Fetched live when ``None``.
    shifts:
        Optional pre-parsed shifts :class:`polars.DataFrame` as returned by
        ``parse_shifts``.  Fetched and parsed live when ``None``.

    Returns
    -------
    pl.DataFrame
        Enriched play-by-play frame with coordinate transforms, clock columns,
        shot geometry, game-meta columns, and on-ice player strings.
    """
    # ------------------------------------------------------------------
    # Step 1: fetch meta if not provided
    # ------------------------------------------------------------------
    if meta is None:
        meta = hockeytech_api(_LG, "gc", "gamesummary", {"game_id": game_id})

    # ------------------------------------------------------------------
    # Step 2: extract game-meta fields
    #
    # Source: GC.Gamesummary (fastRhockey uses pwhl_game_info / statviewfeed
    # gameSummary, which surfaces the same Gameinfo fields; we use the richer
    # GC.Gamesummary because it is already fetched and contains season_id +
    # ISO date in the nested ``meta`` dict).
    # ------------------------------------------------------------------
    gs_root = (meta if isinstance(meta, dict) else {}).get("GC", {}) or {}
    gs = gs_root.get("Gamesummary", gs_root) or {}
    gs_meta = gs.get("meta") or {}
    home_raw = gs.get("home") or {}
    away_raw = gs.get("visitor") or {}

    home_team: str = str(home_raw.get("name") or home_raw.get("city") or "")
    home_team_id: str = str(gs_meta.get("home_team") or home_raw.get("id") or home_raw.get("team_id") or "")
    away_team: str = str(away_raw.get("name") or away_raw.get("city") or "")
    away_team_id: str = str(gs_meta.get("visiting_team") or away_raw.get("id") or away_raw.get("team_id") or "")

    # date_played is ISO "YYYY-MM-DD"; game_date matches fastRhockey column name
    game_date: str = str(gs_meta.get("date_played") or gs.get("game_date_iso_8601") or gs.get("game_date") or "")
    # game_season = calendar year (concluding year from date)
    game_season_raw = game_date[:4] if game_date else None
    game_season: Optional[int] = int(game_season_raw) if game_season_raw and game_season_raw.isdigit() else None
    game_season_id: str = str(gs_meta.get("season_id") or "")

    # ------------------------------------------------------------------
    # Step 3: add game-meta literal columns BEFORE coord transforms
    #   (add_coord_transforms needs home_team_id to compute right/vertical)
    # ------------------------------------------------------------------
    df = df.with_columns(
        game_date=pl.lit(game_date),
        game_season=pl.lit(game_season),
        game_season_id=pl.lit(game_season_id),
        home_team=pl.lit(home_team),
        home_team_id=pl.lit(home_team_id),
        away_team=pl.lit(away_team),
        away_team_id=pl.lit(away_team_id),
    )

    # ------------------------------------------------------------------
    # Step 4: coordinate transforms (adds *_original, *_neutral, *_fixed,
    #   *_right, *_vertical columns — 10 total)
    # ------------------------------------------------------------------
    df = add_coord_transforms(df)

    # ------------------------------------------------------------------
    # Step 5: clock columns (minute_start, second_start, clock, sec_from_start)
    # ------------------------------------------------------------------
    df = add_clock_columns(df)

    # ------------------------------------------------------------------
    # Step 6: shot geometry
    #
    # Coord pair used for geometry: the intermediate "rink-feet" frame
    #   x_t = (x_coord_original / 3) - 100   → range ≈ [-100, 100] ft
    #   y_t = 42.5 - (y_coord_original * 85 / 300)  → range ≈ [-42.5, 42.5] ft
    #
    # This is the standard rink frame where the offensive net sits at x ≈ +89.
    # add_shot_distance_angle uses abs(x_coord) internally, so both attacking
    # directions (positive and negative x) yield sensible distances.
    #
    # x_coord_right is NOT used here because for home-team events the flip
    # formula (100 + (100 - x_t)) pushes values to ~190–290, making abs()
    # produce distances >100 ft for what should be close-range shots.
    # ------------------------------------------------------------------
    geo = df.with_columns(
        x_coord=(pl.col("x_coord_original") / 3.0 - 100.0),
        y_coord=(42.5 - (pl.col("y_coord_original") * 85.0 / 300.0)),
    )
    geo = scoring_chances(add_shot_distance_angle(geo))
    df = df.with_columns(
        shot_distance=geo["shot_distance"],
        shot_angle=geo["shot_angle"],
        scoring_chance=geo["scoring_chance"],
    )

    # ------------------------------------------------------------------
    # Step 7: on-ice player tracking via shifts
    #   build_on_ice joins on integer period_of_game + countdown time_s.
    #   PBP period_of_game is a string; shifts.period is Int64.
    #   Strategy: compute on-ice on a copy with integer period + time_s,
    #   then attach on_ice_home / on_ice_away back by row order.
    # ------------------------------------------------------------------
    if shifts is None:
        shifts_payload = hockeytech_api(_LG, "modulekit", "gameshifts", {"game_id": game_id})
        # parse_shifts expects a dict with SiteKit.Gameshifts; guard against
        # unexpected payloads (e.g. an accidental list from a catch-all mock)
        if isinstance(shifts_payload, dict):
            shifts = P.parse_shifts(shifts_payload, game_id=game_id)
        else:
            shifts = pl.DataFrame()

    if df.height > 0 and shifts.height > 0:
        # elapsed_s = minute_start*60 + second_start (may be null for events
        # without time_of_period, e.g. some goalie_change rows)
        elapsed_s = pl.col("minute_start") * 60 + pl.col("second_start")
        # Shifts use a per-period countdown clock (max 1200 s for regulation).
        # time_s = 1200 - elapsed_s gives remaining seconds to match shift intervals.
        time_s = (1200 - elapsed_s).cast(pl.Int64, strict=False)

        df_copy = df.with_columns(
            _period_str=pl.col("period_of_game"),
            period_of_game=pl.col("period_of_game").cast(pl.Int64, strict=False),
            time_s=time_s,
        )
        result = build_on_ice(df_copy, shifts)
        # Restore string period_of_game and drop helpers
        result = result.with_columns(period_of_game=pl.col("_period_str")).drop(["_period_str", "time_s"])
        df = df.with_columns(
            on_ice_home=result["on_ice_home"],
            on_ice_away=result["on_ice_away"],
        )
    else:
        df = df.with_columns(
            on_ice_home=pl.lit(None, dtype=pl.Utf8),
            on_ice_away=pl.lit(None, dtype=pl.Utf8),
        )

    return df


def pwhl_pbp(game_id: int, return_as_pandas: bool = False) -> Any:
    """PWHL play-by-play — one row per event, fully enriched.

    Matches fastRhockey ``pwhl_pbp`` column parity, adding:

    - Coordinate transforms (``*_original``, ``*_neutral``, ``*_fixed``,
      ``*_right``, ``*_vertical``).
    - Clock columns (``minute_start``, ``second_start``, ``clock``,
      ``sec_from_start``).
    - Shot geometry (``shot_distance``, ``shot_angle``, ``scoring_chance``).
    - Game-meta join (``game_date``, ``game_season``, ``game_season_id``,
      ``home_team``, ``home_team_id``, ``away_team``, ``away_team_id``).
    - On-ice player strings (``on_ice_home``, ``on_ice_away``) derived from
      shift data.
    """
    payload = hockeytech_api(_LG, "statviewfeed", "gameCenterPlayByPlay", {"game_id": game_id, "league_id": ""})
    df = P.parse_pbp(payload, pbp_style="hockeytech_a", game_id=game_id)
    df = _enrich_pwhl_pbp(df, game_id)
    if return_as_pandas:
        return df.to_pandas()
    return df


def pwhl_standings(
    season: Optional[int] = None,
    season_id: Optional[int] = None,
    return_as_pandas: bool = False,
) -> Any:
    """PWHL standings — one row per team."""
    sid = resolve_season_id(
        _LG, season=season if season is not None else most_recent_pwhl_season(), season_id=season_id
    )
    payload = hockeytech_api(
        _LG,
        "statviewfeed",
        "teams",
        {
            "groupTeamsBy": "division",
            "context": "overall",
            "special": "false",
            "league_id": 1,
            "sort": "points",
            "season": sid,
        },
    )
    return P.parse_standings(payload, return_as_pandas)


def pwhl_teams(
    season: Optional[int] = None,
    season_id: Optional[int] = None,
    return_as_pandas: bool = False,
) -> Any:
    """PWHL teams for a given season."""
    sid = resolve_season_id(
        _LG, season=season if season is not None else most_recent_pwhl_season(), season_id=season_id
    )
    return P.parse_teams(hockeytech_api(_LG, "modulekit", "teamsbyseason", {"season": sid}), return_as_pandas)


def pwhl_team_roster(
    team_id: int,
    season: Optional[int] = None,
    season_id: Optional[int] = None,
    return_as_pandas: bool = False,
) -> Any:
    """PWHL team roster for a given team + season."""
    sid = resolve_season_id(
        _LG, season=season if season is not None else most_recent_pwhl_season(), season_id=season_id
    )
    return P.parse_roster(
        hockeytech_api(_LG, "modulekit", "roster", {"team_id": team_id, "season_id": sid}),
        return_as_pandas,
    )


def pwhl_player_stats(player_id: int, return_as_pandas: bool = False) -> Any:
    """PWHL player season stats across all seasons."""
    return P.parse_player_stats(
        hockeytech_api(_LG, "modulekit", "player", {"player_id": player_id, "category": "seasonstats"}),
        return_as_pandas,
    )


def pwhl_leaders(
    season: Optional[int] = None,
    season_id: Optional[int] = None,
    return_as_pandas: bool = False,
) -> Any:
    """PWHL statistical leaders for a given season.

    NOTE: the ``leadersExtended`` endpoint uses ``season_id`` (integer) to filter
    by season, not ``season`` (name string). The resolved integer is passed as the
    ``season_id`` param so historical-season requests return results.
    """
    sid = resolve_season_id(
        _LG, season=season if season is not None else most_recent_pwhl_season(), season_id=season_id
    )
    payload = hockeytech_api(
        _LG,
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


def pwhl_game_summary(game_id: int) -> dict:
    """PWHL game summary — dict of frames (game/goals/penalties/shots_by_period/three_stars)."""
    return P.parse_game_summary(hockeytech_api(_LG, "gc", "gamesummary", {"game_id": game_id}), game_id=game_id)


def pwhl_scorebar(return_as_pandas: bool = False) -> Any:
    """PWHL live scorebar (today ± 3 days)."""
    return P.parse_scorebar(
        hockeytech_api(
            _LG,
            "modulekit",
            "scorebar",
            {"numberofdaysback": 3, "numberofdaysahead": 3, "limit": 100, "league_id": 1},
        ),
        return_as_pandas,
    )


def pwhl_game_info(game_id: int, return_as_pandas: bool = False) -> Any:
    """PWHL single-game metadata."""
    return P.parse_game_info(
        hockeytech_api(_LG, "statviewfeed", "gameSummary", {"game_id": game_id}),
        return_as_pandas,
    )


def pwhl_player_box(game_id: int, return_as_pandas: bool = False) -> Any:
    """PWHL player box score for a single game."""
    return P.parse_player_box(
        hockeytech_api(_LG, "statviewfeed", "gameSummary", {"game_id": game_id}),
        return_as_pandas,
    )


def pwhl_player_info(player_id: int, return_as_pandas: bool = False) -> Any:
    """PWHL player biographical info."""
    return P.parse_player_info(
        hockeytech_api(_LG, "statviewfeed", "player", {"player_id": player_id}),
        return_as_pandas,
    )


def pwhl_player_game_log(player_id: int, return_as_pandas: bool = False) -> Any:
    """PWHL player game-by-game log."""
    return P.parse_player_game_log(
        hockeytech_api(_LG, "modulekit", "player", {"player_id": player_id, "category": "gamebygame"}),
        return_as_pandas,
    )


def pwhl_player_search(name: str, return_as_pandas: bool = False) -> Any:
    """Search for PWHL players by name."""
    return P.parse_player_search(
        hockeytech_api(_LG, "modulekit", "searchplayers", {"search_term": name}),
        return_as_pandas,
    )


def pwhl_stats(
    season: Optional[int] = None,
    season_id: Optional[int] = None,
    position: str = "skaters",
    return_as_pandas: bool = False,
) -> Any:
    """PWHL aggregate stats by season and position."""
    sid = resolve_season_id(
        _LG, season=season if season is not None else most_recent_pwhl_season(), season_id=season_id
    )
    return P.parse_stats(
        hockeytech_api(_LG, "modulekit", "statviewtype", {"type": position, "season_id": sid}),
        return_as_pandas,
    )


def pwhl_streaks(return_as_pandas: bool = False) -> Any:
    """Current PWHL player/team streaks."""
    return P.parse_streaks(hockeytech_api(_LG, "modulekit", "streaks", {"league_id": 1}), return_as_pandas)


def pwhl_transactions(return_as_pandas: bool = False) -> Any:
    """PWHL roster transactions."""
    return P.parse_transactions(hockeytech_api(_LG, "modulekit", "transactions", {"league_id": 1}), return_as_pandas)


def pwhl_playoff_bracket(
    season: Optional[int] = None,
    season_id: Optional[int] = None,
    return_as_pandas: bool = False,
) -> Any:
    """PWHL playoff bracket for a given season."""
    sid = resolve_season_id(
        _LG,
        season=season if season is not None else most_recent_pwhl_season(),
        game_type="playoffs",
        season_id=season_id,
    )
    return P.parse_playoff_bracket(
        hockeytech_api(_LG, "modulekit", "brackets", {"season_id": sid, "league_id": 1}),
        return_as_pandas,
    )
