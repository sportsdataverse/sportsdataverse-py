"""HockeyTech league registry + season resolution.

Values lifted from maxtixador/scrapernhl config.py. Keys are public web-client
defaults shipped in each league's site JS; override per league with the
``SDV_<LEAGUE>_API_KEY`` environment variable.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict, Literal, Optional

LeagueCode = Literal["pwhl", "ahl", "ohl", "whl", "qmjhl"]


@dataclass(frozen=True)
class LeagueConfig:
    name: str
    client_code: str
    api_key: str
    league_id: int
    site_id: int
    base_url: str
    pbp_style: Literal["hockeytech_a", "hockeytech_b"]
    ot_period_length: int  # regulation-OT length in seconds (informational)


_LSCLUSTER = "https://lscluster.hockeytech.com/feed/index.php"
_LEAGUESTAT = "https://cluster.leaguestat.com/feed/index.php"

LEAGUES: Dict[str, LeagueConfig] = {
    "pwhl": LeagueConfig("PWHL", "pwhl", "446521baf8c38984", 1, 0, _LSCLUSTER, "hockeytech_a", 600),
    "ahl": LeagueConfig("AHL", "ahl", "ccb91f29d6744675", 4, 3, _LSCLUSTER, "hockeytech_a", 300),
    "ohl": LeagueConfig("OHL", "ohl", "f1aa699db3d81487", 1, 1, _LSCLUSTER, "hockeytech_b", 300),
    "whl": LeagueConfig("WHL", "whl", "f1aa699db3d81487", 7, 0, _LSCLUSTER, "hockeytech_b", 300),
    "qmjhl": LeagueConfig("QMJHL", "lhjmq", "f322673b6bcae299", 6, 0, _LEAGUESTAT, "hockeytech_b", 300),
}

# gameCenterPlayByPlay uses a distinct key on the statviewfeed PBP view for PWHL
# (observed live 2026-06-09). Other leagues reuse their default key until proven
# otherwise; override per (league, view) here if a different key is needed.
_PBP_KEY_OVERRIDES: Dict[str, str] = {"pwhl": "694cfeed58c932ee"}


def resolve_api_key(league: str, view: Optional[str] = None) -> str:
    """Return the API key for a league, honoring ``SDV_<LEAGUE>_API_KEY``.

    When ``view == "gameCenterPlayByPlay"`` and the league has a PBP-key
    override, that override is used (unless the env var is set, which always
    wins).
    """
    env = os.environ.get(f"SDV_{league.upper()}_API_KEY")
    if env:
        return env
    if view == "gameCenterPlayByPlay" and league in _PBP_KEY_OVERRIDES:
        return _PBP_KEY_OVERRIDES[league]
    return LEAGUES[league].api_key


def get_config(league: str) -> LeagueConfig:
    try:
        return LEAGUES[league]
    except KeyError as exc:  # pragma: no cover - guard
        raise ValueError(f"Unknown HockeyTech league {league!r}; expected one of {sorted(LEAGUES)}") from exc


# Hardcoded PWHL fallback (ported from fastRhockey pwhl_season_id) used when the
# live seasons feed is unreachable.
_PWHL_SEASON_FALLBACK = [
    {"season_id": 1, "season_yr": 2024, "game_type_label": "regular"},
    {"season_id": 3, "season_yr": 2024, "game_type_label": "playoffs"},
    {"season_id": 5, "season_yr": 2025, "game_type_label": "regular"},
    {"season_id": 6, "season_yr": 2025, "game_type_label": "playoffs"},
    {"season_id": 8, "season_yr": 2026, "game_type_label": "regular"},
]


def _fetch_seasons_raw(league: str):
    from sportsdataverse.hockeytech._client import hockeytech_api

    return hockeytech_api(league, "modulekit", "seasons", {})


def resolve_season_id(league: str, season=None, game_type: str = "regular", season_id=None):
    """Resolve an end-year ``season`` (e.g. 2025) to the integer HockeyTech
    ``season_id``. An explicit ``season_id`` short-circuits. PWHL falls back to a
    hardcoded table if the live feed is unreachable.
    """
    if season_id is not None:
        return int(season_id)
    if season is None:
        raise ValueError("Provide either season (end-year) or season_id")

    from sportsdataverse.hockeytech._parsers import parse_seasons

    payload = _fetch_seasons_raw(league)
    df = parse_seasons(payload)
    if df.height:
        hit = df.filter((df["season_yr"] == int(season)) & (df["game_type_label"] == game_type))
        if hit.height:
            return int(hit["season_id"][0])
    if league == "pwhl":
        for row in _PWHL_SEASON_FALLBACK:
            if row["season_yr"] == int(season) and row["game_type_label"] == game_type:
                return row["season_id"]
    raise ValueError(f"No {league} season for season={season}, game_type={game_type}")
