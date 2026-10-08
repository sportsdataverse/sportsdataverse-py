"""HockeyTech league registry + season resolution.

Values lifted from maxtixador/scrapernhl config.py. Keys are public web-client
defaults shipped in each league's site JS; override per league with the
``SDV_<LEAGUE>_API_KEY`` environment variable.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Dict, Literal, Optional

LeagueCode = Literal[
    "pwhl",
    "ahl",
    "ohl",
    "whl",
    "qmjhl",
    "echl",
    "sphl",
    "chl",
    "ushl",
    "bchl",
    "ajhl",
    "sjhl",
    "ojhl",
    "cchl",
    "gojhl",
    "mhl",
    "nojhl",
    "vijhl",
    "kijhl",
    "mjhl",
]


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
    # Verified live 2026-07-12 (all 20 respond on lscluster.hockeytech.com; seasons +
    # scorebar schemas are byte-identical across every league). league_id for the
    # single-league clients is 1 (standings-verified live). pbp_style is a payload-dialect
    # label only: parse_pbp routes both values to the same parser and no code reads it for
    # canvas purposes. Every league ships x/y on one 600x300 canvas with a top-left origin
    # (centre ice at 300,150); a 2026-10-08 gameCenterPlayByPlay probe of PWHL, AHL, OHL
    # and ECHL found no value above 600 in x or 300 in y. New leagues take "hockeytech_b".
    # -- flagship / already-shipped (league_id + pbp_style curated) --
    "pwhl": LeagueConfig("PWHL", "pwhl", "446521baf8c38984", 1, 0, _LSCLUSTER, "hockeytech_a", 600),
    "ahl": LeagueConfig("AHL", "ahl", "ccb91f29d6744675", 4, 3, _LSCLUSTER, "hockeytech_a", 300),
    "ohl": LeagueConfig("OHL", "ohl", "f1aa699db3d81487", 1, 1, _LSCLUSTER, "hockeytech_b", 300),
    "whl": LeagueConfig("WHL", "whl", "f1aa699db3d81487", 7, 0, _LSCLUSTER, "hockeytech_b", 300),
    "qmjhl": LeagueConfig("QMJHL", "lhjmq", "f322673b6bcae299", 6, 0, _LEAGUESTAT, "hockeytech_b", 300),
    # -- professional / major (added 2026-07-12) --
    "echl": LeagueConfig("ECHL", "echl", "2c2b89ea7345cae8", 1, 0, _LSCLUSTER, "hockeytech_b", 300),
    "sphl": LeagueConfig("SPHL", "sphl", "8fa10d218c49ec96", 1, 0, _LSCLUSTER, "hockeytech_b", 300),
    "chl": LeagueConfig("CHL", "chl", "ef96ea7d71574f2a", 1, 0, _LSCLUSTER, "hockeytech_b", 300),
    # -- junior (added 2026-07-12) --
    # ushl: gameCenterPlayByPlay ships goals/penalties/goalie-changes only, no coordinates.
    "ushl": LeagueConfig("USHL", "ushl", "e828f89b243dc43f", 1, 0, _LSCLUSTER, "hockeytech_b", 300),
    "bchl": LeagueConfig("BCHL", "bchl", "f3ed30007ad2124e", 1, 0, _LSCLUSTER, "hockeytech_b", 300),
    "ajhl": LeagueConfig("AJHL", "ajhl", "cbe60a1d91c44ade", 1, 0, _LSCLUSTER, "hockeytech_b", 300),
    "sjhl": LeagueConfig("SJHL", "sjhl", "2fb5c2e84bf3e4a8", 1, 0, _LSCLUSTER, "hockeytech_b", 300),
    "ojhl": LeagueConfig("OJHL", "ojhl", "cce66dd6bebf4790", 1, 0, _LSCLUSTER, "hockeytech_b", 300),
    "cchl": LeagueConfig("CCHL", "cchl", "b370f3e6c805baf3", 1, 0, _LSCLUSTER, "hockeytech_b", 300),
    "gojhl": LeagueConfig("GOJHL", "gojhl", "34b10d4d34d7b59a", 1, 0, _LSCLUSTER, "hockeytech_b", 300),
    "mhl": LeagueConfig("MHL", "mhl", "4a948e7faf5ee58d", 1, 0, _LSCLUSTER, "hockeytech_b", 300),
    "nojhl": LeagueConfig("NOJHL", "nojhl", "c1375ff55168bd71", 1, 0, _LSCLUSTER, "hockeytech_b", 300),
    "vijhl": LeagueConfig("VIJHL", "vijhl", "4f1a61df18906b61", 1, 0, _LSCLUSTER, "hockeytech_b", 300),
    "kijhl": LeagueConfig("KIJHL", "kijhl", "2589e0f644b1bb71", 1, 0, _LSCLUSTER, "hockeytech_b", 300),
    # mjhl: seasons/scorebar/standings work; its public key has NO gamecenter access,
    # so <lg>_pbp / <lg>_game_summary come back empty for MJHL (graceful, not an error).
    "mjhl": LeagueConfig("MJHL", "mjhl", "f894c324fe5fd8f0", 1, 0, _LSCLUSTER, "hockeytech_b", 300),
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
# live seasons feed is unreachable. Ids 1-10 match the committed seasons fixture
# (tests/fixtures/hockeytech/pwhl_seasons.json, a test locks it); 11 is the live
# feed's "2026-27 Regular Season" (2026-10-05). Extend it when a season starts.
_PWHL_SEASON_FALLBACK = [
    {"season_id": 1, "season_yr": 2024, "game_type_label": "regular"},
    {"season_id": 2, "season_yr": 2024, "game_type_label": "preseason"},
    {"season_id": 3, "season_yr": 2024, "game_type_label": "playoffs"},
    {"season_id": 4, "season_yr": 2025, "game_type_label": "preseason"},
    {"season_id": 5, "season_yr": 2025, "game_type_label": "regular"},
    {"season_id": 6, "season_yr": 2025, "game_type_label": "playoffs"},
    {"season_id": 7, "season_yr": 2026, "game_type_label": "preseason"},
    {"season_id": 8, "season_yr": 2026, "game_type_label": "regular"},
    {"season_id": 9, "season_yr": 2026, "game_type_label": "playoffs"},
    {"season_id": 10, "season_yr": 2027, "game_type_label": "preseason"},
    {"season_id": 11, "season_yr": 2027, "game_type_label": "regular"},
]


# Season names that are one-off events, not a league's regular season or playoffs: the
# feed lists them as seasons too ("2026 All-Star Challenge", "2025 Top Prospects",
# "CCHL Pre-Draft Combine 2026", ...), and parse_seasons labels them "regular" because the
# name says neither playoff, preseason nor exhibition. Seen in the 17-league seasons captures
# (sdv-internal-refs hockeytech/, 2026-07-12). Exhibitions get their own "exhibition" label.
SPECIAL_EVENT_SEASON_RE = r"(?i)all[- ]?star|showcase|prospect|combine|special event|exhibition|play[- ]?in\b"
_GAME_TYPE_NAME_RE = {
    "regular": r"(?i)regular season",
    "playoffs": r"(?i)playoff",
    "preseason": r"(?i)pre[- ]?season",
    "exhibition": r"(?i)exhibition",
}


def most_recent_season_yr(seasons, league: str) -> int:
    """Newest regular season's ``season_yr`` in a parsed seasons frame (``<lg>_season_id``).

    Only rows labelled "regular" whose name is not a one-off event count, so a preseason the
    feed lists before its regular season ("2026 Preseason", 2027, ahead of "2026-27 Regular
    Season") never becomes a default season with no regular season behind it. A feed with
    no such row falls back to the newest row of any kind. A seasons list the feed answered
    with no usable season is ``NoDataError``; there is no hard-coded default year to go stale.
    """
    import polars as pl

    if "season_yr" not in seasons.columns:
        yrs = []
    else:
        regular = seasons.filter(
            (pl.col("game_type_label") == "regular")
            & ~pl.col("season_name").fill_null("").str.contains(SPECIAL_EVENT_SEASON_RE)
        )["season_yr"].drop_nulls()
        yrs = regular if len(regular) else seasons["season_yr"].drop_nulls()
    if not len(yrs):
        from sportsdataverse.errors import NoDataError

        raise NoDataError(f"HockeyTech {league}: the seasons feed lists no season")
    return int(yrs.max())


def _fetch_seasons_raw(league: str):
    from sportsdataverse.hockeytech._client import hockeytech_api

    return hockeytech_api(league, "modulekit", "seasons", {})


def resolve_season_id(league: str, season=None, game_type: str = "regular", season_id=None):
    """Resolve an end-year ``season`` (e.g. 2025) to the integer HockeyTech
    ``season_id``. An explicit ``season_id`` short-circuits. PWHL falls back to a
    hardcoded table if the live feed is unreachable or lacks the season; every
    other league re-raises the fetch error (``AssetFetchError`` / ``NoDataError``).

    Of the rows with that ``season_yr`` and ``game_type_label`` ("regular", "playoffs",
    "preseason" or "exhibition"), regular and playoff lookups drop one-off events, then the
    first row wins in this order: no other registered league's code in the name ("CCHL
    2009/2010" in the OJHL feed ranks last), the name says its game type, the name spans two
    years, feed order. Divisions listed side by side for one year with neither marker are
    not told apart: BCHL 2024 resolves to "2023-24 BC Regular Season" but "2024 AB
    Playoffs", GOJHL 2008 to its GHL conference; pass ``season_id`` for the others.
    """
    if season_id is not None:
        return int(season_id)
    if season is None:
        raise ValueError("Provide either season (end-year) or season_id")

    from sportsdataverse.errors import SportsDataverseError
    from sportsdataverse.hockeytech._parsers import TWO_YEAR_NAME_RE, parse_seasons

    try:
        payload = _fetch_seasons_raw(league)
    except SportsDataverseError:
        if league != "pwhl":
            raise
        payload = None
    df = parse_seasons(payload)
    if df.height:
        hit = df.filter((df["season_yr"] == int(season)) & (df["game_type_label"] == game_type))
        if game_type in ("regular", "playoffs"):  # "2025-26 Preseason Exhibition" is a preseason
            hit = hit.filter(~hit["season_name"].fill_null("").str.contains(SPECIAL_EVENT_SEASON_RE))
        # The single-year tournaments the feed also labels regular ("2025 Mowat Cup", "2025
        # Cottage Cup", "2018 ANAVET Cup", "2014 Tie-Break") rank last but are not excluded:
        # CHL lists nothing but its Memorial Cups.
        names = hit["season_name"].fill_null("")
        other_codes = "|".join(code.upper() for code in LEAGUES if code != league)
        hit = hit.with_columns(
            _own=~names.str.contains(rf"\b(?:{other_codes})\b"),
            _named=names.str.contains(_GAME_TYPE_NAME_RE.get(game_type, re.escape(game_type))),
            _spans=names.str.contains(TWO_YEAR_NAME_RE),
        ).sort(["_own", "_named", "_spans"], descending=True, maintain_order=True)
        if hit.height:
            return int(hit["season_id"][0])
    if league == "pwhl":
        for row in _PWHL_SEASON_FALLBACK:
            if row["season_yr"] == int(season) and row["game_type_label"] == game_type:
                return row["season_id"]
    raise ValueError(f"No {league} season for season={season}, game_type={game_type}")
