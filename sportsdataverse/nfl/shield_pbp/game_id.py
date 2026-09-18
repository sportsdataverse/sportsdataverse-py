"""nflverse ``game_id`` + club-abbreviation helpers for Shield (api.nfl.com) game objects.

Moved verbatim from nfl-data ``model_training/play_level/fetcher.py`` (the version
carrying the preseason ``PRE{week}`` id fix) so the Shield play-by-play parser can
live in sdv-py without a dependency on the nfl-data training code.
"""

from __future__ import annotations


# NFL Shield club abbreviations that differ from the nflverse game_id convention.
# The Shield API exposes each club's abbreviation as the trailing path segment of
# its ``currentLogo`` URL (e.g. ``.../clubs/logos/PHI``). All 32 current clubs match
# nflverse except Arizona, which the league renders as ``AZ`` but nflverse spells
# ``ARI``. The Rams already render as ``LA`` (matching nflverse), not ``LAR``.
_NFLVERSE_ABBR_FIXUPS = {"AZ": "ARI"}

# ``currentLogo`` always yields a club's *present-day* abbreviation, so franchises
# that relocated inside the detail era need a season-aware fixup to match nflverse's
# historical game_ids. Map: modern abbr -> (last season under the OLD abbr, old abbr).
# A game in season <= cutoff uses the old abbr.
_RELOCATIONS = {
    "LA": (2015, "STL"),  # Rams: St. Louis through 2015, Los Angeles from 2016
    "LAC": (2016, "SD"),  # Chargers: San Diego through 2016, Los Angeles from 2017
    "LV": (2019, "OAK"),  # Raiders: Oakland through 2019, Las Vegas from 2020
}


def _team_abbr(team: dict) -> str:
    """Return a club's *present-day* abbreviation from a Shield team object.

    Reads the trailing path segment of ``team["currentLogo"]`` (e.g.
    ``.../clubs/logos/PHI`` -> ``"PHI"``). This is the modern abbreviation; apply
    :func:`_nflverse_abbr` to normalize it to the nflverse convention for a season.

    Args:
        team: A Shield ``homeTeam`` / ``awayTeam`` object (must carry
            ``currentLogo``).

    Returns:
        The raw modern club abbreviation (e.g. ``"PHI"``, ``"AZ"``, ``"LA"``).

    Raises:
        ValueError: When no abbreviation can be derived (missing/empty
            ``currentLogo``).
    """
    raw = (team.get("currentLogo") or "").rstrip("/").rsplit("/", 1)[-1].strip()
    if not raw:
        raise ValueError(f"Cannot derive team abbreviation from team object: {team!r}")
    return raw


def _nflverse_abbr(raw: str, season: int) -> str:
    """Normalize a modern club abbreviation to nflverse's for a given season.

    Applies the season-independent rename (``AZ`` -> ``ARI``) and then the
    season-aware relocation fixup (e.g. the Rams are ``STL`` through 2015 and
    ``LA`` from 2016; Chargers ``SD`` <= 2016; Raiders ``OAK`` <= 2019).

    Args:
        raw: The present-day abbreviation from :func:`_team_abbr`.
        season: NFL season year (used to pick the era-correct abbreviation).

    Returns:
        The nflverse club abbreviation for that season.
    """
    abbr = _NFLVERSE_ABBR_FIXUPS.get(raw, raw)
    reloc = _RELOCATIONS.get(abbr)
    if reloc is not None and season <= reloc[0]:
        return reloc[1]
    return abbr


def nflverse_game_id(game: dict, reg_weeks: int = 18) -> str:
    """Build the nflverse ``game_id`` (``{season}_{week:02d}_{away}_{home}``).

    Regular-season games keep their API week number. Postseason games continue
    the numbering past the regular season, matching nflverse: with ``reg_weeks=18``
    (the 2021+ schedule), Wild Card -> 19, Divisional -> 20, Conference -> 21,
    Super Bowl -> 22; with ``reg_weeks=17`` (1999-2020) those become 18-21.
    nflverse has no preseason ids, so preseason games get an explicit ``PRE{week}``
    token (Hall of Fame game = ``PRE0``) that can never collide with a REG/POST id.
    Team abbreviations are normalized to nflverse via :func:`_nflverse_abbr`,
    including season-aware relocation fixups (``LA`` -> ``STL`` for the pre-2016
    Rams, etc.).

    Args:
        game: A Shield game object (carrying ``season``, ``week``,
            ``seasonType``, ``homeTeam``, ``awayTeam``).
        reg_weeks: Number of regular-season weeks for the season, used as the
            postseason offset. ``17`` for 1999-2020, ``18`` for 2021+.

    Returns:
        The nflverse game_id string, e.g. ``"2025_01_DAL_PHI"``,
        ``"2025_19_LA_CAR"`` (Wild Card), ``"2010_01_ARI_STL"`` (pre-relocation),
        or ``"2026_PRE1_DEN_ATL"`` (preseason).
    """
    season = int(game["season"])
    api_week = int(game["week"])
    season_type = game.get("seasonType", "REG")
    if season_type == "PRE":
        week = f"PRE{api_week}"
    else:
        week = f"{api_week if season_type == 'REG' else reg_weeks + api_week:02d}"
    away = _nflverse_abbr(_team_abbr(game["awayTeam"]), season)
    home = _nflverse_abbr(_team_abbr(game["homeTeam"]), season)
    return f"{season}_{week}_{away}_{home}"
