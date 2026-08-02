"""Stamp team + player IDENTIFIERS and readable names onto the parsed families.

The parser stack speaks three name dialects and NO ids:

* ``FIRST.LAST`` ALL-CAPS -- pbp ``player_1``/``player_2``/``home_N``/``away_N``,
  ``player_box.player``, ``possessions.home_N``/``away_N``.
* ``"Last, First"`` display form -- ``lineups[].players[].id.name`` (already
  properly cased; what it lacks is the id).
* A CamelCase within-team code (``"AnBrizzi"``) -- ``shots.shooter_id`` and
  ``lineups[].players[].code``.

...and team NAMES only (``"Buffalo"``), with no NCAA or ESPN team id anywhere.

This module joins all three against the committed datasets trees -- fully
offline, no network -- and writes ids + a properly-cased ``clean_name``
ALONGSIDE every existing column. Nothing is renamed or removed.

Sources (both written by :mod:`ncaa_datasets`):

* ``{root}/{lg}/rosters/json/{season}/{team_id}.json`` -- ``player_id``,
  ``clean_name``, ``player``, ``team_id``, ``team``.
* ``{root}/{lg}/teams/json/{season}.json`` -- ``ncaa_team_id``, ``team``,
  ``espn_team_id``, ``espn_display_name``, ``espn_mascot``, ``conference``.

Two join keys, both reused rather than reinvented:

* :func:`_key_name` -- the format-immune name signature, copied VERBATIM from
  ``ncaa-{lg}-hoops-data``'s ``ncaa_{lg}_data_build/derived.py`` so the two
  repos key players identically. Strip diacritics, casefold, drop suffix
  tokens, sorted-letter signature: ``"Talton Jr, Derrick"`` and
  ``"DERRICK.TALTON"`` both land on the same key.
* :func:`~sportsdataverse.mbb.mbb_ncaa_stints.build_player_code` -- sdv-py's
  own code builder, i.e. the exact function that MINTED ``shooter_id`` in the
  first place, run over the roster's ``clean_name`` to invert it. Not a second
  name normalizer; a different key space (codes, not names).

**The per-game families carry BOTH id namespaces.** Every team-name column on
``pbp`` / ``possessions`` / ``player_box`` / ``team_box`` / ``shots`` /
``lineups`` gets an ``_ncaa_team_id`` AND an ``_espn_team_id`` beside it, and
every row gets the game-level ``espn_game_id`` (from
:mod:`ncaa_espn_game_xwalk`) next to its ``contest_id``. That makes each family
independently joinable against both the NCAA and the ESPN/hoopR-wehoop sides
without a lookup hop through the ``teams`` block -- which keeps its own full
ESPN identity (display name, mascot, conference) as the reference row.

**Never guesses, never drops.** The player index is scoped to the two teams in
the game, and any key that resolves to more than one ``player_id`` is dropped
from the index -- an ambiguous or unmatched name keeps its row and gets a NULL
id, it is never dropped and never approximated. Same for teams: a non-D-I
exhibition opponent has no crosswalk row, so it stays null.

**Degrades to nulls.** A season with no rosters/teams tree yet (most historical
seasons) still parses: every new column is emitted as a typed null and one line
is logged. Absence of a source is never an exception.
"""

from __future__ import annotations

import json
import logging
import re
import unicodedata
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)


__all__ = [
    "LINEUP_TEAM_COLUMNS",
    "PLAYER_COLUMNS",
    "TEAM_COLUMNS",
    "enrich_parsed",
    "load_roster_index",
    "load_team_index",
]

# --- the format-immune name key --------------------------------------------
# Copied VERBATIM from ncaa-mbb-hoops-data/python/ncaa_mbb_data_build/derived.py
# (_NAME_SPLIT / _NAME_SUFFIXES / _key_name). It is duplicated rather than
# imported because the two repos are separate checkouts with no shared package;
# any change MUST land in both or the repos stop agreeing on player keys.
_NAME_SPLIT = re.compile(r"[^a-z0-9']+")
_NAME_SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}

#: The pbp stream's placeholder for a team rebound / team turnover. Not a
#: person, so it must never resolve to one -- excluded from the join and from
#: the reported miss counts.
_NOT_A_PLAYER = {"TEAM", ""}


def _key_name(name: str) -> str:
    """Format-immune canonical form of one player name for hashing.

    The sources render the same player differently -- the roster keeps display
    form (``"Talton Jr, Derrick"`` / ``"B.J. Edwards"``) while pbp carries the
    engine's ``"DERRICK.TALTON"`` / ``"BJ.EDWARDS"`` normalization, which DROPS
    suffixes and collapses initials. So: strip diacritics, casefold, split on
    non-alphanumerics, drop suffix tokens, sort the letters. Both forms above
    map to the same key.
    """
    ascii_name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    words = [w for w in _NAME_SPLIT.split(ascii_name.casefold()) if w]
    words = [w for w in words if w not in _NAME_SUFFIXES]
    # Sorted-letter signature: immune to word order, hyphen/space-collapsed
    # compound surnames (PORTERBROWN vs Porter-Brown), apostrophes (JEKEL vs
    # Je'Kel), and collapsed initials. Collision = exact-anagram teammates,
    # which at team-season scope is negligible -- and is dropped anyway.
    return "".join(sorted("".join(words).replace("'", "")))


def _utf8_id(value: Any) -> Optional[str]:
    """Any id -> Utf8, or ``None``. NEVER stringifies a float directly.

    ``str(123.0)`` is ``"123.0"`` -- the classic join-breaking defect. A float
    id goes through ``int`` first; a non-integral float is not an id at all and
    becomes ``None`` rather than a plausible-looking wrong string.
    """
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, float):
        return str(int(value)) if value.is_integer() else None
    return str(value)


# --- the enriched column plan ----------------------------------------------
#: family -> the existing team-NAME columns that get ``_ncaa_team_id`` and
#: ``_espn_team_id`` stamped beside them.
TEAM_COLUMNS: Dict[str, Tuple[str, ...]] = {
    "pbp": ("home", "away", "event_team", "poss_team"),
    "possessions": ("home", "away", "poss_team"),
    "player_box": ("home", "away", "team"),
    "team_box": ("home", "away", "team"),
    # `shots.team_id` holds a team NAME despite its column name (the shots
    # adapter fills it from `shooting.team.name`); it is left untouched and the
    # real ids land in `ncaa_team_id` / `espn_team_id`.
    "shots": ("team_id",),
}

#: The ten on-court lineup slots. Same ALL-CAPS ``FIRST.LAST`` codes as
#: ``player_1``/``player_2``, so they resolve through the same game-scoped
#: player index -- no second lookup.
_ONCOURT: Tuple[str, ...] = tuple(f"{side}_{slot}" for side in ("home", "away") for slot in range(1, 6))
_ONCOURT_SPECS: Tuple[Tuple[str, str, str], ...] = tuple(
    (column, f"{column}_player_id", f"{column}_clean_name") for column in _ONCOURT
)

#: ``lineups`` carries its teams as nested ``{"team": {"name": ...}}`` structs,
#: so it gets flat sidecar columns instead of a ``{col}_`` suffix pair.
LINEUP_TEAM_COLUMNS: Tuple[str, ...] = ("team", "opponent")

#: family -> (existing ALL-CAPS name column, new id column, new readable column).
PLAYER_COLUMNS: Dict[str, Tuple[Tuple[str, str, str], ...]] = {
    "pbp": (
        ("player_1", "player_1_id", "player_1_clean_name"),
        ("player_2", "player_2_id", "player_2_clean_name"),
    )
    + _ONCOURT_SPECS,
    "player_box": (("player", "player_id", "clean_name"),),
    "possessions": _ONCOURT_SPECS,
}

#: The id columns stamped for every team-name column, in both the ``{column}_``
#: suffixed form and (on ``shots``) the bare form.
_TEAM_ID_KEYS = ("ncaa_team_id", "espn_team_id")
_SHOTS_PLAYER_KEYS = ("shooter_player_id", "shooter_clean_name")

#: The game-level ESPN event id, stamped on every row of every per-game family
#: beside ``contest_id``. Built offline by :mod:`ncaa_espn_game_xwalk`.
_ESPN_GAME_ID_KEY = "espn_game_id"

#: The six per-game families, i.e. every family that gets ``espn_game_id``.
#: Kept in sync with ``ncaa_parse.PER_GAME_FAMILIES`` (duplicated for the same
#: reason ``_key_name`` is: no shared package across these checkouts).
_PER_GAME_FAMILIES: Tuple[str, ...] = (
    "pbp",
    "lineups",
    "player_box",
    "team_box",
    "shots",
    "possessions",
)


def _teams_path(root: Union[str, Path], league: str, season: int) -> Path:
    return Path(root) / league / "teams" / "json" / f"{season}.json"


def _rosters_dir(root: Union[str, Path], league: str, season: int) -> Path:
    return Path(root) / league / "rosters" / "json" / str(season)


@lru_cache(maxsize=8)
def load_team_index(root: str, league: str, season: int) -> Dict[str, Dict[str, Optional[str]]]:
    """``team name -> {ncaa_team_id, espn_team_id, espn_display_name, ...}``.

    Empty dict when the season's teams file was never built -- the caller then
    emits null ids rather than raising.
    """
    path = _teams_path(root, league, season)
    if not path.is_file():
        return {}
    rows = json.loads(path.read_text(encoding="utf-8"))
    index: Dict[str, Dict[str, Optional[str]]] = {}
    for row in rows:
        team = row.get("team")
        if not team:
            continue
        index[team] = {
            "team": team,
            "ncaa_team_id": _utf8_id(row.get("ncaa_team_id")),
            "espn_team_id": _utf8_id(row.get("espn_team_id")),
            "espn_display_name": row.get("espn_display_name"),
            "espn_mascot": row.get("espn_mascot"),
            "conference": row.get("conference"),
            "division": row.get("division"),
        }
    return index


@lru_cache(maxsize=8)
def load_roster_index(root: str, league: str, season: int) -> Dict[str, Dict[str, Dict[str, Tuple[str, str]]]]:
    """``team_id -> {"names": {key_name: (player_id, clean_name)}, "codes": {...}}``.

    Both key spaces are built per team so the per-game index can be assembled
    from just the two teams on the floor. A key that resolves to more than one
    ``player_id`` within a team is DROPPED -- better a null than a coin flip.

    Empty dict when the season's rosters tree was never built.
    """
    directory = _rosters_dir(root, league, season)
    if not directory.is_dir():
        return {}

    # Imported here, not at module scope: the code inversion is the only reason
    # this module needs sdv-py at all, and a season with no rosters must not pay
    # the import at all.
    from sportsdataverse.mbb.mbb_ncaa_models import TeamId
    from sportsdataverse.mbb.mbb_ncaa_stints import build_player_code

    index: Dict[str, Dict[str, Dict[str, Tuple[str, str]]]] = {}
    for path in sorted(directory.glob("*.json")):
        try:
            rows = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            logger.warning("ncaa_identity: unreadable roster %s; skipped", path, exc_info=True)
            continue
        names: Dict[str, set] = {}
        codes: Dict[str, set] = {}
        for row in rows:
            player_id = _utf8_id(row.get("player_id"))
            clean_name = row.get("clean_name")
            if not player_id or not clean_name:
                continue
            entry = (player_id, clean_name)
            # Key on BOTH the display form and the roster's own ALL-CAPS
            # `player` column: they normalize to the same key in the common
            # case, and to two keys exactly where the pbp spelling diverges.
            for raw in (clean_name, row.get("player")):
                if raw:
                    names.setdefault(_key_name(raw), set()).add(entry)
            team = row.get("team")
            try:
                code = build_player_code(clean_name, TeamId(team) if team else None).code
            except Exception:  # noqa: BLE001 -- an un-codeable name just has no code key
                continue
            codes.setdefault(code, set()).add(entry)
        index[path.stem] = {
            "names": {k: next(iter(v)) for k, v in names.items() if len({e[0] for e in v}) == 1},
            "codes": {k: next(iter(v)) for k, v in codes.items() if len({e[0] for e in v}) == 1},
        }
    return index


def _game_player_index(
    roster_index: Dict[str, Dict[str, Dict[str, Tuple[str, str]]]], team_ids: List[str]
) -> Tuple[Dict[str, Tuple[str, str]], Dict[str, Tuple[str, str]]]:
    """Merge the two teams' indices; drop keys the two teams disagree on.

    Scoping to the game (rather than the whole season) is what makes a bare
    name resolvable at all -- and keeps a cross-team anagram from resolving to
    the wrong player, because it is dropped instead.
    """
    merged: Dict[str, Dict[str, set]] = {"names": {}, "codes": {}}
    for team_id in team_ids:
        team_index = roster_index.get(team_id)
        if not team_index:
            continue
        for space in ("names", "codes"):
            for key, entry in team_index[space].items():
                merged[space].setdefault(key, set()).add(entry)
    names = {k: next(iter(v)) for k, v in merged["names"].items() if len({e[0] for e in v}) == 1}
    codes = {k: next(iter(v)) for k, v in merged["codes"].items() if len({e[0] for e in v}) == 1}
    return names, codes


def _stamp_team(
    row: Dict[str, Any],
    column: str,
    teams: Dict[str, Dict[str, Optional[str]]],
    stats: Dict[str, int],
) -> None:
    """Write ``{column}_ncaa_team_id`` / ``{column}_espn_team_id`` beside *column*."""
    name = row.get(column)
    hit = teams.get(name) if name else None
    for key in _TEAM_ID_KEYS:
        row[f"{column}_{key}"] = hit[key] if hit else None
    if name:
        stats["team_total"] += 1
        stats["team_hit" if hit else "team_miss"] += 1


def _stamp_player(
    row: Dict[str, Any],
    name_column: str,
    id_column: str,
    readable_column: str,
    names: Dict[str, Tuple[str, str]],
    stats: Dict[str, int],
) -> None:
    """Write the id + properly-cased name beside an ALL-CAPS name column."""
    raw = row.get(name_column)
    hit = names.get(_key_name(raw)) if raw and raw.upper() not in _NOT_A_PLAYER else None
    row[id_column] = hit[0] if hit else None
    row[readable_column] = hit[1] if hit else None
    if raw and raw.upper() not in _NOT_A_PLAYER:
        stats["player_total"] += 1
        stats["player_hit" if hit else "player_miss"] += 1


def _espn_game_id_for(root: Union[str, Path], league: str, season: Optional[int], contest_id: Any) -> Optional[str]:
    """This contest's ESPN event id from the offline crosswalk, or ``None``.

    Absence of a crosswalk is never an exception: an unbuilt season, an
    unmatched contest and an unreadable file all yield ``None``.
    """
    if season is None or not contest_id:
        return None
    from .espn_game_xwalk import load_espn_game_index

    return load_espn_game_index(str(root), league, season).get(str(contest_id))


def _team_name(value: Any) -> Optional[str]:
    """``lineups`` renders a team as ``{"team": {"name": "Buffalo"}, ...}``."""
    if isinstance(value, dict):
        inner = value.get("team")
        if isinstance(inner, dict):
            return inner.get("name")
        return value.get("name")
    return value if isinstance(value, str) else None


def enrich_parsed(
    parsed: Dict[str, Any],
    *,
    root: Union[str, Path],
    league: str,
    season: Optional[int] = None,
) -> Dict[str, Any]:
    """Stamp ids + readable names onto every family of one parsed game.

    Covers the ten on-court slots (``home_1``..``home_5`` / ``away_1``..
    ``away_5`` on ``pbp`` and ``possessions``) as well as the event players --
    each gets ``{slot}_player_id`` + ``{slot}_clean_name`` off the same
    game-scoped index.

    Strictly additive: existing keys are never renamed, removed or rewritten.
    The one exception is ``lineups[].players[].ncaa_id``, the parser model's own
    always-null slot for the NCAA player id, which is FILLED (that is what it
    is for) rather than shadowed by a duplicate key.

    Args:
        parsed: The dict :func:`ncaa_parse.parse_bundle` builds.
        root: Repo root -- the datasets trees live at ``{root}/{league}/``.
        league: ``"mbb"`` or ``"wbb"``.
        season: Ending year, e.g. ``2026``. ``None`` skips enrichment's joins
            and emits nulls (the season is unknown, so no index can be chosen).

    Returns:
        The same dict, mutated in place, plus a new top-level ``teams`` list
        (one row per side with the full readable team identity). Every row of
        every per-game family additionally carries ``espn_game_id`` -- always
        present, null when the season's crosswalk is unbuilt or the contest has
        no unambiguous ESPN match.
    """
    teams = load_team_index(str(root), league, season) if season is not None else {}
    rosters = load_roster_index(str(root), league, season) if season is not None else {}
    if season is not None and not (teams and rosters):
        logger.info(
            "ncaa_identity: contest_id=%s season=%s league=%s -- %s%s not built; ids emitted as nulls",
            parsed.get("contest_id"),
            season,
            league,
            "" if teams else "teams ",
            "" if rosters else "rosters",
        )

    pbp = parsed.get("pbp") or []
    home = pbp[0].get("home") if pbp else None
    away = pbp[0].get("away") if pbp else None
    sides = [("home", home), ("away", away)]
    team_ids = [teams[n]["ncaa_team_id"] for _, n in sides if n and n in teams and teams[n]["ncaa_team_id"]]
    names, codes = _game_player_index(rosters, [t for t in team_ids if t])

    stats = {
        "team_total": 0,
        "team_hit": 0,
        "team_miss": 0,
        "player_total": 0,
        "player_hit": 0,
        "player_miss": 0,
    }

    for family, columns in TEAM_COLUMNS.items():
        for row in parsed.get(family) or []:
            for column in columns:
                if family == "shots":
                    hit = teams.get(row.get(column))
                    for key in _TEAM_ID_KEYS:
                        row[key] = hit[key] if hit else None
                    if row.get(column):
                        stats["team_total"] += 1
                        stats["team_hit" if hit else "team_miss"] += 1
                else:
                    _stamp_team(row, column, teams, stats)

    for family, specs in PLAYER_COLUMNS.items():
        for row in parsed.get(family) or []:
            for name_column, id_column, readable_column in specs:
                _stamp_player(row, name_column, id_column, readable_column, names, stats)

    for row in parsed.get("shots") or []:
        code = row.get("shooter_id")
        hit = codes.get(code) if code else None
        row[_SHOTS_PLAYER_KEYS[0]] = hit[0] if hit else None
        row[_SHOTS_PLAYER_KEYS[1]] = hit[1] if hit else None
        if code:
            stats["player_total"] += 1
            stats["player_hit" if hit else "player_miss"] += 1

    for lineup in parsed.get("lineups") or []:
        for side in LINEUP_TEAM_COLUMNS:
            hit = teams.get(_team_name(lineup.get(side)))
            for key in _TEAM_ID_KEYS:
                lineup[f"{side}_{key}"] = hit[key] if hit else None
            if _team_name(lineup.get(side)):
                stats["team_total"] += 1
                stats["team_hit" if hit else "team_miss"] += 1
        for slot in ("players", "players_in", "players_out"):
            for player in lineup.get(slot) or []:
                if not isinstance(player, dict):
                    continue
                # `code` first (exact, team-scoped); the display name is the
                # fallback for a player the roster spells differently.
                hit = codes.get(player.get("code"))
                if hit is None:
                    display = (player.get("id") or {}).get("name") if isinstance(player.get("id"), dict) else None
                    hit = names.get(_key_name(display)) if display else None
                player["ncaa_id"] = hit[0] if hit else None
                stats["player_total"] += 1
                stats["player_hit" if hit else "player_miss"] += 1

    # The game-level ESPN event id, on every row of every family beside its
    # contest_id. A season with no crosswalk built (or a contest ESPN has no
    # match for) emits the key as null -- the column is always PRESENT so the
    # frame schema never varies game-to-game.
    espn_game_id = _espn_game_id_for(root, league, season, parsed.get("contest_id"))
    for family in _PER_GAME_FAMILIES:
        for row in parsed.get(family) or []:
            row[_ESPN_GAME_ID_KEY] = espn_game_id

    parsed["teams"] = [
        dict(teams[name], side=side) if name in teams else {"side": side, "team": name} for side, name in sides if name
    ]
    for team_row in parsed["teams"]:
        team_row[_ESPN_GAME_ID_KEY] = espn_game_id

    # The join rate is REPORTED, never enforced: an unmatched name keeps its row
    # with a null id (a non-D-I exhibition opponent has no crosswalk row, a
    # mid-season addition is not on the captured roster snapshot). Logged rather
    # than stored, so the committed JSON stays exactly the documented families.
    if stats["team_miss"] or stats["player_miss"]:
        logger.info(
            "ncaa_identity: contest_id=%s teams %d/%d matched, players %d/%d matched",
            parsed.get("contest_id"),
            stats["team_hit"],
            stats["team_total"],
            stats["player_hit"],
            stats["player_total"],
        )
    return parsed
