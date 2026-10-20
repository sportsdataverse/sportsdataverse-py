"""Parsers for the generated ``ibba`` wrappers (Israeli Premier League, basket.co.il ASMX web service).

Every ASMX op answers ``{"<key>": [row, ...]}`` where every value is a STRING: numbers, the booleans
``'True'`` / ``'False'``, and dates ``'dd/mm/yyyy'``. ``ibba_runtime`` refuses wrong-scope and error bodies
and stamps the request under ``_request``; each public ``parse_ibba_*`` here takes that dict and follows the
package parser contract: a polars frame by default, pandas with ``return_as_pandas=True``, snake_case
columns, and a zero-row frame with the documented columns and dtypes (never an untyped
``pl.DataFrame()``, never an exception) for an empty or unrecognised body.

**Dtypes** are pinned by column name in :data:`SPECS`, never sampled, so a frame parsed from any one body has
the schema of every other. Ids are ``Int64`` (cast once, here, from the digit strings; a non-digit string is
null, never a float and never ``"123.0"``). Box-score counts are ``Int64``. Percentages are ``Float64``
(0-100: whole numbers in a box score, two decimals in season stats). Every season-stats number
(``ibba_player_stats``, ``ibba_team_stats``, ``ibba_leaders``) is ``Float64``, because ``is_averages`` flips
the same column between a total and a per-game decimal. Dates are ``Date``. Text is HTML-unescaped (the host
sends Hebrew entity-encoded: ``&quot;``, ``&#39;``); an empty string is null.

**Renames.** The host's sic names are mapped once: ``jersy`` -> ``jersey``, ``tournovers`` -> ``turnovers``,
``year_id`` -> ``season`` (the END year), ``p21`` / ``p22`` / ``p2`` -> ``fg2m`` / ``fg2a`` / ``fg2_pct``
(and ``fg3*``, ``ft*`` likewise), ``fouls_against`` -> ``fouls_drawn`` (verified against Segev ``f_drawn``),
``fouls_for`` -> ``fouls`` (committed), ``VAL`` -> ``val``. Contact details (e-mail, phone, fax) and coaches'
dates of birth are dropped, not parsed.

**Id key spaces** (all ``Int64``, none interchangeable):

* ``team_uid``: the IBBA CLUB, stable across seasons (Maccabi Tel Aviv = 10). It is a parameter of the
  roster routes and a column of ``ibba_teams`` / ``ibba_team``; game rows never carry it.
* ``team_id``: a TEAM-SEASON, new every season (Maccabi = 1109 in 2025-26, 2111 in 2026-27). ``ibba_teams``
  (``team_year_id`` on the wire) joins ``ibba_players``, ``ibba_coaches``, ``ibba_box_score`` and
  ``ibba_games.home_team_id`` / ``away_team_id``.
* ``player_id``: a PLAYER-SEASON, new every season (Jaylen Hoard = 21912 in 2025-26, 25997 in 2026-27).
  ``ibba_players`` ``player_id`` equals the box score's (``player_year_id`` on the wire) and the leaders'.
  The ASMX host has no cross-season person id; Segev player ids (``ibba_segev_*``) are the only stable key.
* ``game_id``: an IBBA game. ``ibba_games`` = ``ibba_box_score``. In ``ibba_games_ex`` it is an EXTERNAL game
  (the ``ig_`` space: 8888 is a pre-season game), and on external boards (``board_id`` >= 1001) the team ids
  of ``ibba_games`` / ``ibba_games_by_team`` are external-team ids too (the ``ig_team_*`` space), not
  team-seasons.
* ``board_id``: a competition; code table in the sdv-internal-refs ``ibba-returns.md`` (5 league, 10 cup,
  16/26/17 playoff rounds, 33 play-in, 34 Supercup, 1001+ external).
* ``segev_game_id``: ``ibba_games.external_id`` read as a Segev game id when it is all digits (league games
  from 2025-26); a news URL for pre-season games gives null. It joins ``ibba_segev_*`` ``game_id``.

**Season** is the END year and the host's own ``cYear`` (2026 = 2025-26): no mapping.
"""

from __future__ import annotations

import html
import re
from datetime import date, datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import underscore

__all__ = [
    "parse_ibba_boards",
    "parse_ibba_boards_ex",
    "parse_ibba_box_score",
    "parse_ibba_coaches",
    "parse_ibba_configuration",
    "parse_ibba_games",
    "parse_ibba_games_by_team",
    "parse_ibba_games_ex",
    "parse_ibba_leaders",
    "parse_ibba_player",
    "parse_ibba_player_stats",
    "parse_ibba_players",
    "parse_ibba_stats_types",
    "parse_ibba_team",
    "parse_ibba_team_stats",
    "parse_ibba_teams",
]

Frame = Union[pl.DataFrame, pd.DataFrame]
Spec = Dict[str, pl.DataType]

INT, F, S, B, D = pl.Int64, pl.Float64, pl.Utf8, pl.Boolean, pl.Date
_KIND: Dict[str, pl.DataType] = {"i": INT, "n": INT, "f": F, "s": S, "b": B, "d": D}

#: Wire name -> output name for the names the host spells badly or inconsistently across routes.
_RENAME = {
    "jersy": "jersey",
    "p21": "fg2m",
    "p22": "fg2a",
    "p2": "fg2_pct",
    "p31": "fg3m",
    "p32": "fg3a",
    "p3": "fg3_pct",
    "p11": "ftm",
    "p12": "fta",
    "p1": "ft_pct",
    "defensive_reb": "dreb",
    "offensive_reb": "oreb",
    "total_reb": "reb",
    "fouls_against": "fouls_drawn",
    "fouls_for": "fouls",
    "tournovers": "turnovers",
    "blocks_for": "blocks",
    "block_against": "blocks_against",
    "VAL": "val",
    "plusminus": "plus_minus",
    "dunk": "dunks",
    "year_id": "season",
    "team_year_id": "team_id",
    "TeamUID": "team_uid",
    "player_year_id": "player_id",
    "BOARD_ID": "board_id",
    "board": "board_id",
}
_STAT = (
    "minutes:K pts:K p21:K p22:K p2:f p31:K p32:K p3:f p11:K p12:K p1:f defensive_reb:K offensive_reb:K total_reb:K "
    "fouls_against:K fouls_for:K steals:K tournovers:K assists:K blocks_for:K block_against:K VAL:K plusminus:K dunk:K"
)
_STAT_INT, _STAT_FLOAT = _STAT.replace(":K", ":i"), _STAT.replace(":K", ":f")

#: route -> (body key, token spec). A token is ``wire[>out]:kind`` (kinds: i id/count, n id with 0 = none,
#: f float, s text, b boolean, d dd/mm/yyyy date); ``out`` defaults to the :data:`_RENAME` entry or the
#: snake_case wire name.
_ROUTES: Dict[str, Tuple[str, str]] = {
    "configuration": (
        "settings",
        "Current_Year>season:i Current_Year_Full>season_label:s red_line:i ActiveComp>active_board_id:i "
        "nextGN>next_round:i TotalGN>total_rounds:i",
    ),
    "boards": (
        "boards",
        "BOARD_ID:i year_id:i h_type>board_name_he:s type>board_title_he:s category_name>category_he:s",
    ),
    "boards_ex": ("boards-ex", "ig_board>board_id:i o_comp_name>board_name_he:s"),
    "teams": (
        "teams",
        "TeamUID:i team_year_id:i year_id:i team>team_name_he:s team_shortName>team_short_name_he:s "
        "teamEng>team_name:s team_shortNameEng>team_short_name:s board:i place:i games:i win>wins:i lose>losses:i "
        "team_scores>points_for:i rival_scores>points_against:i points>table_points:i team_icon2>logo_url:s "
        "team_pic>photo_path:s team_address>address_he:s team_sponsers>sponsors_he:s "
        "team_social_link>facebook_url:s team_twitter>twitter_url:s team_insta>instagram_url:s "
        "team_youtube>youtube_url:s ticket_link>ticket_url:s team_subscribe_url>subscribe_url:s COACHES>coaches_he:s",
    ),
    "team": (
        "team",
        "TeamUID:i team_year_id:i year_id:i team>team_name_he:s team_shortName>team_short_name_he:s "
        "teamEng>team_name:s team_shortNameEng>team_short_name:s win>wins:i lose>losses:i "
        "team_scores>points_for:i rival_scores>points_against:i points>table_points:i place:i "
        "team_site>website_url:s team_pic>photo_path:s team_icon>icon_path:s team_icon2>logo_url:s "
        "team_icon3>icon3_path:s team_arena_id>arena_id:i team_address>address_he:s team_addressEng>address:s "
        "team_color>colors_he:s team_colorEng>colors:s team_sponsers>sponsors_he:s team_sponsersEng>sponsors:s "
        "team_About>about_he:s team_AboutEng>about:s team_social_link>facebook_url:s team_twitter>twitter_url:s "
        "team_insta>instagram_url:s team_youtube>youtube_url:s team_subscribe_url>subscribe_url:s "
        "addDiaryCode>diary_url:s",
    ),
    "players": (
        "players",
        "player_id:i jersy:s year_id:i team_id:i name>player_name_he:s name_eng>player_name:s "
        "isReplaced>is_replaced:b isLocal>is_local:b pic>photo_path:s Height>height_m:f position>position_he:s "
        "position_eng>position:s birth_date:d",
    ),
    "player": (
        "player",
        "player_id:i jersy:s year_id:i team_id:i name>player_name_he:s name_eng>player_name:s "
        "isReplaced>is_replaced:b isLocal>is_local:b pic>photo_path:s",
    ),
    "coaches": (
        "coaches",
        "coach_id:i cType>coach_type:i year_id:i team_id:i name>coach_name_he:s name_eng>coach_name:s "
        "isReplaced>is_replaced:b pic>photo_path:s mOrder>sort_order:i",
    ),
    "box_score": (
        "boxscore",
        "player_year_id:n jersy:s team_id:i game_id:i board:i year_id:i round:i starter:b " + _STAT_INT,
    ),
    "player_stats": ("player", "player_id:i jersy:s year_id:i team_id:i games:i " + _STAT_FLOAT),
    "stats_types": ("statsTypes", "stat_type_field>stat_type:s stat_type_name>stat_name_he:s"),
    "leaders": (
        "leaders",
        "DATA>value:f GP>games:i Year>season:i team_id:i player_number>jersey:s player_year_id>player_id:i "
        "player_name>player_name_he:s pic>photo_path:s",
    ),
    "team_stats": ("team-stats", "year_id:i team_id:i games:i " + _STAT_FLOAT),
    "games": (
        "games",
        "game_id:i game_date_txt>game_date:d year_id:i team1>home_team_id:i team2>away_team_id:i "
        "team1_name>home_team_he:s team2_name>away_team_he:s team1_name_eng>home_team:s team2_name_eng>away_team:s "
        "team1_logo>home_logo_url:s team2_logo>away_logo_url:s score_team1>home_score:i score_team2>away_score:i "
        "BOARD_ID:i board_name>board_name_he:s board_name_eng>board_name:s gameNumber>round:i TimeOfGame>tip_off:s "
        "PlaceOfGameHeb>venue_he:s PlaceOfGame>venue:s Ref>referees_he:s total_viewers>attendance:i "
        "overtime:b total_ot>overtimes:i score_by_q_1>home_score_by_period:s score_by_q_2>away_score_by_period:s "
        "tickets_url:s ExternalID>external_id:s",
    ),
    "games_ex": (
        "games_ex",
        "game_id:i ig_date>game_date:d year_id:i ig_team_1>home_team_id:i ig_team_2>away_team_id:i "
        "team1_name>home_team_he:s team2_name>away_team_he:s team1_logo>home_logo_url:s "
        "team2_logo>away_logo_url:s ig_score_team_1>home_score:i ig_score_team_2>away_score:i BOARD_ID:i "
        "board_name>board_name_he:s ig_gn>round:i ig_time>tip_off:s ig_place>venue_he:s",
    ),
}
_ROUTES["games_by_team"] = ("games", _ROUTES["games"][1])
#: Columns a parser derives (after the wire columns).
_DERIVED: Dict[str, Spec] = {
    "box_score": {"row_type": S},
    "games": {"segev_game_id": INT, "is_external": B},
    "games_by_team": {"segev_game_id": INT, "is_external": B},
}


def _tokens(spec: str) -> List[Tuple[str, str, pl.DataType, str]]:
    out = []
    for tok in spec.split():
        head, kind = tok.rsplit(":", 1)
        wire, _, name = head.partition(">")
        out.append((wire, name or _RENAME.get(wire) or underscore(wire), _KIND[kind], kind))
    return out


_COLS = {short: (key, _tokens(spec)) for short, (key, spec) in _ROUTES.items()}
#: Each parser's column dtypes, for the generator and the tests.
SPECS: Dict[str, Spec] = {
    short: {**{name: dtype for _w, name, dtype, _k in toks}, **_DERIVED.get(short, {})}
    for short, (_key, toks) in _COLS.items()
}

_DIGITS = re.compile(r"-?\d+")


# ---------------------------------------------------------------------------------------- cells
def _text(v: Any) -> Optional[str]:
    if v is None or isinstance(v, (dict, list)):
        return None
    s = html.unescape(str(v)).strip()
    return s or None


def _int(v: Any) -> Optional[int]:
    s = _text(v)
    return int(s) if s is not None and _DIGITS.fullmatch(s) else None


def _float(v: Any) -> Optional[float]:
    s = _text(v)
    if s is None:
        return None
    try:
        return float(s.replace(",", "."))
    except ValueError:
        return None


def _bool(v: Any) -> Optional[bool]:
    s = (_text(v) or "").lower()
    return True if s in ("true", "1") else False if s in ("false", "0") else None


def _date(v: Any) -> Optional[date]:
    s = _text(v)
    try:
        return datetime.strptime(s, "%d/%m/%Y").date() if s else None
    except ValueError:
        return None


_CONVERT: Dict[str, Callable[[Any], Any]] = {
    "i": _int,
    "n": lambda v: _int(v) or None,  # 0 = "no player" on the _TEAM and TOTAL rows
    "f": _float,
    "s": _text,
    "b": _bool,
    "d": _date,
}


def _frame(rows: List[Dict[str, Any]], spec: Spec, columns: Optional[Sequence[str]], as_pandas: bool) -> Frame:
    """``rows`` as a frame with exactly ``spec``'s columns and dtypes (a zero-row frame when there are none).

    ``columns`` is the wrapper's documented column list; a name the spec lacks is added as a null string column.
    """
    full: Dict[str, pl.DataType] = dict(spec)
    for c in columns or ():
        full.setdefault(c, S)
    df = pl.DataFrame({c: [r.get(c) for r in rows] for c in full}, schema=full)
    return df.to_pandas() if as_pandas else df


def _derive(short: str, row: Dict[str, Any]) -> None:
    if short == "box_score":
        j = row.get("jersey")
        row["row_type"] = "team" if j == "_TEAM" else "total" if j == "TOTAL" else "player"
    elif short in ("games", "games_by_team"):
        ext = row.get("external_id")
        row["segev_game_id"] = int(ext) if ext is not None and ext.isdigit() else None
        board = row.get("board_id")
        row["is_external"] = None if board is None else board >= 1001


def _parse(short: str, raw: Any, as_pandas: bool, columns: Optional[Sequence[str]]) -> Frame:
    key, toks = _COLS[short]
    body = raw.get(key) if isinstance(raw, dict) else None
    rows: List[Dict[str, Any]] = []
    for r in body if isinstance(body, list) else []:
        if not isinstance(r, dict):
            continue
        row = {name: _CONVERT[kind](r.get(wire)) for wire, name, _dt, kind in toks}
        _derive(short, row)
        rows.append(row)
    return _frame(rows, SPECS[short], columns, as_pandas)


# ------------------------------------------------------------------------------ public parsers


def parse_ibba_configuration(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """One row: the season's settings (current season label, round counters, standings cut line).

    Args:
        raw: the dict ``ibba_configuration`` returns with ``return_parsed=False`` (the ASMX body, keyed
            ``settings``, plus ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        A frame with the ``configuration`` schema in ``SPECS``: ids ``Int64``, counts ``Int64`` or ``Float64`` as
        pinned there, dates ``Date``, text HTML-unescaped. An empty, malformed or unrecognised body gives a
        zero-row frame with the same columns and dtypes, so two empty frames concatenate.
        Bad input is a zero-row frame. The fetch errors (``NoDataError``, ``AssetFetchError``) come
        from ``ibba_configuration`` itself.

    Example:
        Quick start::

            from sportsdataverse.basketball.ibba import ibba_configuration, parse_ibba_configuration

            raw = ibba_configuration(season=2026, return_parsed=False)
            df = parse_ibba_configuration(raw)
    """
    return _parse("configuration", raw, return_as_pandas, columns)


def parse_ibba_boards(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """One row per competition (board) of the season: league, cup, playoff rounds, play-in, Supercup.

    Args:
        raw: the dict ``ibba_boards`` returns with ``return_parsed=False`` (the ASMX body, keyed
            ``boards``, plus ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        A frame with the ``boards`` schema in ``SPECS``: ids ``Int64``, counts ``Int64`` or ``Float64`` as
        pinned there, dates ``Date``, text HTML-unescaped. An empty, malformed or unrecognised body gives a
        zero-row frame with the same columns and dtypes, so two empty frames concatenate.
        Bad input is a zero-row frame. The fetch errors (``NoDataError``, ``AssetFetchError``) come
        from ``ibba_boards`` itself.

    Example:
        Quick start::

            from sportsdataverse.basketball.ibba import ibba_boards, parse_ibba_boards

            raw = ibba_boards(season=2026, return_parsed=False)
            df = parse_ibba_boards(raw)
    """
    return _parse("boards", raw, return_as_pandas, columns)


def parse_ibba_boards_ex(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """One row per external competition a club played in (European cups, national team, pre-season).

    Args:
        raw: the dict ``ibba_boards_ex`` returns with ``return_parsed=False`` (the ASMX body, keyed
            ``boards-ex``, plus ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        A frame with the ``boards_ex`` schema in ``SPECS``: ids ``Int64``, counts ``Int64`` or ``Float64`` as
        pinned there, dates ``Date``, text HTML-unescaped. An empty, malformed or unrecognised body gives a
        zero-row frame with the same columns and dtypes, so two empty frames concatenate.
        Bad input is a zero-row frame. The fetch errors (``NoDataError``, ``AssetFetchError``) come
        from ``ibba_boards_ex`` itself.

    Example:
        Quick start::

            from sportsdataverse.basketball.ibba import ibba_boards_ex, parse_ibba_boards_ex

            raw = ibba_boards_ex(team_uid=10, season=2026, return_parsed=False)
            df = parse_ibba_boards_ex(raw)
    """
    return _parse("boards_ex", raw, return_as_pandas, columns)


def parse_ibba_teams(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """One row per team of a board with its standings line and club profile fields.

    Args:
        raw: the dict ``ibba_teams`` returns with ``return_parsed=False`` (the ASMX body, keyed
            ``teams``, plus ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        A frame with the ``teams`` schema in ``SPECS``: ids ``Int64``, counts ``Int64`` or ``Float64`` as
        pinned there, dates ``Date``, text HTML-unescaped. An empty, malformed or unrecognised body gives a
        zero-row frame with the same columns and dtypes, so two empty frames concatenate.
        Bad input is a zero-row frame. The fetch errors (``NoDataError``, ``AssetFetchError``) come
        from ``ibba_teams`` itself.

    Example:
        Quick start::

            from sportsdataverse.basketball.ibba import ibba_teams, parse_ibba_teams

            raw = ibba_teams(season=2026, board_id=5, return_parsed=False)
            df = parse_ibba_teams(raw)
    """
    return _parse("teams", raw, return_as_pandas, columns)


def parse_ibba_team(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """One row: a team-season's standings line and club profile.

    Args:
        raw: the dict ``ibba_team`` returns with ``return_parsed=False`` (the ASMX body, keyed
            ``team``, plus ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        A frame with the ``team`` schema in ``SPECS``: ids ``Int64``, counts ``Int64`` or ``Float64`` as
        pinned there, dates ``Date``, text HTML-unescaped. An empty, malformed or unrecognised body gives a
        zero-row frame with the same columns and dtypes, so two empty frames concatenate.
        Bad input is a zero-row frame. The fetch errors (``NoDataError``, ``AssetFetchError``) come
        from ``ibba_team`` itself.

    Example:
        Quick start::

            from sportsdataverse.basketball.ibba import ibba_team, parse_ibba_team

            raw = ibba_team(team_id=1109, team_uid=10, season=2026, return_parsed=False)
            df = parse_ibba_team(raw)
    """
    return _parse("team", raw, return_as_pandas, columns)


def parse_ibba_players(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """One row per player on a team-season roster.

    Args:
        raw: the dict ``ibba_players`` returns with ``return_parsed=False`` (the ASMX body, keyed
            ``players``, plus ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        A frame with the ``players`` schema in ``SPECS``: ids ``Int64``, counts ``Int64`` or ``Float64`` as
        pinned there, dates ``Date``, text HTML-unescaped. An empty, malformed or unrecognised body gives a
        zero-row frame with the same columns and dtypes, so two empty frames concatenate.
        Bad input is a zero-row frame. The fetch errors (``NoDataError``, ``AssetFetchError``) come
        from ``ibba_players`` itself.

    Example:
        Quick start::

            from sportsdataverse.basketball.ibba import ibba_players, parse_ibba_players

            raw = ibba_players(team_id=1109, team_uid=10, season=2026, return_parsed=False)
            df = parse_ibba_players(raw)
    """
    return _parse("players", raw, return_as_pandas, columns)


def parse_ibba_player(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """One row: a player-season (jersey, team, name, photo).

    Args:
        raw: the dict ``ibba_player`` returns with ``return_parsed=False`` (the ASMX body, keyed
            ``player``, plus ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        A frame with the ``player`` schema in ``SPECS``: ids ``Int64``, counts ``Int64`` or ``Float64`` as
        pinned there, dates ``Date``, text HTML-unescaped. An empty, malformed or unrecognised body gives a
        zero-row frame with the same columns and dtypes, so two empty frames concatenate.
        Bad input is a zero-row frame. The fetch errors (``NoDataError``, ``AssetFetchError``) come
        from ``ibba_player`` itself.

    Example:
        Quick start::

            from sportsdataverse.basketball.ibba import ibba_player, parse_ibba_player

            raw = ibba_player(player_id=21912, season=2026, return_parsed=False)
            df = parse_ibba_player(raw)
    """
    return _parse("player", raw, return_as_pandas, columns)


def parse_ibba_coaches(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """One row per coach on a team-season staff (birth dates are dropped).

    Args:
        raw: the dict ``ibba_coaches`` returns with ``return_parsed=False`` (the ASMX body, keyed
            ``coaches``, plus ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        A frame with the ``coaches`` schema in ``SPECS``: ids ``Int64``, counts ``Int64`` or ``Float64`` as
        pinned there, dates ``Date``, text HTML-unescaped. An empty, malformed or unrecognised body gives a
        zero-row frame with the same columns and dtypes, so two empty frames concatenate.
        Bad input is a zero-row frame. The fetch errors (``NoDataError``, ``AssetFetchError``) come
        from ``ibba_coaches`` itself.

    Example:
        Quick start::

            from sportsdataverse.basketball.ibba import ibba_coaches, parse_ibba_coaches

            raw = ibba_coaches(team_id=1109, team_uid=10, season=2026, return_parsed=False)
            df = parse_ibba_coaches(raw)
    """
    return _parse("coaches", raw, return_as_pandas, columns)


def parse_ibba_box_score(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """One row per box-score line of a finished game: players, per-team '_TEAM' rows and TOTAL rows.

    Args:
        raw: the dict ``ibba_box_score`` returns with ``return_parsed=False`` (the ASMX body, keyed
            ``boxscore``, plus ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        A frame with the ``box_score`` schema in ``SPECS``: ids ``Int64``, counts ``Int64`` or ``Float64`` as
        pinned there, dates ``Date``, text HTML-unescaped. An empty, malformed or unrecognised body gives a
        zero-row frame with the same columns and dtypes, so two empty frames concatenate.
        Bad input is a zero-row frame. The fetch errors (``NoDataError``, ``AssetFetchError``) come
        from ``ibba_box_score`` itself.

    Example:
        Quick start::

            from sportsdataverse.basketball.ibba import ibba_box_score, parse_ibba_box_score

            raw = ibba_box_score(game_id=26392, season=2026, return_parsed=False)
            df = parse_ibba_box_score(raw)
    """
    return _parse("box_score", raw, return_as_pandas, columns)


def parse_ibba_player_stats(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """One row: a player-season's totals, or per-game averages with is_averages=True.

    Args:
        raw: the dict ``ibba_player_stats`` returns with ``return_parsed=False`` (the ASMX body, keyed
            ``player``, plus ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        A frame with the ``player_stats`` schema in ``SPECS``: ids ``Int64``, counts ``Int64`` or ``Float64`` as
        pinned there, dates ``Date``, text HTML-unescaped. An empty, malformed or unrecognised body gives a
        zero-row frame with the same columns and dtypes, so two empty frames concatenate.
        Bad input is a zero-row frame. The fetch errors (``NoDataError``, ``AssetFetchError``) come
        from ``ibba_player_stats`` itself.

    Example:
        Quick start::

            from sportsdataverse.basketball.ibba import ibba_player_stats, parse_ibba_player_stats

            raw = ibba_player_stats(player_id=21912, season=2026, return_parsed=False)
            df = parse_ibba_player_stats(raw)
    """
    return _parse("player_stats", raw, return_as_pandas, columns)


def parse_ibba_stats_types(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """One row per stat category ibba_leaders accepts.

    Args:
        raw: the dict ``ibba_stats_types`` returns with ``return_parsed=False`` (the ASMX body, keyed
            ``statsTypes``, plus ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        A frame with the ``stats_types`` schema in ``SPECS``: ids ``Int64``, counts ``Int64`` or ``Float64`` as
        pinned there, dates ``Date``, text HTML-unescaped. An empty, malformed or unrecognised body gives a
        zero-row frame with the same columns and dtypes, so two empty frames concatenate.
        Bad input is a zero-row frame. The fetch errors (``NoDataError``, ``AssetFetchError``) come
        from ``ibba_stats_types`` itself.

    Example:
        Quick start::

            from sportsdataverse.basketball.ibba import ibba_stats_types, parse_ibba_stats_types

            raw = ibba_stats_types(return_parsed=False)
            df = parse_ibba_stats_types(raw)
    """
    return _parse("stats_types", raw, return_as_pandas, columns)


def parse_ibba_leaders(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """The top five players in a stat category (all teams, or one team).

    Args:
        raw: the dict ``ibba_leaders`` returns with ``return_parsed=False`` (the ASMX body, keyed
            ``leaders``, plus ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        A frame with the ``leaders`` schema in ``SPECS``: ids ``Int64``, counts ``Int64`` or ``Float64`` as
        pinned there, dates ``Date``, text HTML-unescaped. An empty, malformed or unrecognised body gives a
        zero-row frame with the same columns and dtypes, so two empty frames concatenate.
        Bad input is a zero-row frame. The fetch errors (``NoDataError``, ``AssetFetchError``) come
        from ``ibba_leaders`` itself.

    Example:
        Quick start::

            from sportsdataverse.basketball.ibba import ibba_leaders, parse_ibba_leaders

            raw = ibba_leaders(stat_type='pts', season=2026, return_parsed=False)
            df = parse_ibba_leaders(raw)
    """
    return _parse("leaders", raw, return_as_pandas, columns)


def parse_ibba_team_stats(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """One row: a team-season's totals, or per-game averages with is_averages=True.

    Args:
        raw: the dict ``ibba_team_stats`` returns with ``return_parsed=False`` (the ASMX body, keyed
            ``team-stats``, plus ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        A frame with the ``team_stats`` schema in ``SPECS``: ids ``Int64``, counts ``Int64`` or ``Float64`` as
        pinned there, dates ``Date``, text HTML-unescaped. An empty, malformed or unrecognised body gives a
        zero-row frame with the same columns and dtypes, so two empty frames concatenate.
        Bad input is a zero-row frame. The fetch errors (``NoDataError``, ``AssetFetchError``) come
        from ``ibba_team_stats`` itself.

    Example:
        Quick start::

            from sportsdataverse.basketball.ibba import ibba_team_stats, parse_ibba_team_stats

            raw = ibba_team_stats(team_id=1109, season=2026, return_parsed=False)
            df = parse_ibba_team_stats(raw)
    """
    return _parse("team_stats", raw, return_as_pandas, columns)


def parse_ibba_games(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """One row per game (schedule and results) matching the board, round, team and game filters.

    Args:
        raw: the dict ``ibba_games`` returns with ``return_parsed=False`` (the ASMX body, keyed
            ``games``, plus ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        A frame with the ``games`` schema in ``SPECS``: ids ``Int64``, counts ``Int64`` or ``Float64`` as
        pinned there, dates ``Date``, text HTML-unescaped. An empty, malformed or unrecognised body gives a
        zero-row frame with the same columns and dtypes, so two empty frames concatenate.
        Bad input is a zero-row frame. The fetch errors (``NoDataError``, ``AssetFetchError``) come
        from ``ibba_games`` itself.

    Example:
        Quick start::

            from sportsdataverse.basketball.ibba import ibba_games, parse_ibba_games

            raw = ibba_games(season=2026, board_id=5, return_parsed=False)
            df = parse_ibba_games(raw)
    """
    return _parse("games", raw, return_as_pandas, columns)


def parse_ibba_games_ex(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """One row per game a club played in an external competition.

    Args:
        raw: the dict ``ibba_games_ex`` returns with ``return_parsed=False`` (the ASMX body, keyed
            ``games_ex``, plus ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        A frame with the ``games_ex`` schema in ``SPECS``: ids ``Int64``, counts ``Int64`` or ``Float64`` as
        pinned there, dates ``Date``, text HTML-unescaped. An empty, malformed or unrecognised body gives a
        zero-row frame with the same columns and dtypes, so two empty frames concatenate.
        Bad input is a zero-row frame. The fetch errors (``NoDataError``, ``AssetFetchError``) come
        from ``ibba_games_ex`` itself.

    Example:
        Quick start::

            from sportsdataverse.basketball.ibba import ibba_games_ex, parse_ibba_games_ex

            raw = ibba_games_ex(team_uid=10, season=2026, return_parsed=False)
            df = parse_ibba_games_ex(raw)
    """
    return _parse("games_ex", raw, return_as_pandas, columns)


def parse_ibba_games_by_team(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """One row per game of a club in a season, domestic and external.

    Args:
        raw: the dict ``ibba_games_by_team`` returns with ``return_parsed=False`` (the ASMX body, keyed
            ``games``, plus ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        A frame with the ``games_by_team`` schema in ``SPECS``: ids ``Int64``, counts ``Int64`` or ``Float64`` as
        pinned there, dates ``Date``, text HTML-unescaped. An empty, malformed or unrecognised body gives a
        zero-row frame with the same columns and dtypes, so two empty frames concatenate.
        Bad input is a zero-row frame. The fetch errors (``NoDataError``, ``AssetFetchError``) come
        from ``ibba_games_by_team`` itself.

    Example:
        Quick start::

            from sportsdataverse.basketball.ibba import ibba_games_by_team, parse_ibba_games_by_team

            raw = ibba_games_by_team(team_uid=10, season=2026, return_parsed=False)
            df = parse_ibba_games_by_team(raw)
    """
    return _parse("games_by_team", raw, return_as_pandas, columns)
