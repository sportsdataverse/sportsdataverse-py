"""Parsers for the generated ``euroleague`` wrappers (EuroLeague APIs, three hosts).

``api-live.euroleague.net`` (v2 + v3) is keyless (``Accept: application/json`` is sent
by :mod:`sportsdataverse.euroleague.euroleague_runtime`; without it the API answers
XML) and serves these body shapes, all handled by :func:`parse_euroleague`:

* **List routes** (``competitions``, ``seasons``, ``rounds``, ``clubs``, ``people``,
  ``games``, the v3 season ``statistics``) answer an envelope with one list
  (``{"total": n, "data": [...]}``, ``{"total": n, "players": [...]}``). Rows are
  the list; ``total`` is paging metadata and is dropped.
* **The v3 standings** (``rounds/{round}/basicstandings`` and its siblings) are
  ``{"winner": {...}, "teams": [...]}``: rows are ``teams`` (the season winner is
  ``null`` while the season is in progress and is dropped either way).
* **Page objects** -- the v2 box score ``{"local": {...}, "road": {...}}`` and the v3
  ``games/{gameCode}/report`` -- become **one row**: nested blocks flatten to
  prefixed columns (``local_total_points``, ``local_club_code``) and each list cell
  (``players``, ``local_last5_form``) is kept JSON-encoded.

Row rule: a list is the rows; a dict with exactly one list-valued top-level key is
that list; any other non-empty dict is a single row; anything else is zero rows.
The "id-keyed map" rule used by sibling families is deliberately **not** applied
here -- the box score's ``{"local", "road"}`` object would otherwise split into
two rows.

``live.euroleague.net/api`` (the legacy live API, per game by
``?gamecode=&seasoncode=``) has its own parsers because its keys are UPPER-CASE
fixed-width strings (``TEAM = "IST       "``):

* :func:`parse_euroleague_live` -- ``Points`` (``{"Rows": [...]}``, the shot chart)
  and ``Header`` (a flat object -> one row).
* :func:`parse_euroleague_pbp` -- ``PlayByPlay``: one array per quarter
  (``FirstQuarter`` ... ``ForthQuarter``, ``ExtraTime``) unrolled to one row per play
  with a leading ``quarter`` column.
* :func:`parse_euroleague_boxscore` -- ``Boxscore``: one row per player per side
  plus each side's team-only (``tmr``) and totals (``totr``) rows, tagged by
  ``row_type``.

Every live-API string cell is whitespace-stripped (the codes are space-padded),
and an all-null column is pinned to ``Utf8`` so a truncated or empty game keeps a
stable schema. The live API's "no such game" answer is an empty 200 body, which
the runtime hands over as ``{}`` -> a zero-row frame, never an exception.

Ids and codes (``id``, ``code``, ``*_id``, ``*_code``, ``id_*``, ``codeteam``,
``code_team_a``, ``team``, ...) are opaque join keys and are pinned to ``Utf8``;
an integer one (``gameCode``, ``externalId``) is cast through ``Int64`` first so it
can never stringify as ``"406.0"``.

Follows the package-wide parser contract: polars by default, pandas via
``return_as_pandas=True``, a zero-row frame (never an exception) on an empty or
malformed payload, and snake_cased columns.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Union

import pandas as pd
import polars as pl

from sportsdataverse.soccer._frames import as_output, rows_to_frame, to_utf8_ids

__all__ = [
    "parse_euroleague",
    "parse_euroleague_boxscore",
    "parse_euroleague_live",
    "parse_euroleague_pbp",
]

# ``id`` / ``*_id`` / ``code`` / ``*_code`` -- every api-live join key.
_ID = re.compile(r"(^|_)(id|code)$")
# The live API's join keys: ``id_player`` / ``id_action``, ``player_id``, ``codeteam``,
# ``code_team_a`` / ``code_team_b``, ``tv_code_a`` / ``tv_code_b`` and the ``team`` code.
_LIVE_ID = re.compile(r"^(id_\w+|\w+_id|codeteam|code_team_[ab]|tv_code_[ab]|team)$")

# ``PlayByPlay`` arrays in game order; every overtime period is in ``ExtraTime``.
_QUARTERS = (
    ("FirstQuarter", 1),
    ("SecondQuarter", 2),
    ("ThirdQuarter", 3),
    ("ForthQuarter", 4),
    ("ExtraTime", 5),
)


def _as_rows(raw: Union[Dict[str, Any], List[Any], None]) -> List[Any]:
    """Normalize a EuroLeague body to a row list (``[]`` for anything unusable)."""
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict) and raw:
        lists = [v for v in raw.values() if isinstance(v, list)]
        if len(lists) == 1:
            return lists[0]
        return [raw]
    return []


def parse_euroleague(
    raw: Union[Dict[str, Any], List[Any], None],
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Parse an ``api-live.euroleague.net`` (v2 / v3) body into a tidy frame.

    Covers every api-live ``euroleague`` route: the single-list envelopes
    (``competitions``, ``seasons``, ``rounds``, ``clubs``, ``people``, ``games``,
    the v3 ``player_stats`` / ``team_stats``), the v3 ``standings``
    (``{"winner", "teams"}`` -> one row per team) and the page objects that become
    a single row (the v2 ``game_stats`` box score, the v3 ``game_report``).

    Args:
        raw: a EuroLeague JSON body -- an envelope whose one list becomes the
            rows, a bare list, or a page object that becomes a single row.
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per record, snake_cased, nested objects flattened to prefixed
        columns, list cells JSON-encoded, and every ``id`` / ``*_id`` / ``code`` /
        ``*_code`` column pinned to ``Utf8``. A zero-row frame when the payload is
        ``None``, empty or malformed -- callers can chain without a null-check.

    Raises:
        None: malformed payloads yield a zero-row frame rather than an exception.

    Example:
        Quick start::

            from sportsdataverse.euroleague.euroleague import euroleague_games

            df = euroleague_games(competition_code="E", season_code="E2025")
            print(df.shape)

        Pipeline next step (one line)::

            import polars as pl

            df.filter(pl.col("played") == True).select("game_code", "local_club_code", "road_club_code")

    See Also:
        * `EuroLeague Basketball`_ -- the site the API serves.

    .. _EuroLeague Basketball: https://www.euroleaguebasketball.net/
    """
    df = rows_to_frame(_as_rows(raw))
    df = to_utf8_ids(df, [c for c in df.columns if _ID.search(c)])
    return as_output(df, return_as_pandas=return_as_pandas)


def _live_frame(rows: List[Any]) -> pl.DataFrame:
    """Finish a live-API row list: snake_case, strip the space padding, pin ids + all-null columns."""
    df = rows_to_frame(rows)
    df = df.with_columns(pl.col(pl.String).str.strip_chars())
    return to_utf8_ids(df, [c for c, t in df.schema.items() if _LIVE_ID.match(c) or t == pl.Null])


def parse_euroleague_live(
    raw: Union[Dict[str, Any], List[Any], None],
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Parse a ``live.euroleague.net/api`` ``Points`` (shot chart) or ``Header`` body.

    ``Points`` is the shot-chart source: one row per field-goal attempt (``id_action``
    ``2FGM`` / ``2FGA`` / ``3FGM`` / ``3FGA``) and per made free throw (``FTM``), with
    ``coord_x`` / ``coord_y``, ``zone``, the ``fastbreak`` / ``second_chance`` /
    ``points_off_turnover`` flags, the running score (``points_a`` / ``points_b``),
    ``minute``, the game clock (``console``) and a ``utc`` timestamp. The coordinate
    frame, as measured in the reference capture (E2025 game 1, 158 rows; U2025 game 1,
    176 rows):

    * **Units: integer centimeters. Origin: the hoop.** 2FG radii run 8-512 cm, 3FG
      radii 722-925 cm (the FIBA arc is 675 cm), zone ``A`` (rim) sits at radius
      8-33 cm.
    * **Both teams are mapped onto one basket; ``coord_y`` grows away from the
      baseline toward the court** (-6 to 865 cm for every team on the E game, -69 to
      1028 on the U game; 3FG rows sit at y 414-865). Negative y is behind the hoop.
    * ``coord_x`` is signed left/right of the hoop (-683 to 696 cm); 3-point zone
      ``H`` is x < 0 and ``I`` is x > 0. Which sideline is positive (from the
      shooter's view or from the scorer's table) is **UNVERIFIED** from the data alone.
    * **Free throws are not located**: ``FTM`` rows carry ``coord_x = coord_y = -1``
      and ``zone = ""`` (a sentinel, not a spot 1 cm from the hoop). Filter them out
      before plotting.
    * ``zone`` letters A-I: A rim, B/C close left/right, D/E mid, F/G long two, H/I three.

    ``Header`` is a flat object (teams, codes, coaches, score by quarter, venue,
    referees) and becomes one row.

    Args:
        raw: the live API JSON body -- ``{"Rows": [...]}`` for ``Points``, a flat
            object for ``Header``, or ``{}`` (the runtime's "no such game" answer).
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per shot (``Points``) or one row (``Header``), snake_cased, every
        string cell stripped of its space padding, the id / code columns pinned to
        ``Utf8``. A zero-row frame for ``{}``, ``None`` or a malformed payload.

    Raises:
        None: malformed payloads yield a zero-row frame rather than an exception.

    Example:
        Quick start::

            from sportsdataverse.euroleague.euroleague import euroleague_game_points

            shots = euroleague_game_points(game_code=1, season_code="E2025")
            print(shots.shape)

        Pipeline next step (one line)::

            import polars as pl

            shots.filter(pl.col("id_action") != "FTM").select("team", "id_action", "coord_x", "coord_y", "zone")

    See Also:
        * `EuroLeague Basketball`_ -- the site the live API serves.

    .. _EuroLeague Basketball: https://www.euroleaguebasketball.net/
    """
    return as_output(_live_frame(_as_rows(raw)), return_as_pandas=return_as_pandas)


def parse_euroleague_pbp(
    raw: Union[Dict[str, Any], List[Any], None],
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Parse a ``live.euroleague.net/api`` ``PlayByPlay`` body into one row per play.

    The body carries one array per period (``FirstQuarter``, ``SecondQuarter``,
    ``ThirdQuarter``, ``ForthQuarter`` (sic), ``ExtraTime``) beside the two team
    names and codes. The arrays are unrolled in game order with a leading
    ``quarter`` column (1-4; 5 for every overtime period, which the body does not
    split further) and the ``team_a`` / ``team_b`` / ``code_team_a`` / ``code_team_b``
    header fields repeated on every row.

    Args:
        raw: the ``PlayByPlay`` JSON body, or ``{}`` (the runtime's "no such game" answer).
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per play, snake_cased, string cells stripped of their space padding,
        ``codeteam`` / ``player_id`` / the team codes pinned to ``Utf8``. A zero-row
        frame for ``{}``, ``None`` or a malformed payload.

    Raises:
        None: malformed payloads yield a zero-row frame rather than an exception.

    Example:
        Quick start::

            from sportsdataverse.euroleague.euroleague import euroleague_game_pbp

            pbp = euroleague_game_pbp(game_code=1, season_code="E2025")
            print(pbp.shape)

        Pipeline next step (one line)::

            import polars as pl

            pbp.filter(pl.col("playtype").is_in(["2FGM", "3FGM", "FTM"])).group_by("codeteam").len()

    See Also:
        * `EuroLeague Basketball`_ -- the site the live API serves.

    .. _EuroLeague Basketball: https://www.euroleaguebasketball.net/
    """
    rows: List[Dict[str, Any]] = []
    if isinstance(raw, dict):
        head = {k: raw.get(k) for k in ("TeamA", "TeamB", "CodeTeamA", "CodeTeamB")}
        for key, quarter in _QUARTERS:
            plays = raw.get(key)
            for play in plays if isinstance(plays, list) else []:
                if isinstance(play, dict):
                    rows.append({"Quarter": quarter, **head, **play})
    df = _live_frame(rows)
    # The running score is an integer on the wire but null on the admin plays that
    # open a period; a truncated or partial body must not drift it to Utf8.
    df = df.with_columns(pl.col(c).cast(pl.Int64, strict=False) for c in ("points_a", "points_b") if c in df.columns)
    return as_output(df, return_as_pandas=return_as_pandas)


def parse_euroleague_boxscore(
    raw: Union[Dict[str, Any], List[Any], None],
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Parse a ``live.euroleague.net/api`` ``Boxscore`` body into one row per player.

    ``Stats`` holds one object per side (``Team``, ``Coach``, ``PlayersStats``,
    ``tmr`` = team-only rebounds, ``totr`` = totals). Each side's players become
    rows tagged ``row_type = "player"``, followed by the side's ``"team"`` and
    ``"total"`` rows, all carrying ``team_name`` and ``coach``. The by-quarter scores,
    referees and attendance are left in the raw payload (``return_parsed=False``);
    the quarter scores and referees are also on ``Header``.

    Args:
        raw: the ``Boxscore`` JSON body, or ``{}`` (the runtime's "no such game" answer).
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per player plus two per side, snake_cased, string cells stripped of
        their space padding, ``player_id`` / ``team`` pinned to ``Utf8``. A zero-row
        frame for ``{}``, ``None`` or a malformed payload.

    Raises:
        None: malformed payloads yield a zero-row frame rather than an exception.

    Example:
        Quick start::

            from sportsdataverse.euroleague.euroleague import euroleague_game_boxscore

            box = euroleague_game_boxscore(game_code=1, season_code="E2025")
            print(box.shape)

        Pipeline next step (one line)::

            import polars as pl

            box.filter(pl.col("row_type") == "player").sort("valuation", descending=True).head()

    See Also:
        * `EuroLeague Basketball`_ -- the site the live API serves.

    .. _EuroLeague Basketball: https://www.euroleaguebasketball.net/
    """
    rows: List[Dict[str, Any]] = []
    sides = raw.get("Stats") if isinstance(raw, dict) else None
    for side in sides if isinstance(sides, list) else []:
        if not isinstance(side, dict):
            continue
        head = {"TeamName": side.get("Team"), "Coach": side.get("Coach")}
        players = side.get("PlayersStats")
        for player in players if isinstance(players, list) else []:
            if isinstance(player, dict):
                rows.append({"RowType": "player", **head, **player})
        for key, kind in (("tmr", "team"), ("totr", "total")):
            if isinstance(side.get(key), dict):
                rows.append({"RowType": kind, **head, **side[key]})
    return as_output(_live_frame(rows), return_as_pandas=return_as_pandas)
