"""Parsers for the generated ``sihf_api_web`` wrappers (``data.sihf.ch/statistic/api/cms``).

The SIHF statistics API serves two body shapes:

* **Tables** (``/table?alias=results|standing``, the 11 ``team*``, 9 ``player*`` and 4
  ``goalkeeper*`` aliases) --
  ``{header: [{alias, name, ...}], data: [[cell, ...], ...], filters: [...],
  highlightedPositions: [...]}``. Rows are positional lists aligned with ``header``.
  A cell is a string, or an object tagged by ``type``: ``team`` (``id`` / ``name`` /
  ``acronym``), ``result`` (home / away score), ``periods`` (home / away per-period
  lists), ``gameStatus``, ``linkToGameDetail`` (the ``gameId``) or ``broadcasts``.
  :func:`parse_sihf_table` turns one into a frame keyed by the header aliases.
* **The game page** (``/gameoverview?alias=gameDetail&searchQuery={game_id}``) -- one
  object per game: the event timeline (``summary.periods[]`` with ``goals``,
  ``fouls``, ``goalkeepers``, ``revokedGoals``, plus ``summary.shootout.shoots``),
  ``lineUps``, the ``players`` roster (licence number -> name / jersey), five embedded
  tables in ``stats`` (per-side player and goalie box scores, team stats), and the game
  header (league, phase, venue, officials, score). Each game wrapper reads one part.

Every frame follows the package parser contract: polars by default, pandas via
``return_as_pandas=True``, snake_cased columns, and a zero-row frame (never an
exception) on an empty or malformed payload. The game frames keep their documented
columns even when empty. Ids (``game_id``, ``team_id``, ``player_id``, the SIHF
licence numbers, ``league_id``, ``phase_id``) are ``Int64`` join keys; a licence
number of ``0`` means "no player" upstream and becomes null.

The ``language`` query parameter translates only part of a body: status names,
positions and penalty names stay German (``Ende``, ``Stürmer``, ``Haken``) whatever
is asked.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Union

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import underscore

__all__ = [
    "parse_sihf_game_goalie_stats",
    "parse_sihf_game_info",
    "parse_sihf_game_lineups",
    "parse_sihf_game_player_stats",
    "parse_sihf_game_team_stats",
    "parse_sihf_pbp",
    "parse_sihf_phases",
    "parse_sihf_table",
]

Frame = Union[pl.DataFrame, pd.DataFrame]

# Table header aliases whose string cells are numbers. Anything not listed stays Utf8;
# a value that does not parse (``""``, ``"-"``) becomes null rather than raising.
_INT_ALIASES = frozenset(
    {
        "rank",
        "gamesPlayed",
        "points",
        "gamesWonInTime",
        "gamesWonExtraTime",
        "gamesWonPenalty",
        "gamesLostPenalty",
        "gamesLostExtraTime",
        "gamesLostInTime",
        "goalsFor",
        "goalsAgainst",
        "goalsDifference",
        "goals",
        "assists",
        "assists1",
        "assists2",
        "pimTotal",
        "plusMinus",
        "firstKeeper",
        "shotsAgainst",
        "saves",
        "penaltyInMinutes",
        "playerNumber",
        "shotsOnGoal",
        "sogMissed",
        "sogPost",
        "blockings",
        "faceoffsWon",
        "faceoffsLost",
        # the team / skater / goalie detail tables (counts in every row of the full 2025-26 NL bodies;
        # the two attendance averages are whole numbers with a thousands separator, ``5’276``)
        "eqAssists",
        "eqGoals",
        "evenGoalsAgainst",
        "evenGoalsFor",
        "faceoffs",
        "faceoffsDefensive",
        "faceoffsLostDefensive",
        "faceoffsLostNeutral",
        "faceoffsLostOffensive",
        "faceoffsNeutral",
        "faceoffsOffensive",
        "faceoffsWonDefensive",
        "faceoffsWonNeutral",
        "faceoffsWonOffensive",
        "gameMisconducts",
        "gameWinningGoals",
        "goalsForDf",
        "goalsForFw",
        "goalsForFwDf",
        "goalsForIm",
        "hattricks",
        "homeGames",
        "losses",
        "lossesIntime",
        "lossesOvertime",
        "lossesPenalty",
        "matchPenalties",
        "overtimeGoals",
        "penShotGoals",
        "penShotGoalsAgainst",
        "penShotSaves",
        "penShotWinningGoals",
        "penShots",
        "penShotsAgainst",
        "penShotsMissed",
        "pim10",
        "pim2",
        "pim5",
        "pkAssists",
        "pkGoals",
        "pkGoalsAgainst",
        "pkSituations",
        "ppAssists",
        "ppGoals",
        "ppGoalsAgainst",
        "ppGoalsFor",
        "ppOpportunities",
        "shGoals",
        "shifts",
        "shutout",
        "sogSog",
        "sogSogSlot",
        "sogaMissed",
        "sogaPost",
        "sogaSog",
        "sogaSogSlot",
        "sowGoalsAgainst",
        "sowGoalsFor",
        "spectators",
        "wins",
        "winsIntime",
        "winsOvertime",
        "winsPenalty",
    }
)
# Rates: decimals (``"2.25"``) in every row of the full bodies. Every ``*Quotient`` (a per-game ratio)
# is a rate, Float64 even where SIHF rounds it to a whole number (``spectatorsQuotient`` ``"5’276"``),
# so a quotient never changes dtype between tables. The ``timeOnIce*``, ``powerPlayTime*`` and
# ``penaltyKillTime*`` columns are ``"mm:ss"`` strings, quotient or not, and stay String.
_FLOAT_ALIASES = frozenset(
    {
        "spectatorsQuotient",
        "stadiumCapacityQuotient",
        "pointsQuotient",
        "pointsPerGame",
        "goalsAgainstAverage",
        "savesPercentage",
        "faceoffsPercent",
        "blockingsPerGame",
        "evenGoalsAgainstQuotient",
        "evenGoalsForQuotient",
        "faceoffsDefensivePerGame",
        "faceoffsDefensivePercent",
        "faceoffsLostDefensivePerGame",
        "faceoffsLostNeutralPerGame",
        "faceoffsLostOffensivePerGame",
        "faceoffsLostPerGame",
        "faceoffsNeutralPerGame",
        "faceoffsNeutralPercent",
        "faceoffsOffensivePerGame",
        "faceoffsOffensivePercent",
        "faceoffsPerGame",
        "faceoffsWonDefensivePerGame",
        "faceoffsWonNeutralPerGame",
        "faceoffsWonOffensivePerGame",
        "faceoffsWonPerGame",
        "goalsAgainstQuotient",
        "goalsForDfPercent",
        "goalsForFwPercent",
        "goalsForImPercent",
        "goalsForQuotient",
        "penShotSavesPercent",
        "penShotsPercent",
        "pkGoalsAgainstQuotient",
        "pkPercent",
        "pkSituationsQuotient",
        "ppGoalsAgainstQuotient",
        "ppGoalsForQuotient",
        "ppOpportunitiesQuotient",
        "ppPercent",
        "savesSlotPercentage",
        "shGoalsQuotient",
        "shiftsPerGame",
        "sogMissedPerGame",
        "sogPostPerGame",
        "sogSogPercent",
        "sogSogQuotient",
        "sogSogSlotPerGame",
        "sogaMissedPerGame",
        "sogaPostPerGame",
        "sogaSogPerGame",
        "sogaSogSlotPerGame",
        "stadiumCapacityPercent",
    }
)
# highlightedPositions styles that mark a summary row (the rest -- ``line`` -- are visual separators).
_SUMMARY_STYLES = frozenset({"average", "total"})
# The pseudo-team of a summary row: team id 100000, named ``Durchschnitt`` (league average) or ``Total``.
# An empty rank is NOT a signal on its own: every goaltender below the qualification line has one too.
_PSEUDO_TEAM_ID = 100000
_PSEUDO_TEAM_NAMES = {"Durchschnitt": "average", "Total": "total"}
# The thousands separator in attendance figures (``3’458’381``).
_THOUSANDS = "[’']"
# Season-table columns whose dtype neither their header alias nor the ``*_id`` rule gives: the
# expanded ``result`` / ``periods`` / ``gameStatus`` cells, ``date`` and the echoed ``season``.
_TABLE_DTYPES: Dict[str, Any] = {
    "season": pl.Int64,
    "date": pl.Date,
    "score_home": pl.Int64,
    "score_away": pl.Int64,
    "score_per_period_home": pl.List(pl.Int64),
    "score_per_period_away": pl.List(pl.Int64),
    "status_percent": pl.Int64,
    "status_canceled": pl.Boolean,
}


def _out(df: pl.DataFrame, return_as_pandas: bool) -> Frame:
    return df.to_pandas() if return_as_pandas else df


def _col(alias: str) -> str:
    return underscore(str(alias)).replace(" ", "_").replace(".", "_")


_INT_COLS = frozenset(_col(a) for a in _INT_ALIASES)
_FLOAT_COLS = frozenset(_col(a) for a in _FLOAT_ALIASES)


def _table_dtype(col: str) -> Any:
    """The documented dtype of a season-table column (String for one the parser does not type)."""
    if col in _TABLE_DTYPES:
        return _TABLE_DTYPES[col]
    if col == "id" or col.endswith("_id") or col in _INT_COLS:
        return pl.Int64
    return pl.Float64 if col in _FLOAT_COLS else pl.String


def _raw_cell_dtype(col: str) -> Any:
    """The dtype a table column holds BEFORE ``_typed``: the parser's own ids, scores, period lists
    and flags keep their dtype; every header cell is still a string (``_typed`` casts it after)."""
    dtype = _table_dtype(col)
    if isinstance(dtype, pl.List) or dtype == pl.Boolean:
        return dtype
    if dtype == pl.Int64 and (col in _TABLE_DTYPES or col == "id" or col.endswith("_id")):
        return pl.Int64
    return pl.String


def _int(value: Any) -> Optional[int]:
    """``int(value)`` for an int or a digit string; ``None`` for anything else (incl. ``0`` ids handled by callers)."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        text = re.sub(_THOUSANDS, "", value.strip())
        return int(text) if re.fullmatch(r"-?\d+", text) else None
    return None


def _licence(value: Any) -> Optional[int]:
    """A SIHF licence number / id as ``int``; ``0`` (the API's "nobody") -> ``None``."""
    n = _int(value)
    return n if n else None


def _clock_seconds(value: Any) -> Optional[int]:
    """``"65:56"`` -> ``3956`` (elapsed game seconds); ``None`` when malformed."""
    m = re.fullmatch(r"(\d+):(\d{2})", str(value or "").strip())
    return int(m.group(1)) * 60 + int(m.group(2)) if m else None


def _obj(value: Any) -> Mapping[str, Any]:
    """``value`` when it is an object, else ``{}``: a nested block of the wrong type reads as absent."""
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> List[Any]:
    """``value`` when it is a list, else ``[]`` (``x or []`` lets a string or a number through)."""
    return value if isinstance(value, list) else []


def _joined(values: Iterable[Any], sep: str) -> Optional[str]:
    """The present values joined by ``sep``; ``None`` when there are none (a missing name never becomes ``"None"``)."""
    text = [str(v) for v in values if v is not None and str(v).strip()]
    return sep.join(text) if text else None


def _coerce(value: Any, dtype: Any) -> Any:
    """``value`` as its column's dtype allows, else ``None``: a scalar of the wrong type (an object
    where a name belongs) must not make polars raise. A number in a String column is stringified."""
    if value is None:
        return None
    if isinstance(dtype, pl.List):
        return [_coerce(v, dtype.inner) for v in value] if isinstance(value, list) else None
    if dtype == pl.Boolean:
        return value if isinstance(value, bool) else None
    if isinstance(value, (bool, dict, list)):
        return None
    if dtype == pl.String:
        return value if isinstance(value, str) else str(value) if isinstance(value, (int, float)) else None
    if dtype == pl.Int64:
        return _int(value)
    if dtype == pl.Float64:
        return float(value) if isinstance(value, (int, float)) else None
    return value


def _blank(value: Any) -> Any:
    """``""`` and SIHF's ``"-"`` missing-value placeholder -> ``None``."""
    return None if isinstance(value, str) and value.strip() in ("", "-") else value


# ---------------------------------------------------------------------------
# tables
# ---------------------------------------------------------------------------


def _expand_cell(col: str, value: Any) -> Dict[str, Any]:
    """One table cell -> ``{column: value}``; a typed object becomes several columns."""
    if not isinstance(value, dict):
        return {col: _blank(value)}
    kind = value.get("type")
    if kind == "team":
        return {
            f"{col}_id": _licence(value.get("id")),
            f"{col}_name": value.get("name"),
            f"{col}_acronym": value.get("acronym"),
        }
    if kind == "result":
        return {f"{col}_home": _int(value.get("homeTeam")), f"{col}_away": _int(value.get("awayTeam"))}
    if kind == "periods":
        return {
            f"{col}_home": [_int(v) for v in _list(value.get("homeTeam"))],
            f"{col}_away": [_int(v) for v in _list(value.get("awayTeam"))],
        }
    if kind == "gameStatus":
        return {
            f"{col}_id": _int(value.get("id")),
            f"{col}_name": value.get("name"),
            f"{col}_percent": _int(value.get("percent")),
            f"{col}_canceled": value.get("canceled"),
            "start_date_time": value.get("startDateTime"),
            "end_date_time": value.get("endDateTime"),
            "update_date_time": value.get("updateDateTime"),
        }
    if kind == "linkToGameDetail":
        return {"game_id": _int(value.get("gameId"))}
    if kind == "broadcasts":
        return {col: _joined(_list(value.get("entries")), ",")}
    # An object type not seen when the parser was written: keep its name / id when it has them.
    return {f"{col}_id": _licence(value.get("id")), f"{col}_name": value.get("name")}


def _typed(df: pl.DataFrame, aliases: Mapping[str, str]) -> pl.DataFrame:
    """Cast the known numeric header columns; parse ``date`` (``dd.mm.yyyy``)."""
    exprs = []
    for alias, col in aliases.items():
        if col not in df.columns or df.schema[col] != pl.String:
            continue
        if alias in _INT_ALIASES:
            exprs.append(pl.col(col).str.replace_all(_THOUSANDS, "").cast(pl.Int64, strict=False))
        elif alias in _FLOAT_ALIASES:
            exprs.append(pl.col(col).str.replace_all(_THOUSANDS, "").cast(pl.Float64, strict=False))
        elif alias == "date":
            exprs.append(pl.col(col).str.to_date("%d.%m.%Y", strict=False))
    return df.with_columns(exprs) if exprs else df


def _echoed(raw: Any) -> Dict[str, Optional[int]]:
    """``season`` / ``phase_id`` from the body's ``filters[].selected`` echo; null when not echoed.

    The league is not among them: it travels in ``searchQuery``, which the body does not echo."""
    names = {"Season": "season", "Phase": "phase_id"}
    out: Dict[str, Optional[int]] = dict.fromkeys(names.values())
    for f in _list(raw.get("filters") if isinstance(raw, dict) else None):
        # a list / dict alias is unhashable: only a string can be one of the echoed filter names
        if isinstance(f, dict) and isinstance(f.get("alias"), str) and f["alias"] in names:
            out[names[f["alias"]]] = _int(f.get("selected"))
    return out


def _table_frame(raw: Any, extra: Optional[Mapping[str, Any]] = None) -> pl.DataFrame:
    """A SIHF table object -> polars frame (zero rows, no columns, when unusable)."""
    if not isinstance(raw, dict):
        return pl.DataFrame()
    header = [h for h in _list(raw.get("header")) if isinstance(h, dict)]
    data = raw.get("data") or []
    if not header or not isinstance(data, list):
        return pl.DataFrame()
    aliases = {str(h.get("alias")): _col(str(h.get("alias"))) for h in header}
    summary: Dict[int, str] = {}
    for hp in _list(raw.get("highlightedPositions")):
        if isinstance(hp, dict) and isinstance(hp.get("style"), str) and hp["style"] in _SUMMARY_STYLES:
            for r in _list(hp.get("rows")):
                if isinstance(r, int):
                    summary[r - 1] = str(hp["style"])  # rows are 1-based
    prefix = dict(extra or {})
    rows: List[Dict[str, Any]] = []
    for i, cells in enumerate(data):
        if not isinstance(cells, list):
            continue
        row: Dict[str, Any] = dict(prefix)
        for h, value in zip(header, cells):
            row.update(_expand_cell(aliases[str(h.get("alias"))], value))
        # A pseudo-team row is a summary row even when highlightedPositions does not tag it, and its
        # team id (100000) and acronym are not a team's: null them so they never join to one.
        team_name = row.get("team_name")
        name = team_name if isinstance(team_name, str) else ""
        pseudo = row.get("team_id") == _PSEUDO_TEAM_ID or name in _PSEUDO_TEAM_NAMES
        row["row_type"] = summary.get(i) or (_PSEUDO_TEAM_NAMES.get(name, "average") if pseudo else "data")
        if pseudo:
            row.update({c: None for c in ("team_id", "team_acronym") if c in row})
        # A cell of the wrong type (a list where a string belongs) becomes null; header string
        # cells stay strings, which _typed casts afterwards.
        rows.append({c: _coerce(v, _raw_cell_dtype(c)) for c, v in row.items()})
    if not rows:
        return pl.DataFrame()
    df = _typed(pl.from_dicts(rows, infer_schema_length=None), aliases)
    # Ids are Int64 join keys even when every row is null (inference would make them Null).
    ids = [c for c in df.columns if c == "id" or c.endswith("_id")]
    df = df.with_columns(pl.col(c).cast(pl.Int64, strict=False) for c in ids) if ids else df
    # A column that is blank in every row (or ``season`` when the body does not echo it) infers as
    # Null, and a periods column of empty lists as List(Null): give each its documented dtype.
    blank: Dict[str, Any] = {}
    for c, dtype in df.schema.items():
        doc = _table_dtype(c)
        if dtype == pl.Null or (dtype == pl.List(pl.Null) and isinstance(doc, pl.List)):
            blank[c] = doc
    return df.with_columns(pl.col(c).cast(t) for c, t in blank.items()) if blank else df


def parse_sihf_table(
    raw: Optional[Dict[str, Any]],
    *,
    return_as_pandas: bool = False,
    columns: Optional[Sequence[str]] = None,
) -> Frame:
    """Parse a SIHF ``/table`` body (results, standings, player or goalie stats) into a tidy frame.

    Each positional row is keyed by its ``header`` alias, snake_cased (``gamesPlayed`` ->
    ``games_played``). The columns therefore follow the header the league publishes: the
    documented ones are the National League's (the MyHockey League's standings header is the
    same), and a league that publishes an extra alias gets an extra column. Typed cells
    expand: a ``team`` cell to ``{col}_id`` / ``{col}_name`` / ``{col}_acronym``, a ``result`` to ``{col}_home`` / ``{col}_away``, ``periods`` to
    per-side ``List[Int64]`` columns, ``gameStatus`` to ``status_*`` plus the start / end /
    update timestamps, and ``linkToGameDetail`` to ``game_id``. Known count and rate columns
    are cast to ``Int64`` / ``Float64`` (the ``’`` thousands separator is dropped, ``""`` and
    ``"-"`` become null) and ``date`` to a ``Date``; ``"mm:ss"`` times stay strings, with
    ``"-"`` null. ``season`` / ``phase_id`` come from the body's own filter echo (``phase_id`` is
    null unless a phase was asked for; the body does not echo the league, so there is no
    league column), every ``*_id`` is ``Int64``, and ``row_type`` is ``"data"``, or
    ``"average"`` / ``"total"`` for the summary rows SIHF mixes into the team and goalie
    tables: the league-average row and the attendance table's ``Total`` row, both on the
    pseudo-team ``100000`` (``Durchschnitt`` / ``Total``). A summary row is recognised from
    ``highlightedPositions`` or, failing that, from its pseudo-team, whose ``team_id`` and
    ``team_acronym`` are null so it never joins to a real team. An empty ``rank`` alone is
    not a summary row: every goaltender below the qualification line has one.

    Args:
        raw (dict | None): a SIHF table JSON body.
        return_as_pandas (bool): return a pandas DataFrame instead of polars.
        columns (Sequence[str] | None): the table's documented column names (the generated wrappers pass their
            returns-schema). A table with no rows then parses to a zero-row frame with exactly
            these columns, in their documented dtypes.

    Returns:
        One row per table row. A zero-row frame when the payload is ``None``, empty or
        malformed -- with the ``columns`` schema when given -- so callers can chain without a
        null-check.

    Raises:
        None: malformed payloads yield a zero-row frame rather than an exception.

    Example:
        Quick start::

            from sportsdataverse.hockey.sihf import sihf_standings

            df = sihf_standings(season=2026, league_id=1)
            print(df.shape)

        Pipeline next step (one line)::

            df.select("rank", "team_name", "games_played", "points")

    See Also:
        * `SIHF`_ -- the Swiss Ice Hockey Federation site the API serves.

    .. _SIHF: https://www.sihf.ch/
    """
    df = _table_frame(raw, _echoed(raw))
    if df.height == 0 and columns is not None:
        df = pl.DataFrame(schema={c: _table_dtype(c) for c in columns})
    return _out(df, return_as_pandas)


_PHASES_SCHEMA: Dict[str, Any] = {
    "season": pl.Int64,
    "phase_id": pl.Int64,
    "phase_name": pl.String,
    "n_games": pl.Int64,
}
# ``Regular Season (364)``: a phase name may end with a count, a game count only in the results table.
_PHASE_COUNT = re.compile(r"^(.*?)\s*\((\d+)\)$")


def parse_sihf_phases(
    raw: Optional[Dict[str, Any]],
    *,
    return_as_pandas: bool = False,
    columns: Optional[Sequence[str]] = None,
) -> Frame:
    """Parse the ``Phase`` filter of a SIHF ``/table`` body into one row per phase.

    The body lists its phases in ``filters[alias=Phase].entries`` (``{alias: id, name}``) when
    the request filtered on ``Phase``. The ids depend on the league, the season and the table.
    The results table lists the playoff and play-in rounds, the regular season and any
    playout or league-qualification round, and suffixes each name with its game count
    (``Regular Season (364)``). That count becomes ``n_games``. It is null for the other
    tables and for a phase still in progress, which has no count. The standings table also
    suffixes a number (``Regular Season (3)``, in every league), but that number is not a
    game count, so it is stripped from the name and dropped. The player and goalie tables
    also list the ``Playoff Overall`` / ``Play-In Overall`` totals. The standings table
    lists only the phases played as a table (the regular season, in some seasons a playout
    round). The ``all`` pseudo-entry is dropped, because ``phase_id=None`` already means
    every phase.
    ``season`` comes from the body's Season echo. Rows keep SIHF's order.

    Args:
        raw (dict | None): a SIHF table JSON body requested with a ``Phase`` filter.
        return_as_pandas (bool): return a pandas DataFrame instead of polars.
        columns (Sequence[str] | None): accepted for the generated wrappers, which pass the documented column names;
            unused, because the phase frame always carries its fixed documented schema.

    Returns:
        Columns ``season``, ``phase_id`` (``Int64``), ``phase_name`` and ``n_games``. A
        zero-row frame with those columns when the payload is ``None``, empty or malformed,
        or carries no Phase filter.

    Raises:
        None: malformed payloads yield a zero-row frame rather than an exception.

    Example:
        Quick start::

            from sportsdataverse.hockey.sihf import sihf_phases

            df = sihf_phases(season=2026, league_id=1)
            print(df)

        Feed a phase id to a table wrapper::

            from sportsdataverse.hockey.sihf import sihf_results

            final = df.filter(df["phase_name"] == "Playoff Final")["phase_id"][0]
            sihf_results(season=2026, league_id=1, phase_id=final)

    See Also:
        * `SIHF`_ -- the Swiss Ice Hockey Federation site the API serves.

    .. _SIHF: https://www.sihf.ch/
    """
    season = _echoed(raw)["season"]
    # Only the results table (the one with home / away team columns) counts games per phase.
    counts_games = isinstance(raw, dict) and any(
        isinstance(h, dict) and h.get("alias") == "homeTeam" for h in _list(raw.get("header"))
    )
    rows: List[Dict[str, Any]] = []
    for f in _list(raw.get("filters") if isinstance(raw, dict) else None):
        if not (isinstance(f, dict) and f.get("alias") == "Phase"):
            continue
        for e in _list(f.get("entries")):
            phase_id = _int(e.get("alias")) if isinstance(e, dict) else None
            if phase_id is None:  # the "all" pseudo-entry
                continue
            # A name that is not a string (missing, a list, an object) is null, never "" or "['x']".
            raw_name = e.get("name")
            name = (raw_name.strip() or None) if isinstance(raw_name, str) else None
            m = _PHASE_COUNT.match(name) if name else None
            rows.append(
                {
                    "season": season,
                    "phase_id": phase_id,
                    "phase_name": m.group(1) if m else name,
                    "n_games": int(m.group(2)) if m and counts_games else None,
                }
            )
    return _out(_frame(rows, _PHASES_SCHEMA), return_as_pandas)


# ---------------------------------------------------------------------------
# the game page
# ---------------------------------------------------------------------------


def _game(raw: Any) -> Dict[str, Any]:
    return raw if isinstance(raw, dict) and raw.get("gameId") else {}


def _sides(game: Mapping[str, Any]) -> Dict[int, str]:
    """``{team_id: "home" | "away"}`` from ``details.homeTeam`` / ``awayTeam``."""
    details = _obj(game.get("details"))
    out: Dict[int, str] = {}
    for side in ("home", "away"):
        tid = _licence(_obj(details.get(f"{side}Team")).get("id"))
        if tid is not None:
            out[tid] = side
    return out


def _roster(game: Mapping[str, Any]) -> Dict[int, Mapping[str, Any]]:
    """``{licence: player}`` from the ``players`` list."""
    return {
        pid: p for p in _list(game.get("players")) if isinstance(p, dict) and (pid := _licence(p.get("id"))) is not None
    }


def _person(roster: Mapping[int, Mapping[str, Any]], licence: Any, prefix: str = "player") -> Dict[str, Any]:
    """``{prefix}_id`` / ``{prefix}_name`` (+ ``player_jersey`` for the player) for a licence number."""
    pid = _licence(licence)
    p = (roster.get(pid) if pid is not None else None) or {}
    out = {f"{prefix}_id": pid, f"{prefix}_name": p.get("fullName")}
    if prefix == "player":
        out["player_jersey"] = _int(p.get("jerseyNumber"))
    return out


def _stats_table(game: Mapping[str, Any], alias: str) -> Optional[Mapping[str, Any]]:
    """The embedded ``stats`` table with ``alias`` (``gameTeamStats``, ``gamePlayerStatsHome``, ...)."""
    return next((t for t in _list(game.get("stats")) if isinstance(t, dict) and t.get("alias") == alias), None)


def _frame(rows: Iterable[Mapping[str, Any]], schema: Mapping[str, Any]) -> pl.DataFrame:
    rows = [{c: _coerce(r.get(c), t) for c, t in schema.items()} for r in rows]
    if not rows:
        return pl.DataFrame(schema=dict(schema))
    return pl.from_dicts(rows, schema=dict(schema))


_PBP_SCHEMA: Dict[str, Any] = {
    "game_id": pl.Int64,
    "event_index": pl.Int64,
    "period_number": pl.Int64,
    "period": pl.String,
    "event_type": pl.String,
    "time": pl.String,
    "game_seconds": pl.Int64,
    "team_id": pl.Int64,
    "team_side": pl.String,
    "player_id": pl.Int64,
    "player_name": pl.String,
    "player_jersey": pl.Int64,
    "assist1_player_id": pl.Int64,
    "assist1_player_name": pl.String,
    "assist2_player_id": pl.Int64,
    "assist2_player_name": pl.String,
    "goalie_id": pl.Int64,
    "goalie_name": pl.String,
    "strength": pl.String,
    "goal_tags": pl.String,
    "score_home": pl.Int64,
    "score_away": pl.Int64,
    "plus_player_ids": pl.List(pl.Int64),
    "minus_player_ids": pl.List(pl.Int64),
    "penalty_id": pl.Int64,
    "penalty_name": pl.String,
    "penalty_minutes": pl.Int64,
    "penalty_start": pl.String,
    "penalty_end": pl.String,
    "goalie_change": pl.String,
    "revoke_reason": pl.String,
    "shootout_number": pl.Int64,
    "shootout_scored": pl.Boolean,
    "text": pl.String,
}
# ``**PP1,GWG** / **2:3** - **Scorer (7)**`` -> tags + running score (home:away).
_GOAL_HEAD = re.compile(r"^\*\*(?P<tags>[^*]*)\*\*\s*/\s*\*\*(?P<home>\d+):(?P<away>\d+)\*\*")
_FIRST_BOLD = re.compile(r"^\*\*(?P<reason>[^*]+)\*\*")
_GOALIE_CHANGE = re.compile(r"\((IN|OUT)\)\s*$")
# Same-clock tie-break: the order SIHF's own timeline prints events in.
_TYPE_ORDER = {"penalty": 0, "goal": 1, "revoked_goal": 2, "goalie": 3}


def parse_sihf_pbp(
    raw: Optional[Dict[str, Any]],
    *,
    return_as_pandas: bool = False,
    columns: Optional[Sequence[str]] = None,
) -> Frame:
    """Parse a SIHF ``gameoverview`` body into the game's event timeline, one row per event.

    ``event_type`` is ``goal``, ``penalty``, ``goalie`` (a goaltender in / out, see
    ``goalie_change``), ``revoked_goal`` (a disallowed goal, ``revoke_reason`` e.g.
    ``CC-Offiside``) or ``shootout`` (one row per attempt, after every period). Events are
    ordered by ``game_seconds`` (elapsed game time from ``time``, ``"mm:ss"``) and numbered
    by ``event_index``. Player names and jerseys are resolved from the game's ``players``
    roster by licence number.

    Goal rows carry ``goal_tags`` as SIHF prints them (``"PP1,GWG"``), ``strength`` (the
    first tag: ``EQ``, ``PP1``, ``SH1``, ...), the running ``score_home`` / ``score_away``
    after the goal, and the players credited plus / minus (``plus_player_ids`` /
    ``minus_player_ids``), which SIHF lists only where plus-minus applies (empty on a
    power-play goal). Shootout rows put the shooter in ``player_*`` and the goaltender in
    ``goalie_*``.

    Args:
        raw (dict | None): the ``gameoverview`` JSON body.
        return_as_pandas (bool): return a pandas DataFrame instead of polars.
        columns (Sequence[str] | None): accepted for the generated wrappers, which pass the documented column names;
            unused, because a game frame always carries its fixed documented schema.

    Returns:
        One row per event with the documented columns; a zero-row frame with the same
        columns for a game not yet played, an empty or a malformed payload.

    Raises:
        None: malformed payloads yield a zero-row frame rather than an exception.

    Example:
        Quick start::

            from sportsdataverse.hockey.sihf import sihf_pbp

            pbp = sihf_pbp(game_id=20261105000325)
            print(pbp.shape)

        Pipeline next step (one line)::

            import polars as pl

            pbp.filter(pl.col("event_type") == "goal").select("time", "team_side", "player_name", "strength")

    See Also:
        * `SIHF`_ -- the Swiss Ice Hockey Federation site the API serves.

    .. _SIHF: https://www.sihf.ch/
    """
    game = _game(raw)
    if not game:
        return _out(_frame([], _PBP_SCHEMA), return_as_pandas)
    gid, sides, roster = _int(game.get("gameId")), _sides(game), _roster(game)

    def who(prefix: str, licence: Any) -> Dict[str, Any]:
        return _person(roster, licence, prefix)

    events: List[Dict[str, Any]] = []
    periods = [p for p in _list(_obj(game.get("summary")).get("periods")) if isinstance(p, dict)]
    for number, period in enumerate(periods):
        for kind, key in (
            ("penalty", "fouls"),
            ("goal", "goals"),
            ("revoked_goal", "revokedGoals"),
            ("goalie", "goalkeepers"),
        ):
            for e in _list(period.get(key)):
                if not isinstance(e, dict):
                    continue
                tid = _licence(e.get("teamId"))
                raw_text = e.get("text")
                text = raw_text if isinstance(raw_text, str) else ""
                row: Dict[str, Any] = {
                    "period_number": number + 1,
                    "period": period.get("name"),
                    "event_type": kind,
                    "time": e.get("time"),
                    "team_id": tid,
                    "team_side": sides.get(tid) if tid is not None else None,
                    "text": text,
                    "_order": (_TYPE_ORDER[kind], _int(e.get("sortOrder")) or 0),
                }
                if kind in ("goal", "revoked_goal"):
                    row |= who("player", e.get("scorerLicenceNr"))
                    row |= who("assist1_player", e.get("assist1LicenceNr"))
                    row |= who("assist2_player", e.get("assist2LicenceNr"))
                if kind == "goal":
                    m = _GOAL_HEAD.match(text)
                    if m:
                        tags = m.group("tags").strip()
                        row |= {
                            "goal_tags": tags or None,
                            "strength": tags.split(",")[0].strip() or None,
                            "score_home": int(m.group("home")),
                            "score_away": int(m.group("away")),
                        }
                    row["plus_player_ids"] = [n for i in range(1, 7) if (n := _licence(e.get(f"p{i}LicenceNr")))]
                    row["minus_player_ids"] = [n for i in range(1, 7) if (n := _licence(e.get(f"m{i}LicenceNr")))]
                elif kind == "revoked_goal":
                    m = _FIRST_BOLD.match(text)
                    row["revoke_reason"] = m.group("reason").strip() if m else None
                elif kind == "penalty":
                    row |= who("player", e.get("playerLicenceNr"))
                    row |= {
                        "penalty_id": _int(e.get("id")),
                        "penalty_name": e.get("name"),
                        "penalty_minutes": _int(e.get("minutes")),
                        "penalty_start": e.get("startTime"),
                        "penalty_end": e.get("endTime"),
                    }
                else:  # goalie
                    row |= who("player", e.get("playerLicenceNr"))
                    m = _GOALIE_CHANGE.search(text)
                    row["goalie_change"] = m.group(1).lower() if m else None
                events.append(row)
    events.sort(key=lambda r: (_clock_seconds(r["time"]) is None, _clock_seconds(r["time"]) or 0, r["_order"]))
    shootout = _obj(_obj(game.get("summary")).get("shootout"))
    for s in _list(shootout.get("shoots")):
        if not isinstance(s, dict):
            continue
        shooter = who("player", s.get("scorerLicenceNr"))
        tid = _licence((roster.get(shooter["player_id"]) or {}).get("teamId")) if shooter["player_id"] else None
        events.append(
            {
                "period_number": len(periods) + 1,
                "period": shootout.get("name"),
                "event_type": "shootout",
                "team_id": tid,
                "team_side": sides.get(tid) if tid is not None else None,
                "shootout_number": _int(s.get("number")),
                "shootout_scored": s.get("scored") if isinstance(s.get("scored"), bool) else None,
                "text": s.get("text"),
                **shooter,
                # ``goalkeeperLiceneNr`` (sic) is the API's spelling.
                **who("goalie", s.get("goalkeeperLiceneNr", s.get("goalkeeperLicenceNr"))),
            }
        )
    rows = []
    for i, e in enumerate(events):
        e.pop("_order", None)
        rows.append({"game_id": gid, "event_index": i + 1, "game_seconds": _clock_seconds(e.get("time")), **e})
    return _out(_frame(rows, _PBP_SCHEMA), return_as_pandas)


def _side_tables(game: Mapping[str, Any], alias: str) -> pl.DataFrame:
    """The per-side ``stats`` tables ``{alias}Home`` + ``{alias}Away``, stacked with side / team / player ids."""
    details = _obj(game.get("details"))
    # (team, jersey) -> licence; a jersey two roster players share maps to None rather than a guess.
    by_jersey: Dict[tuple, Optional[int]] = {}
    for pid, p in _roster(game).items():
        tid, jersey = _licence(p.get("teamId")), _int(p.get("jerseyNumber"))
        if tid is not None and jersey is not None:
            by_jersey[(tid, jersey)] = None if (tid, jersey) in by_jersey else pid
    frames = []
    for side in ("home", "away"):
        table = _stats_table(game, f"{alias}{side.title()}")
        team = _obj(details.get(f"{side}Team"))
        tid = _licence(team.get("id"))
        df = _table_frame(
            table,
            {"game_id": _int(game.get("gameId")), "team_side": side, "team_id": tid, "team_name": team.get("name")},
        )
        if df.height == 0:
            continue
        if "player_number" in df.columns:
            ids = [by_jersey.get((tid, n)) if n is not None else None for n in df["player_number"].to_list()]
            df = df.with_columns(pl.Series("player_id", ids, dtype=pl.Int64))
        frames.append(df)
    return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()


_GAME_PLAYER_SCHEMA: Dict[str, Any] = {
    "game_id": pl.Int64,
    "team_side": pl.String,
    "team_id": pl.Int64,
    "team_name": pl.String,
    "player_number": pl.Int64,
    "player": pl.String,
    "player_position": pl.String,
    "goals": pl.Int64,
    "assists": pl.Int64,
    "assists1": pl.Int64,
    "assists2": pl.Int64,
    "points": pl.Int64,
    "pim_total": pl.Int64,
    "plus_minus": pl.Int64,
    "shots_on_goal": pl.Int64,
    "sog_missed": pl.Int64,
    "sog_post": pl.Int64,
    "blockings": pl.Int64,
    "faceoffs_won": pl.Int64,
    "faceoffs_lost": pl.Int64,
    "faceoffs_percent": pl.Float64,
    "time_on_ice": pl.String,
    "time_on_ice_pp": pl.String,
    "time_on_ice_pk": pl.String,
    "row_type": pl.String,
    "player_id": pl.Int64,
}
_GAME_GOALIE_SCHEMA: Dict[str, Any] = {
    "game_id": pl.Int64,
    "team_side": pl.String,
    "team_id": pl.Int64,
    "team_name": pl.String,
    "player_number": pl.Int64,
    "player": pl.String,
    "goals_against": pl.Int64,
    "saves": pl.Int64,
    "shots_against": pl.Int64,
    "saves_percentage": pl.Float64,
    "seconds_played": pl.String,
    "goals_against_average": pl.Float64,
    "penalty_in_minutes": pl.Int64,
    "row_type": pl.String,
    "player_id": pl.Int64,
}


def _conform(df: pl.DataFrame, schema: Mapping[str, Any]) -> pl.DataFrame:
    """``df`` in the documented column order and dtypes; unexpected extra columns are kept at the end."""
    if df.height == 0:
        return pl.DataFrame(schema=dict(schema))
    exprs = [
        (pl.col(c).cast(t, strict=False) if c in df.columns else pl.lit(None, dtype=t)).alias(c)
        for c, t in schema.items()
    ]
    return df.select(exprs + [pl.col(c) for c in df.columns if c not in schema])


def parse_sihf_game_player_stats(
    raw: Optional[Dict[str, Any]],
    *,
    return_as_pandas: bool = False,
    columns: Optional[Sequence[str]] = None,
) -> Frame:
    """Parse a SIHF ``gameoverview`` body into the skater box score, one row per player per side.

    Stacks the page's ``gamePlayerStatsHome`` and ``gamePlayerStatsAway`` tables, tagged with
    ``team_side`` / ``team_id`` / ``team_name``. ``player_id`` (the SIHF licence number) is
    resolved from the roster by team and jersey. Each side ends with its ``row_type =
    "total"`` row (no player). Time-on-ice columns stay ``"mm:ss"`` strings;
    ``faceoffs_percent`` is null (``"-"`` upstream) for a player with no faceoffs.

    Args:
        raw (dict | None): the ``gameoverview`` JSON body.
        return_as_pandas (bool): return a pandas DataFrame instead of polars.
        columns (Sequence[str] | None): accepted for the generated wrappers, which pass the documented column names;
            unused, because a game frame always carries its fixed documented schema.

    Returns:
        One row per skater plus one total row per side; a zero-row frame with the documented
        columns for a game not yet played, an empty or a malformed payload.

    Raises:
        None: malformed payloads yield a zero-row frame rather than an exception.

    Example:
        Quick start::

            from sportsdataverse.hockey.sihf import sihf_game_player_stats

            box = sihf_game_player_stats(game_id=20261105000325)
            print(box.shape)

        Pipeline next step (one line)::

            import polars as pl

            box.filter(pl.col("row_type") == "data").sort("points", descending=True).head()

    See Also:
        * `SIHF`_ -- the Swiss Ice Hockey Federation site the API serves.

    .. _SIHF: https://www.sihf.ch/
    """
    return _out(_conform(_side_tables(_game(raw), "gamePlayerStats"), _GAME_PLAYER_SCHEMA), return_as_pandas)


def parse_sihf_game_goalie_stats(
    raw: Optional[Dict[str, Any]],
    *,
    return_as_pandas: bool = False,
    columns: Optional[Sequence[str]] = None,
) -> Frame:
    """Parse a SIHF ``gameoverview`` body into the goaltender box score, one row per goalie per side.

    Stacks ``gameGoalieStatsHome`` and ``gameGoalieStatsAway`` with ``team_side`` /
    ``team_id`` / ``team_name`` and the roster-resolved ``player_id``. ``seconds_played`` is,
    despite its upstream name, ice time as ``"mm:ss"``; a goalie who did not play has null
    rates.

    Args:
        raw (dict | None): the ``gameoverview`` JSON body.
        return_as_pandas (bool): return a pandas DataFrame instead of polars.
        columns (Sequence[str] | None): accepted for the generated wrappers, which pass the documented column names;
            unused, because a game frame always carries its fixed documented schema.

    Returns:
        One row per dressed goaltender; a zero-row frame with the documented columns for a
        game not yet played, an empty or a malformed payload.

    Raises:
        None: malformed payloads yield a zero-row frame rather than an exception.

    Example:
        Quick start::

            from sportsdataverse.hockey.sihf import sihf_game_goalie_stats

            goalies = sihf_game_goalie_stats(game_id=20261105000325)
            print(goalies.shape)

        Pipeline next step (one line)::

            goalies.select("team_side", "player", "saves", "shots_against", "saves_percentage")

    See Also:
        * `SIHF`_ -- the Swiss Ice Hockey Federation site the API serves.

    .. _SIHF: https://www.sihf.ch/
    """
    return _out(_conform(_side_tables(_game(raw), "gameGoalieStats"), _GAME_GOALIE_SCHEMA), return_as_pandas)


_TEAM_STATS_SCHEMA: Dict[str, Any] = {
    "game_id": pl.Int64,
    "statistic": pl.String,
    "home_value": pl.String,
    "away_value": pl.String,
}


def parse_sihf_game_team_stats(
    raw: Optional[Dict[str, Any]],
    *,
    return_as_pandas: bool = False,
    columns: Optional[Sequence[str]] = None,
) -> Frame:
    """Parse a SIHF ``gameoverview`` body into the team stats, one row per statistic.

    The page's ``gameTeamStats`` table is one row per statistic with one column per team
    (headed by the team names). It becomes ``statistic`` / ``home_value`` / ``away_value``
    in SIHF's own order and labels (``SOG Total``, ``SOG 1P``, ``FO%``, ``PPT``, ``PK%``,
    ...). Values stay strings because the column mixes counts, percentages and ``"mm:ss"``
    times (``PPT`` / ``PKT``).

    Args:
        raw (dict | None): the ``gameoverview`` JSON body.
        return_as_pandas (bool): return a pandas DataFrame instead of polars.
        columns (Sequence[str] | None): accepted for the generated wrappers, which pass the documented column names;
            unused, because a game frame always carries its fixed documented schema.

    Returns:
        One row per statistic; a zero-row frame with the documented columns for a game not
        yet played, an empty or a malformed payload.

    Raises:
        None: malformed payloads yield a zero-row frame rather than an exception.

    Example:
        Quick start::

            from sportsdataverse.hockey.sihf import sihf_game_team_stats

            stats = sihf_game_team_stats(game_id=20261105000325)
            print(stats.shape)

        Pipeline next step (one line)::

            import polars as pl

            stats.filter(pl.col("statistic").str.starts_with("SOG"))

    See Also:
        * `SIHF`_ -- the Swiss Ice Hockey Federation site the API serves.

    .. _SIHF: https://www.sihf.ch/
    """
    game = _game(raw)
    table = _stats_table(game, "gameTeamStats")
    rows = []
    for cells in _list(_obj(table).get("data")):
        if isinstance(cells, list) and len(cells) >= 3:
            rows.append(
                {
                    "game_id": _int(game.get("gameId")),
                    "statistic": cells[0],
                    "home_value": _blank(cells[1]),
                    "away_value": _blank(cells[2]),
                }
            )
    return _out(_frame(rows, _TEAM_STATS_SCHEMA), return_as_pandas)


_INFO_SCHEMA: Dict[str, Any] = {
    "game_id": pl.Int64,
    "season": pl.Int64,
    "round": pl.Int64,
    "league_id": pl.Int64,
    "league_name": pl.String,
    "region_id": pl.Int64,
    "region_name": pl.String,
    "group_id": pl.Int64,
    "group_name": pl.String,
    "tournament_id": pl.Int64,
    "tournament_name": pl.String,
    "qualification_id": pl.Int64,
    "qualification_name": pl.String,
    "phase_id": pl.Int64,
    "phase_name": pl.String,
    "start_date_time": pl.String,
    "end_date_time": pl.String,
    "update_date_time": pl.String,
    "status_id": pl.Int64,
    "status_name": pl.String,
    "status_percent": pl.Int64,
    "status_canceled": pl.Boolean,
    "home_team_id": pl.Int64,
    "home_team_name": pl.String,
    "home_team_acronym": pl.String,
    "away_team_id": pl.Int64,
    "away_team_name": pl.String,
    "away_team_acronym": pl.String,
    "home_score": pl.Int64,
    "away_score": pl.Int64,
    "home_score_by_period": pl.List(pl.Int64),
    "away_score_by_period": pl.List(pl.Int64),
    "home_shots_by_period": pl.List(pl.Int64),
    "away_shots_by_period": pl.List(pl.Int64),
    "period_indicators": pl.List(pl.String),
    "venue_name": pl.String,
    "venue_city": pl.String,
    "spectators": pl.Int64,
    "referees": pl.String,
    "linesmen": pl.String,
    "broadcasts": pl.String,
}


def parse_sihf_game_info(
    raw: Optional[Dict[str, Any]],
    *,
    return_as_pandas: bool = False,
    columns: Optional[Sequence[str]] = None,
) -> Frame:
    """Parse a SIHF ``gameoverview`` body into the game header, one row.

    League, region, group, tournament, qualification (``Regular Season``, ``Playoff
    Final``, ...) and phase as id + name pairs; the start / end / update timestamps
    (ISO-8601 with offset, as strings); the status (``status_percent`` is 100 once the
    game is over); both teams; the final score; per-period goals and shots on goal as
    per-side lists aligned with ``period_indicators`` (``"1"``, ``"2"``, ``"3"``, ``"OT1"``);
    venue and ``spectators``; and the officials and broadcasters as comma-joined names.

    Args:
        raw (dict | None): the ``gameoverview`` JSON body.
        return_as_pandas (bool): return a pandas DataFrame instead of polars.
        columns (Sequence[str] | None): accepted for the generated wrappers, which pass the documented column names;
            unused, because a game frame always carries its fixed documented schema.

    Returns:
        One row; a zero-row frame with the documented columns for an empty or a malformed
        payload.

    Raises:
        None: malformed payloads yield a zero-row frame rather than an exception.

    Example:
        Quick start::

            from sportsdataverse.hockey.sihf import sihf_game_info

            info = sihf_game_info(game_id=20261105000325)
            print(info.shape)

        Pipeline next step (one line)::

            info.select("home_team_name", "away_team_name", "home_score", "away_score", "spectators")

    See Also:
        * `SIHF`_ -- the Swiss Ice Hockey Federation site the API serves.

    .. _SIHF: https://www.sihf.ch/
    """
    game = _game(raw)
    if not game:
        return _out(_frame([], _INFO_SCHEMA), return_as_pandas)
    details = _obj(game.get("details"))
    result = _obj(game.get("result"))
    status = _obj(game.get("status"))
    venue = _obj(details.get("venue"))
    row: Dict[str, Any] = {
        "game_id": _int(game.get("gameId")),
        "season": _int(game.get("season")),
        "round": _int(game.get("round")),
    }
    for key in ("league", "region", "group", "tournament", "qualification", "phase"):
        block = _obj(game.get(key))
        row[f"{key}_id"] = _int(block.get("id"))
        row[f"{key}_name"] = block.get("name")
    row |= {
        "start_date_time": game.get("startDateTime"),
        "end_date_time": game.get("endDateTime"),
        "update_date_time": game.get("updateDateTime"),
        "status_id": _int(status.get("id")),
        "status_name": status.get("name"),
        "status_percent": _int(status.get("percent")),
        "status_canceled": status.get("canceled") if isinstance(status.get("canceled"), bool) else None,
    }
    for side in ("home", "away"):
        team = _obj(details.get(f"{side}Team"))
        row |= {
            f"{side}_team_id": _licence(team.get("id")),
            f"{side}_team_name": team.get("name"),
            f"{side}_team_acronym": team.get("acronym"),
            f"{side}_score": _int(result.get(f"{side}Team")),
            f"{side}_score_by_period": [
                _int(s.get(f"{side}Team")) for s in _list(result.get("scores")) if isinstance(s, dict)
            ],
            f"{side}_shots_by_period": [
                _int(s.get(f"{side}Team")) for s in _list(result.get("sogs")) if isinstance(s, dict)
            ],
        }
    # aligned with the per-period lists, so a missing indicator stays a null element (never "None")
    row["period_indicators"] = [
        None if s.get("indicator") is None else str(s.get("indicator"))
        for s in _list(result.get("scores"))
        if isinstance(s, dict)
    ]
    row |= {
        "venue_name": venue.get("name"),
        "venue_city": venue.get("city"),
        "spectators": _int(venue.get("spectators")),
        "referees": _joined([_obj(o).get("fullName") for o in _list(details.get("referees"))], ", "),
        "linesmen": _joined([_obj(o).get("fullName") for o in _list(details.get("linesmen"))], ", "),
        "broadcasts": _joined(_list(_obj(game.get("broadcasts")).get("entries")), ","),
    }
    return _out(_frame([row], _INFO_SCHEMA), return_as_pandas)


_LINEUP_SCHEMA: Dict[str, Any] = {
    "game_id": pl.Int64,
    "team_side": pl.String,
    "team_id": pl.Int64,
    "unit": pl.String,
    "slot": pl.Int64,
    "player_id": pl.Int64,
    "player_name": pl.String,
    "player_jersey": pl.Int64,
    "is_captain": pl.Boolean,
    "is_first_goalkeeper": pl.Boolean,
    "absence_code": pl.String,
    "absence_reason": pl.String,
}
# lineUps keys -> unit label, in the order the page lists them.
_UNITS = (
    (("goalkeepers",), "goalkeeper"),
    (("defenders", "left"), "defense_left"),
    (("defenders", "right"), "defense_right"),
    (("forwarders", "left"), "forward_left"),
    (("forwarders", "center"), "forward_center"),
    (("forwarders", "right"), "forward_right"),
    (("otherPlayers",), "other"),
)


def parse_sihf_game_lineups(
    raw: Optional[Dict[str, Any]],
    *,
    return_as_pandas: bool = False,
    columns: Optional[Sequence[str]] = None,
) -> Frame:
    """Parse a SIHF ``gameoverview`` body into both lineups and absentees, one row per player.

    Dressed players come from ``lineUps``, one row per player per ``unit``
    (``goalkeeper``, ``defense_left`` / ``defense_right``, ``forward_left`` /
    ``forward_center`` / ``forward_right``, ``other``), with ``slot`` the 1-based position
    in SIHF's listing of that unit. ``is_captain`` / ``is_first_goalkeeper`` come from the
    lineup's tags. Players listed in ``details.*Absentees`` follow with a null ``unit`` and
    ``absence_code`` / ``absence_reason`` as SIHF names them (``SU`` / ``Überzählig``,
    ``IN`` / ``Verletzt``, ``AB`` / ``Abwesend``). Names and jerseys are resolved from the
    roster.

    Args:
        raw (dict | None): the ``gameoverview`` JSON body.
        return_as_pandas (bool): return a pandas DataFrame instead of polars.
        columns (Sequence[str] | None): accepted for the generated wrappers, which pass the documented column names;
            unused, because a game frame always carries its fixed documented schema.

    Returns:
        One row per listed player; a zero-row frame with the documented columns for a game
        whose lineups are not out yet, an empty or a malformed payload.

    Raises:
        None: malformed payloads yield a zero-row frame rather than an exception.

    Example:
        Quick start::

            from sportsdataverse.hockey.sihf import sihf_game_lineups

            lineups = sihf_game_lineups(game_id=20261105000325)
            print(lineups.shape)

        Pipeline next step (one line)::

            import polars as pl

            lineups.filter(pl.col("unit").is_not_null()).group_by("team_side", "unit").len()

    See Also:
        * `SIHF`_ -- the Swiss Ice Hockey Federation site the API serves.

    .. _SIHF: https://www.sihf.ch/
    """
    game = _game(raw)
    gid, roster = _int(game.get("gameId")), _roster(game)
    details = _obj(game.get("details"))
    lineups = _obj(game.get("lineUps"))
    rows: List[Dict[str, Any]] = []

    for side in ("home", "away"):
        tid = _licence(_obj(details.get(f"{side}Team")).get("id"))
        team = lineups.get(f"{side}Team") if isinstance(lineups, dict) else None
        if isinstance(team, dict):
            tags = [t for t in _list(team.get("taggedPlayers")) if isinstance(t, dict)]
            captains = {_licence(t.get("id")) for t in tags if t.get("type") == "captain"}
            first_goalies = {_licence(t.get("id")) for t in tags if t.get("type") == "goalkeeper"}
            for path, unit in _UNITS:
                node: Any = team
                for key in path:
                    node = node.get(key) if isinstance(node, dict) else None
                for slot, licence in enumerate(node if isinstance(node, list) else []):
                    who = _person(roster, licence)
                    if who["player_id"] is None:
                        continue
                    rows.append(
                        {
                            "game_id": gid,
                            "team_side": side,
                            "team_id": tid,
                            "unit": unit,
                            "slot": slot + 1,
                            **who,
                            "is_captain": who["player_id"] in captains,
                            "is_first_goalkeeper": who["player_id"] in first_goalies,
                        }
                    )
        for group in _list(details.get(f"{side}Absentees")):
            if not isinstance(group, dict):
                continue
            for licence in _list(group.get("absentees")):
                who = _person(roster, licence)
                if who["player_id"] is None:
                    continue
                rows.append(
                    {
                        "game_id": gid,
                        "team_side": side,
                        "team_id": tid,
                        **who,
                        "absence_code": group.get("acronym"),
                        "absence_reason": group.get("name"),
                    }
                )
    return _out(_frame(rows, _LINEUP_SCHEMA), return_as_pandas)
