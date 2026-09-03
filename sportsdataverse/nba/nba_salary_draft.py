"""NBA salary, mock-draft and injury scrapes ported from hoopR.

Four third-party HTML/JSON sources with no official API, ported one-for-one from
the hoopR wrappers ``spotrac_team_cap()``, ``hoopshype_salaries()``,
``nbadraft_mock_draft()`` and ``rotowire_injuries()``:

===================== ============================================== ==================================
Function              Source                                         Shape
===================== ============================================== ==================================
:func:`spotrac_team_cap`   ``spotrac.com/nba/cap/_/year/{season}/``  static HTML table, one row per team
:func:`hoopshype_salaries` ``hoopshype.com/salaries/{team}/`` (x30)  ``__NEXT_DATA__`` JSON, one row per player-season
:func:`nbadraft_mock_draft` ``nbadraft.net/nba-mock-drafts/``        static HTML tables, one row per pick
:func:`rotowire_injuries`  ``rotowire.com/basketball/tables/injury-report.php`` JSON, one row per injured player
===================== ============================================== ==================================

Documented gotchas (see ``sdv-internal-refs/nba-salary-draft-sources/README.md``):

* **Spotrac** ships a ``<noscript>enable JavaScript</noscript>`` fallback that
  reads as a JS challenge but is a false positive -- the team cap table is in the
  static HTML. The team cell duplicates the abbreviation (``"ORL ORL"``); only the
  first token is kept. Dollar figures are ``$``-formatted strings, parsed to
  numeric. The player-level ``/nba/rankings/`` page IS genuinely JS-rendered and is
  deliberately NOT wrapped.
* **HoopsHype** is a Next.js app whose ``/salaries/players/`` page paginates
  client-side (~20 static rows), but each of the 30 team pages embeds that team's
  full roster in ``<script id="__NEXT_DATA__">``. :func:`hoopshype_salaries` walks
  :data:`HOOPSHYPE_TEAMS` and parses that JSON -- ~30 requests per call, paced by
  ``SDV_PY_HOOPSHYPE_DELAY``.
* **NBADraft.net** serves round 1 and round 2 as the first two pick tables; a
  third table repeats round 1, so only the first two are taken. The team cell
  carries ``*`` for a traded pick, which is stripped.
* **RotoWire** is undocumented in the internal refs. The rendered injuries grid is
  client-side; the wrapper reads the JSON table endpoint the grid itself calls.
  The projected return date is subscriber-gated and comes back as
  ``"Subscribers Only"``, mapped to null.

.. warning::
   **This port has NOT been verified against live pages.** It was written with no
   network access: every URL, query parameter, JSON key and column name is
   transcribed from the hoopR R wrappers and the internal-refs README, and the
   tests run against inline fixtures, not captured HTML. A first live run should
   verify, in this order: (1) Spotrac's actual column labels after
   ``html_tables`` header cleaning -- the ``rank`` / ``record`` /
   ``players_active`` / ``avg_age_team`` / ``total_cap_allocations`` /
   ``cap_space_all`` set below is hoopR's documented set, not an observed one;
   (2) that NBADraft's pick tables still carry a ``player`` column after cleaning
   and that the ``#`` header maps to ``pick``; (3) that HoopsHype's
   ``dehydratedState`` still nests contracts at ``state.data.contracts.contracts``;
   (4) RotoWire's JSON field names (``ID`` / ``firstname`` / ``rDate`` / ``URL``).
"""

from __future__ import annotations

import os
import re
import time
import warnings
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import polars as pl

from sportsdataverse._html_tables import html_tables
from sportsdataverse.dl_utils import download

__all__ = [
    "HOOPSHYPE_TEAMS",
    "hoopshype_salaries",
    "nbadraft_mock_draft",
    "rotowire_injuries",
    "spotrac_team_cap",
]

#: Browser User-Agent. All four hosts require one (internal-refs "General gotchas" #2).
_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

#: The 30 HoopsHype team slugs. The single list that drives the fan-out -- one
#: request per entry, in this order.
HOOPSHYPE_TEAMS = (
    "atlanta_hawks",
    "boston_celtics",
    "brooklyn_nets",
    "charlotte_hornets",
    "chicago_bulls",
    "cleveland_cavaliers",
    "dallas_mavericks",
    "denver_nuggets",
    "detroit_pistons",
    "golden_state_warriors",
    "houston_rockets",
    "indiana_pacers",
    "los_angeles_clippers",
    "los_angeles_lakers",
    "memphis_grizzlies",
    "miami_heat",
    "milwaukee_bucks",
    "minnesota_timberwolves",
    "new_orleans_pelicans",
    "new_york_knicks",
    "oklahoma_city_thunder",
    "orlando_magic",
    "philadelphia_76ers",
    "phoenix_suns",
    "portland_trail_blazers",
    "sacramento_kings",
    "san_antonio_spurs",
    "toronto_raptors",
    "utah_jazz",
    "washington_wizards",
)

# Empty-frame schemas. A zero-row return still carries the documented columns so
# callers see a stable set (transcribed from the hoopR @return tables).
_SPOTRAC_SCHEMA: Dict[str, Any] = {
    "rank": pl.Int64,
    "team": pl.Utf8,
    "record": pl.Utf8,
    "players_active": pl.Int64,
    "avg_age_team": pl.Float64,
    "total_cap_allocations": pl.Float64,
    "cap_space_all": pl.Float64,
    "season": pl.Int64,
}

_HOOPSHYPE_SCHEMA: Dict[str, Any] = {
    "player_id": pl.Utf8,
    "player": pl.Utf8,
    "first_name": pl.Utf8,
    "last_name": pl.Utf8,
    "team_id": pl.Utf8,
    "team": pl.Utf8,
    "season": pl.Int64,
    "salary": pl.Float64,
    "cap_allocation": pl.Float64,
    "team_option": pl.Boolean,
    "player_option": pl.Boolean,
    "two_way": pl.Boolean,
    "qualifying_offer": pl.Boolean,
}

_NBADRAFT_SCHEMA: Dict[str, Any] = {
    "round": pl.Int64,
    "pick": pl.Int64,
    "team": pl.Utf8,
    "player": pl.Utf8,
    "height": pl.Utf8,
    "weight": pl.Utf8,
    "position": pl.Utf8,
    "school": pl.Utf8,
    "class": pl.Utf8,
}

_ROTOWIRE_SCHEMA: Dict[str, Any] = {
    "player_id": pl.Utf8,
    "player": pl.Utf8,
    "first_name": pl.Utf8,
    "last_name": pl.Utf8,
    "team": pl.Utf8,
    "position": pl.Utf8,
    "injury": pl.Utf8,
    "status": pl.Utf8,
    "return_date": pl.Utf8,
    "url": pl.Utf8,
}

#: NBADraft's terse headers -> the documented names. ``#`` cleans to ``number``
#: (:mod:`sportsdataverse._html_tables` spells symbol-only headers out), and the
#: page has also been seen using ``No``.
_NBADRAFT_RENAME = {
    "number": "pick",
    "no": "pick",
    "h": "height",
    "w": "weight",
    "p": "position",
    "c": "class",
}

_TAG = re.compile(r"<[^>]*>")


def _out(frame: pl.DataFrame, return_as_pandas: bool) -> Union[pl.DataFrame, pd.DataFrame]:
    """Return ``frame`` as pandas when asked, else unchanged."""
    return frame.to_pandas() if return_as_pandas else frame


def _empty(schema: Dict[str, Any], return_as_pandas: bool) -> Union[pl.DataFrame, pd.DataFrame]:
    """Zero-row frame carrying ``schema``'s documented columns."""
    return _out(pl.DataFrame(schema=schema), return_as_pandas)


def _fetch(url: str, *, params: Optional[Dict[str, Any]] = None, proxy: Any = None) -> Optional[Any]:
    """GET ``url`` through the shared gateway, warning instead of raising.

    Mirrors the R wrappers' ``tryCatch`` posture: a dead host or a 404 yields an
    empty frame, not a traceback.
    """
    try:
        return download(url=url, params=params, headers={"User-Agent": _USER_AGENT}, proxy=proxy)
    except Exception as exc:  # noqa: BLE001 - transport errors vary by backend
        warnings.warn(f"{url} could not be fetched ({type(exc).__name__}: {exc}).", UserWarning, stacklevel=3)
        return None


def _env_float(name: str, default: float) -> float:
    """Read a non-negative float override from the environment, else ``default``."""
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except (TypeError, ValueError):
        warnings.warn(f"{name}={raw!r} is not numeric; using {default}.", UserWarning, stacklevel=2)
        return default
    return value if value >= 0 else default


def _currency_to_number(frame: pl.DataFrame) -> pl.DataFrame:
    """Cast every ``$``-formatted string column to ``Float64``.

    ``"$59,606,817"`` -> ``59606817.0``. Detection is by content, not by column
    name, so a renamed Spotrac column still parses.
    """
    exprs = []
    for name in frame.columns:
        if frame.schema[name] != pl.Utf8:
            continue
        if not bool(frame[name].str.contains(r"\$").fill_null(False).any()):
            continue
        exprs.append(pl.col(name).str.replace_all(r"[^0-9.-]", "").cast(pl.Float64, strict=False).alias(name))
    return frame.with_columns(exprs) if exprs else frame


def _id_str(value: Any) -> Optional[str]:
    """Stringify an id without the float->str ``"123.0"`` trap."""
    if value is None:
        return None
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return str(int(value)) if value.is_integer() else str(value)
    text = str(value).strip()
    return text or None


def _num(value: Any) -> Optional[float]:
    """Coerce a JSON scalar to ``float``, or ``None`` when it is not numeric."""
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int(value: Any) -> Optional[int]:
    """Coerce a JSON scalar to ``int``, or ``None`` when it is not numeric."""
    number = _num(value)
    return None if number is None else int(number)


def _flag(value: Any) -> Optional[bool]:
    """Coerce a JSON scalar to ``bool``, preserving a missing value as ``None``."""
    return None if value is None else bool(value)


def _text(value: Any) -> Optional[str]:
    """Trim a JSON string to ``None`` when blank."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def spotrac_team_cap(
    season: Optional[int] = None,
    *,
    proxy: Any = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Team salary-cap allocations from Spotrac.

    One row per team: cap allocations, cap space, active-player count and average
    roster age for a season. No API key required.

    The page carries a ``<noscript>`` fallback that looks like a JS challenge but
    is not -- the cap table is in the static HTML. The team cell duplicates the
    abbreviation (``"ORL ORL"``), so only the first token is kept, and every
    ``$``-formatted column is parsed to ``Float64``.

    Args:
        season: Season in 4-digit ENDING-year form (``2024`` = the 2023-24
            season). Defaults to
            :func:`~sportsdataverse.nba.nba_schedule.most_recent_nba_season`.
        proxy: Proxy configuration forwarded to
            :func:`~sportsdataverse.dl_utils.download` (``requests``
            ``proxies=`` shape).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per team. Columns follow Spotrac's table -- ``rank``, ``team``,
        ``record``, ``players_active``, ``avg_age_team``,
        ``total_cap_allocations``, ``cap_space_all`` -- plus ``season``. An
        unreachable or table-less page yields a zero-row frame with that schema.

    Raises:
        UserWarning: Emitted (not raised) when the page cannot be fetched or
            carries no table; the call still returns an empty frame.

    Example:
        Quick start::

            from sportsdataverse.nba import spotrac_team_cap

            cap = spotrac_team_cap(season=2024)
            print(cap.shape)

        As pandas::

            cap_pd = spotrac_team_cap(season=2024, return_as_pandas=True)

        Pipeline next step (most cap space)::

            cap.sort("cap_space_all", descending=True).head()

        See Also:
            * `hoopR`_ -- ``spotrac_team_cap()``, the R original.

        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    if season is None:
        from sportsdataverse.nba.nba_schedule import most_recent_nba_season

        season = int(most_recent_nba_season())

    response = _fetch(f"https://www.spotrac.com/nba/cap/_/year/{season}/", proxy=proxy)
    if response is None:
        return _empty(_SPOTRAC_SCHEMA, return_as_pandas)

    tables = html_tables(response.text or "", min_rows=1)
    if not tables:
        warnings.warn(f"Spotrac served no cap table for {season}.", UserWarning, stacklevel=2)
        return _empty(_SPOTRAC_SCHEMA, return_as_pandas)

    # Largest table, not merely the first: Spotrac wraps the cap grid in nav
    # chrome that occasionally parses as a small table of its own.
    frame: pl.DataFrame = max(tables.values(), key=lambda f: f.height)
    if "team" in frame.columns and frame.schema["team"] == pl.Utf8:
        frame = frame.with_columns(pl.col("team").str.strip_chars().str.split(" ").list.first().alias("team"))
    frame = _currency_to_number(frame)
    frame = frame.with_columns(pl.lit(int(season), pl.Int64).alias("season"))
    return _out(frame, return_as_pandas)


def _hoopshype_contracts(html: str) -> List[Dict[str, Any]]:
    """Pull the contracts array out of a HoopsHype team page's ``__NEXT_DATA__``.

    The page's React-Query cache lives at
    ``props.pageProps.dehydratedState.queries[].state.data.contracts.contracts``;
    a team page's query is limited to 500 results, so the whole roster is present.
    """
    import json

    from bs4 import BeautifulSoup

    node = BeautifulSoup(html or "", "lxml").find("script", id="__NEXT_DATA__")
    if node is None:
        return []
    try:
        blob = json.loads(node.get_text())
    except (ValueError, TypeError):
        return []
    if not isinstance(blob, dict):
        return []
    state = ((blob.get("props") or {}).get("pageProps") or {}).get("dehydratedState") or {}
    for query in state.get("queries") or []:
        data = (query or {}).get("state", {}).get("data")
        contracts = data.get("contracts") if isinstance(data, dict) else None
        if isinstance(contracts, dict) and isinstance(contracts.get("contracts"), list):
            return [c for c in contracts["contracts"] if isinstance(c, dict)]
    return []


def _hoopshype_rows(contracts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Flatten one team's contracts to one row per player-season."""
    rows: List[Dict[str, Any]] = []
    for contract in contracts:
        player = contract.get("player") or {}
        team = player.get("team") or {}
        team_name = " ".join(str(p) for p in (team.get("location"), team.get("nickname")) if p).strip()
        for season in contract.get("seasons") or []:
            if not isinstance(season, dict):
                continue
            rows.append(
                {
                    "player_id": _id_str(contract.get("playerID")),
                    "player": _text(contract.get("playerName")),
                    "first_name": _text(player.get("firstName")),
                    "last_name": _text(player.get("lastName")),
                    "team_id": _id_str(team.get("id")),
                    "team": team_name or None,
                    "season": _int(season.get("season")),
                    "salary": _num(season.get("salary")),
                    "cap_allocation": _num(season.get("capAllocation")),
                    "team_option": _flag(season.get("teamOption")),
                    "player_option": _flag(season.get("playerOption")),
                    "two_way": _flag(season.get("twoWayContract")),
                    "qualifying_offer": _flag(season.get("qualifyingOffer")),
                }
            )
    return rows


def hoopshype_salaries(
    *,
    proxy: Any = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """League-wide NBA player salaries from HoopsHype.

    One row per player per contract season (current plus the future seasons
    HoopsHype lists), for the whole league (~600 players).

    HoopsHype is a Next.js app: the single ``/salaries/players/`` page paginates
    client-side and only ~20 rows survive a static fetch, but each team page
    embeds that team's complete roster in ``<script id="__NEXT_DATA__">``. This
    walks the 30 slugs in :data:`HOOPSHYPE_TEAMS` **serially** -- ~30 requests per
    call -- and parses that JSON. A team page that fails is warned about and
    skipped rather than aborting the league.

    Pacing is environment-tunable, never hardcoded in the fetch path:

    ============================ ==================================================
    ``SDV_PY_HOOPSHYPE_DELAY``   seconds slept between team pages (default ``0.5``)
    ============================ ==================================================

    Args:
        proxy: Proxy configuration forwarded to
            :func:`~sportsdataverse.dl_utils.download` (``requests``
            ``proxies=`` shape).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per player-season with ``player_id``, ``player``, ``first_name``,
        ``last_name``, ``team_id``, ``team``, ``season``, ``salary``,
        ``cap_allocation``, ``team_option``, ``player_option``, ``two_way`` and
        ``qualifying_offer``. Ids are ``Utf8``, money is ``Float64``, options are
        ``Boolean``. All 30 pages failing yields a zero-row frame with that schema.

    Raises:
        UserWarning: Emitted (not raised) per team page that cannot be fetched or
            parsed; the remaining teams still load.

    Example:
        Quick start::

            from sportsdataverse.nba import hoopshype_salaries

            salaries = hoopshype_salaries()
            print(salaries.shape)

        As pandas::

            salaries_pd = hoopshype_salaries(return_as_pandas=True)

        Pipeline next step (this season's top-paid)::

            salaries.filter(pl.col("season") == 2026).sort("salary", descending=True).head()

        See Also:
            * `hoopR`_ -- ``hoopshype_salaries()``, the R original.

        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    delay = _env_float("SDV_PY_HOOPSHYPE_DELAY", 0.5)
    rows: List[Dict[str, Any]] = []
    for index, slug in enumerate(HOOPSHYPE_TEAMS):
        if index and delay:
            time.sleep(delay)
        response = _fetch(f"https://www.hoopshype.com/salaries/{slug}/", proxy=proxy)
        if response is None:
            continue
        contracts = _hoopshype_contracts(response.text or "")
        if not contracts:
            warnings.warn(f"HoopsHype page for {slug} carried no __NEXT_DATA__ contracts.", UserWarning, stacklevel=2)
            continue
        rows.extend(_hoopshype_rows(contracts))

    if not rows:
        return _empty(_HOOPSHYPE_SCHEMA, return_as_pandas)
    return _out(pl.DataFrame(rows, schema=_HOOPSHYPE_SCHEMA), return_as_pandas)


def nbadraft_mock_draft(
    year: Optional[int] = None,
    *,
    proxy: Any = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """The current consensus mock draft from NBADraft.net.

    One row per pick across both rounds. The page renders round 1 and round 2 as
    the first two pick tables and then **repeats round 1 in a third table**, so
    only the first two are taken -- concatenating all three double-counts round 1.
    The ``<noscript>`` fallback is another false-positive JS challenge; the pick
    tables are static. A traded pick's team cell carries ``*``, which is stripped.

    Args:
        year: Draft year (e.g. ``2025``). ``None`` (default) reads the site's
            current mock; a year uses the ``/nba-mock-drafts/{year}/`` path where
            NBADraft.net has one.
        proxy: Proxy configuration forwarded to
            :func:`~sportsdataverse.dl_utils.download` (``requests``
            ``proxies=`` shape).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per pick with ``round`` (1 or 2), ``pick``, ``team``, ``player``,
        ``height``, ``weight``, ``position``, ``school`` and ``class``. An
        unreachable or table-less page yields a zero-row frame with that schema.

    Raises:
        UserWarning: Emitted (not raised) when the page cannot be fetched or
            carries no pick table; the call still returns an empty frame.

    Example:
        Quick start::

            from sportsdataverse.nba import nbadraft_mock_draft

            mock = nbadraft_mock_draft()
            print(mock.shape)

        A specific draft year, as pandas::

            mock_pd = nbadraft_mock_draft(year=2025, return_as_pandas=True)

        Pipeline next step (lottery only)::

            mock.filter((pl.col("round") == 1) & (pl.col("pick") <= 14))

        See Also:
            * `hoopR`_ -- ``nbadraft_mock_draft()``, the R original.

        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    url = "https://www.nbadraft.net/nba-mock-drafts/"
    if year is not None:
        url = f"{url}{int(year)}/"

    response = _fetch(url, proxy=proxy)
    if response is None:
        return _empty(_NBADRAFT_SCHEMA, return_as_pandas)

    tables = html_tables(response.text or "", min_rows=1)
    picks = [frame for frame in tables.values() if "player" in frame.columns]
    if not picks:
        warnings.warn("NBADraft.net served no mock-draft pick table.", UserWarning, stacklevel=2)
        return _empty(_NBADRAFT_SCHEMA, return_as_pandas)

    frames: List[pl.DataFrame] = []
    # Only the FIRST TWO pick tables -- the third repeats round 1.
    for round_number, frame in enumerate(picks[:2], start=1):
        renames = {
            old: new for old, new in _NBADRAFT_RENAME.items() if old in frame.columns and new not in frame.columns
        }
        frame = frame.rename(renames)
        if "team" in frame.columns and frame.schema["team"] == pl.Utf8:
            frame = frame.with_columns(pl.col("team").str.replace_all(r"\*", "").str.strip_chars().alias("team"))
        frames.append(frame.with_columns(pl.lit(round_number, pl.Int64).alias("round")))

    out = pl.concat(frames, how="diagonal_relaxed")
    ordered = [c for c in _NBADRAFT_SCHEMA if c in out.columns]
    return _out(out.select(ordered + [c for c in out.columns if c not in ordered]), return_as_pandas)


def _strip_html(value: Any) -> Optional[str]:
    """Strip tags from a RotoWire cell; blank / subscriber-gated text becomes ``None``."""
    if value is None:
        return None
    text = _TAG.sub("", str(value)).strip()
    if not text or "subscribers only" in text.lower():
        return None
    return text


def rotowire_injuries(
    *,
    proxy: Any = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """The current NBA injury report from RotoWire.

    One row per injured player: team, position, the injury, the designation
    (Out / Doubtful / Questionable / GTD / Day-To-Day) and a link to the player's
    RotoWire page. This is the live replacement for the defunct RotoWorld feed.

    The rendered grid at ``/basketball/news.php?view=injuries`` builds itself
    client-side, so this reads the JSON table endpoint the grid calls
    (``/basketball/tables/injury-report.php?team=ALL&pos=ALL``) rather than
    scraping the page. The projected return date is subscriber-gated and comes
    back as ``"Subscribers Only"``; it is returned as null for non-subscribers.

    Args:
        proxy: Proxy configuration forwarded to
            :func:`~sportsdataverse.dl_utils.download` (``requests``
            ``proxies=`` shape).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per injured player with ``player_id``, ``player``, ``first_name``,
        ``last_name``, ``team``, ``position``, ``injury``, ``status``,
        ``return_date`` and ``url``. An unreachable endpoint or a non-list body
        yields a zero-row frame with that schema.

    Raises:
        UserWarning: Emitted (not raised) when the endpoint cannot be fetched or
            does not answer with a JSON array; the call still returns an empty
            frame.

    Example:
        Quick start::

            from sportsdataverse.nba import rotowire_injuries

            injuries = rotowire_injuries()
            print(injuries.shape)

        As pandas::

            injuries_pd = rotowire_injuries(return_as_pandas=True)

        Pipeline next step (who is ruled out)::

            injuries.filter(pl.col("status") == "Out").select("player", "team", "injury")

        See Also:
            * `hoopR`_ -- ``rotowire_injuries()``, the R original.

        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    response = _fetch(
        "https://www.rotowire.com/basketball/tables/injury-report.php",
        params={"team": "ALL", "pos": "ALL"},
        proxy=proxy,
    )
    if response is None:
        return _empty(_ROTOWIRE_SCHEMA, return_as_pandas)

    try:
        payload = response.json()
    except ValueError:
        payload = None
    if not isinstance(payload, list) or not payload:
        warnings.warn("RotoWire returned no injury rows.", UserWarning, stacklevel=2)
        return _empty(_ROTOWIRE_SCHEMA, return_as_pandas)

    rows: List[Dict[str, Any]] = []
    for record in payload:
        if not isinstance(record, dict):
            continue
        link = _text(record.get("URL"))
        if link is not None and not link.startswith("http"):
            link = f"https://www.rotowire.com{link}"
        rows.append(
            {
                "player_id": _id_str(record.get("ID")),
                "player": _text(record.get("player")),
                "first_name": _text(record.get("firstname")),
                "last_name": _text(record.get("lastname")),
                "team": _text(record.get("team")),
                "position": _text(record.get("position")),
                "injury": _text(record.get("injury")),
                "status": _text(record.get("status")),
                "return_date": _strip_html(record.get("rDate")),
                "url": link,
            }
        )

    if not rows:
        return _empty(_ROTOWIRE_SCHEMA, return_as_pandas)
    return _out(pl.DataFrame(rows, schema=_ROTOWIRE_SCHEMA), return_as_pandas)
