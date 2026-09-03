"""Basketball-Reference scrapers for the NBA and WNBA (basketball-reference.com).

`Basketball-Reference <https://www.basketball-reference.com>`_ (Sports Reference
LLC) has no API -- every dataset here is a server-rendered HTML table. This module
is the Python port of hoopR's nine ``bref_*()`` wrappers (``R/bref_*.R``) and
wehoop's three ``bref_wnba_*()`` wrappers (``R/bref_wnba.R``). Basketball-Reference
serves the NBA under ``/leagues/NBA_*`` and the WNBA under ``/wnba/years/*``; where
the page shape is the same the ``league=`` argument selects between them instead of
a duplicated function family.

**PORTING HONESTY -- NOT VALIDATED AGAINST LIVE HTML.** This module was written
with no network access. Every URL path, table ``id`` and column name below is
*transcribed* from the R wrappers and from
``sdv-internal-refs/basketball-reference/README.md`` -- none of it was checked
against a live page. The unit tests exercise the parser against inline fixtures
only. A first live run should verify, in this order:

1. **Table ids resolve.** ``bref_teams_stats`` (``per_game-team`` …),
   ``bref_standings`` (``confs_standings_E`` / ``standings_e``) and
   ``bref_wnba``-shaped pages are the fragile ones -- Sports Reference has
   renamed table ids before. An unknown id yields a zero-row frame, not an error.
2. **``data-stat`` keys.** The renames applied here (``name_display`` -> ``player``,
   ``team_name_abbr`` -> ``team``, ``games`` -> ``g``, ``games_started`` -> ``gs``,
   ``team_name`` -> ``team``) are no-ops if upstream has changed the underlying key,
   so a missing ``player`` column means the key moved, not that the row is empty.
3. **WNBA player-stat table id.** wehoop selects the table by the bare ``table``
   name (``per_game``) on the WNBA player page but by first-table position on the
   NBA one; that asymmetry is reproduced here and is worth confirming.
4. **Rate limiting.** Confirm ~20 requests/minute is still the ceiling before
   lowering ``SDV_PY_BREF_RATE_DELAY``.

Two Sports-Reference quirks are handled centrally in :func:`_bref_table`:

* **Tables hidden in HTML comments.** All but the first table on a page is wrapped
  in ``<!-- ... -->`` to defer rendering; the comment markers are stripped before
  parsing or the secondary tables are invisible.
* **Column names come from each cell's ``data-stat`` attribute**, never from the
  rendered header. Sports Reference tables carry multi-row "over-headers" that
  flatten into useless names; ``data-stat`` carries the stable canonical keys
  (``pts_per_g``, ``ts_pct``, ``ws``, ``award_share`` …).

Rate limiting: Basketball-Reference 429s / temporarily blocks past roughly **20
requests per minute**. Requests are spaced by ``SDV_PY_BREF_RATE_DELAY`` seconds
(default ``3.0``, read at call time); set it to ``0`` to disable the pacing.

Divergences from the R wrappers, all deliberate:

* ``conference`` is ``"E"`` / ``"W"`` for both leagues; wehoop emits
  ``"Eastern"`` / ``"Western"``.
* WNBA player stats name the team column ``team``; wehoop names it ``team_id``.
* Award voting and standings pages carry several tables but are returned stacked
  into one frame (with an ``award`` / ``conference`` column), matching the R
  wrappers rather than the multi-table ``dict`` convention.
"""

from __future__ import annotations

import os
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import polars as pl

from sportsdataverse.dl_utils import download
from sportsdataverse.errors import NoDataError

if TYPE_CHECKING:  # pragma: no cover -- annotation-only import
    import pandas as pd

__all__ = [
    "bref_awards",
    "bref_draft",
    "bref_injuries",
    "bref_player_bios",
    "bref_player_game_log",
    "bref_players_stats",
    "bref_standings",
    "bref_team_roster",
    "bref_teams_stats",
]

_BASE_URL = "https://www.basketball-reference.com"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) sportsdataverse-py Safari/537.36"
    )
}

_RATE_DELAY_ENV = "SDV_PY_BREF_RATE_DELAY"
_DEFAULT_RATE_DELAY = 3.0

# Wall-clock of the last Basketball-Reference request, for request spacing.
# ponytail: module-global, not thread-safe. Concurrent scraping of this host is a
# ban, not a speedup, so a per-thread budget would be the wrong upgrade anyway.
_LAST_REQUEST = 0.0

_PLAYER_TABLES = {
    "nba": ("per_game", "totals", "advanced", "per_minute", "per_poss"),
    "wnba": ("per_game", "totals", "advanced"),
}
_TEAM_TABLES: Dict[str, Dict[str, str]] = {
    "nba": {
        "per_game": "per_game-team",
        "totals": "totals-team",
        "per_poss": "per_poss-team",
        "advanced": "advanced-team",
        "opponent": "per_game-opponent",
    },
    "wnba": {
        "per_game": "per_game-team",
        "totals": "totals-team",
        "per_poss": "per_poss-team",
        "advanced": "advanced-team",
    },
}
_STANDINGS_TABLES = {
    "nba": (("confs_standings_E", "E"), ("confs_standings_W", "W")),
    "wnba": (("standings_e", "E"), ("standings_w", "W")),
}


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------


def _rate_delay() -> float:
    """Seconds to space consecutive requests, from ``SDV_PY_BREF_RATE_DELAY``.

    Read at call time (not import time) so a script or CI step can retune the
    pace after the module is imported. A malformed or negative value falls back
    to the default rather than raising -- this runs on every request.

    Returns:
        The delay in seconds; ``3.0`` when the variable is unset or unusable.
    """
    try:
        value = float(os.environ.get(_RATE_DELAY_ENV, _DEFAULT_RATE_DELAY))
    except (TypeError, ValueError):
        return _DEFAULT_RATE_DELAY
    return value if value >= 0 else _DEFAULT_RATE_DELAY


def _throttle() -> None:
    """Sleep just long enough to keep requests ``_rate_delay()`` seconds apart."""
    global _LAST_REQUEST
    delay = _rate_delay()
    if delay <= 0:
        return
    wait = delay - (time.monotonic() - _LAST_REQUEST)
    if wait > 0:
        time.sleep(wait)
    _LAST_REQUEST = time.monotonic()


def _bref_get(path: str, *, proxy: Any = None, **kwargs: Any) -> str:
    """GET a Basketball-Reference page and return its raw HTML.

    Args:
        path: Page path beginning with ``/`` (e.g. ``"/leagues/NBA_2024.html"``).
        proxy: Proxy configuration in the ``requests`` ``proxies=`` shape.
        **kwargs: Forwarded to :func:`sportsdataverse.dl_utils.download`
            (``timeout``, ``num_retries``, ``session`` …).

    Returns:
        The page HTML, or ``""`` when the page does not exist (a 404 is a
        definitive "no such season/player" here, and the callers turn an empty
        body into a zero-row frame).
    """
    _throttle()
    try:
        resp = download(url=f"{_BASE_URL}{path}", headers=_HEADERS, proxy=proxy, **kwargs)
    except NoDataError:
        return ""
    return getattr(resp, "text", "") or ""


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def _bref_table(html: str, table_id: Optional[str] = None) -> pl.DataFrame:
    """Parse one Basketball-Reference table into an all-``Utf8`` frame.

    This is a **dedicated** Sports-Reference parser and deliberately does NOT use
    :func:`sportsdataverse._html_tables.html_tables`. That helper derives column
    names from the rendered ``<thead>``, which is exactly what breaks here:
    Sports Reference stacks a ``colspan`` "over-header" ("Per Game", "Shooting",
    "Voting") above the real header row, so the rendered names flatten into
    useless labels. Every cell instead carries a ``data-stat`` attribute holding
    Sports Reference's stable canonical key, and that is what is read below.
    Do not "simplify" this into the shared header-based reader.

    The second quirk handled here: all but the first table on a page is wrapped
    in ``<!-- ... -->`` to defer rendering, so the comment markers are stripped
    before parsing (the R port does the same with two ``gsub()`` calls).

    Args:
        html: Full page HTML.
        table_id: The table's HTML ``id``; ``None`` selects the first table.

    Returns:
        A ``pl.DataFrame`` of string columns keyed by ``data-stat``, in
        first-seen order. A zero-row, zero-column frame when the page is empty,
        the id is absent, or the table has no body rows.
    """
    from bs4 import BeautifulSoup

    # Quirk 1: un-comment the page so comment-hidden tables are reachable.
    text = (html or "").replace("<!--", "").replace("-->", "")
    if not text.strip():
        return pl.DataFrame()

    soup = BeautifulSoup(text, "lxml")
    node = soup.find("table", id=table_id) if table_id else soup.find("table")
    if node is None:
        return pl.DataFrame()

    bodies = node.find_all("tbody")
    if bodies:
        trs = [tr for body in bodies for tr in body.find_all("tr", recursive=False)]
    else:  # no <tbody> -- take every row that is not inside a <thead>
        trs = [tr for tr in node.find_all("tr") if tr.find_parent("thead") is None]

    rows: List[Dict[str, str]] = []
    for tr in trs:
        # Sports Reference repeats the header mid-table as class="thead".
        if "thead" in (tr.get("class") or []):
            continue
        row: Dict[str, str] = {}
        for cell in tr.find_all(["th", "td"]):
            # Quirk 2: the column name is the cell's data-stat, not the header.
            key = cell.get("data-stat")
            if not key:
                continue
            row[key] = cell.get_text(strip=True)
        if row:
            rows.append(row)
    if not rows:
        return pl.DataFrame()

    keys: List[str] = []
    for row in rows:
        for key in row:
            if key not in keys:
                keys.append(key)
    return pl.DataFrame(
        {key: [row.get(key) for row in rows] for key in keys},
        schema={key: pl.Utf8 for key in keys},
    )


def _finish(df: pl.DataFrame) -> pl.DataFrame:
    """Drop spacer columns and blank rows, then numeric-cast what is fully numeric.

    The Python counterpart of wehoop's ``.bref_wnba_finish()`` plus hoopR's
    ``.bref_type_convert()``: Sports Reference emits blank ``DUMMY`` spacer
    columns and all-empty separator rows, and every value arrives as a string.

    Args:
        df: The raw string frame from :func:`_bref_table`.

    Returns:
        The cleaned frame; columns whose non-blank values all parse as numbers
        become ``Float64``, everything else stays ``Utf8``.
    """
    if df.height == 0 or df.width == 0:
        return df
    keep = [c for c in df.columns if c and c != "DUMMY"]
    df = df.select(keep)
    if not keep:
        return pl.DataFrame()

    blank = [(pl.col(c).is_not_null()) & (pl.col(c) != "") for c in df.columns]
    df = df.filter(pl.any_horizontal(blank))

    for name in df.columns:
        col = df[name]
        nonblank = col.filter(col.is_not_null() & (col != ""))
        if nonblank.len() == 0:
            continue
        if nonblank.cast(pl.Float64, strict=False).null_count() == 0:
            df = df.with_columns(pl.col(name).cast(pl.Float64, strict=False))
    return df


def _rename(df: pl.DataFrame, mapping: Dict[str, str]) -> pl.DataFrame:
    """Rename only the columns that are actually present (``dplyr::any_of``)."""
    return df.rename({old: new for old, new in mapping.items() if old in df.columns})


def _with_literals(df: pl.DataFrame, **values: Any) -> pl.DataFrame:
    """Append constant echo columns (``season``, ``team``, ``letter`` …)."""
    if df.width == 0:
        return df
    return df.with_columns(
        [
            pl.lit(value, dtype=pl.Int64).alias(name)
            if isinstance(value, int) and not isinstance(value, bool)
            else pl.lit(value).alias(name)
            for name, value in values.items()
        ]
    )


def _out(df: pl.DataFrame, return_as_pandas: bool) -> pl.DataFrame | pd.DataFrame:
    """Convert to pandas on request."""
    return df.to_pandas() if return_as_pandas else df


def _check_league(league: str) -> str:
    """Normalize and validate the ``league`` argument."""
    value = str(league).lower()
    if value not in ("nba", "wnba"):
        raise ValueError(f"`league` must be one of 'nba', 'wnba'. You passed {league!r}.")
    return value


def _default_season(league: str) -> int:
    """The current season for ``league``, in 4-digit ending-year format."""
    if league == "wnba":
        from sportsdataverse.wnba.wnba_schedule import most_recent_wnba_season

        return int(most_recent_wnba_season())
    from sportsdataverse.nba.nba_schedule import most_recent_nba_season

    return int(most_recent_nba_season())


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def bref_players_stats(
    season: Optional[int] = None,
    table: str = "per_game",
    league: str = "nba",
    *,
    return_as_pandas: bool = False,
    proxy: Any = None,
    **kwargs: Any,
) -> pl.DataFrame | pd.DataFrame:
    """Player season statistics for an entire league season.

    Port of hoopR's ``bref_players_stats()`` (NBA) and wehoop's
    ``bref_wnba_player_stats()`` (WNBA). One row per player, with columns named
    by Basketball-Reference ``data-stat`` keys.

    Args:
        season: Season in 4-digit ending-year format (``2024`` = 2023-24). The
            WNBA season is a plain calendar year. Defaults to the current season.
        table: Which stat table. NBA accepts ``per_game`` (default), ``totals``,
            ``advanced``, ``per_minute`` (per 36) and ``per_poss`` (per 100
            possessions); WNBA accepts ``per_game``, ``totals`` and ``advanced``.
        league: ``"nba"`` (default) or ``"wnba"``.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.
        proxy: Proxy configuration in the ``requests`` ``proxies=`` shape.
        **kwargs: Forwarded to :func:`sportsdataverse.dl_utils.download`.

    Returns:
        One row per player: ``ranker``, ``player``, ``age``, ``team``, ``pos``,
        ``g``, ``gs`` plus the box columns scaled to ``table`` (the ``advanced``
        table adds ``per``, ``ts_pct``, ``usg_pct``, ``ws``, ``bpm``, ``vorp``
        …), and echoed ``season`` / ``table`` / ``league`` columns. A zero-row
        frame when the page carries no player table.

    Raises:
        ValueError: When ``league`` is not ``"nba"``/``"wnba"``, or ``table`` is
            not one of that league's stat tables.

    Example:
        Quick start::

            from sportsdataverse.nba.bref import bref_players_stats

            df = bref_players_stats(season=2024)
            print(df.shape)

        Advanced metrics, and the WNBA page::

            adv = bref_players_stats(season=2024, table="advanced")
            wnba = bref_players_stats(season=2024, league="wnba")

        Pipeline next step (one line)::

            adv.filter(pl.col("vorp") > 3.0).sort("vorp", descending=True).head()

        See Also:
            * `hoopR`_ -- R sister package; ``bref_players_stats()``.
            * `wehoop`_ -- R sister package; ``bref_wnba_player_stats()``.

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _wehoop: https://wehoop.sportsdataverse.org
    """
    league = _check_league(league)
    valid = _PLAYER_TABLES[league]
    if table not in valid:
        raise ValueError(f"`table` must be one of {list(valid)} for {league}. You passed {table!r}.")
    season = int(season) if season is not None else _default_season(league)

    if league == "wnba":
        html = _bref_get(f"/wnba/years/{season}_{table}.html", proxy=proxy, **kwargs)
        # wehoop selects the WNBA player table by its bare `table` name; hoopR
        # takes the first table on the NBA page (id `{table}_stats`).
        raw = _bref_table(html, table)
    else:
        html = _bref_get(f"/leagues/NBA_{season}_{table}.html", proxy=proxy, **kwargs)
        raw = _bref_table(html)

    df = _finish(raw)
    df = _rename(
        df,
        {
            "name_display": "player",
            "team_name_abbr": "team",
            "games": "g",
            "games_started": "gs",
        },
    )
    if "player" in df.columns:
        df = df.filter(pl.col("player").is_not_null() & (pl.col("player") != "Player"))
    df = _with_literals(df, season=season, table=table, league=league)
    return _out(df, return_as_pandas)


def bref_teams_stats(
    season: Optional[int] = None,
    table: str = "per_game",
    league: str = "nba",
    *,
    return_as_pandas: bool = False,
    proxy: Any = None,
    **kwargs: Any,
) -> pl.DataFrame | pd.DataFrame:
    """Team season statistics from the league season page.

    Port of hoopR's ``bref_teams_stats()`` (NBA) and wehoop's
    ``bref_wnba_team_stats()`` (WNBA). Every team table lives on the one season
    page and all but the first are comment-hidden, which is why the table ``id``
    selection in :func:`_bref_table` matters here.

    Args:
        season: Season in 4-digit ending-year format (``2024`` = 2023-24).
            Defaults to the current season.
        table: NBA accepts ``per_game`` (default), ``totals``, ``per_poss``,
            ``advanced`` and ``opponent`` (opponent per-game); WNBA accepts the
            same set minus ``opponent``.
        league: ``"nba"`` (default) or ``"wnba"``.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.
        proxy: Proxy configuration in the ``requests`` ``proxies=`` shape.
        **kwargs: Forwarded to :func:`sportsdataverse.dl_utils.download`.

    Returns:
        One row per team: ``ranker``, ``team``, ``g``, ``mp`` and the box
        categories scaled to ``table``, plus echoed ``season`` / ``table`` /
        ``league``. The WNBA path drops the ``League Average`` footer row, as
        wehoop does. A zero-row frame when the table id is absent.

    Raises:
        ValueError: When ``league`` or ``table`` is not one of the supported
            values for that league.

    Example:
        Quick start::

            from sportsdataverse.nba.bref import bref_teams_stats

            df = bref_teams_stats(season=2024)
            print(df.shape)

        Opponent per-game, and pandas output::

            opp = bref_teams_stats(season=2024, table="opponent")
            df_pd = bref_teams_stats(season=2024, return_as_pandas=True)

        Pipeline next step (one line)::

            df.sort("pts_per_g", descending=True).head()

        See Also:
            * `hoopR`_ -- R sister package; ``bref_teams_stats()``.
            * `wehoop`_ -- R sister package; ``bref_wnba_team_stats()``.

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _wehoop: https://wehoop.sportsdataverse.org
    """
    league = _check_league(league)
    ids = _TEAM_TABLES[league]
    if table not in ids:
        raise ValueError(f"`table` must be one of {list(ids)} for {league}. You passed {table!r}.")
    season = int(season) if season is not None else _default_season(league)

    path = f"/wnba/years/{season}.html" if league == "wnba" else f"/leagues/NBA_{season}.html"
    df = _finish(_bref_table(_bref_get(path, proxy=proxy, **kwargs), ids[table]))
    df = _rename(df, {"team_name": "team", "games": "g"})
    if league == "wnba" and "team" in df.columns:
        df = df.filter(pl.col("team").is_not_null() & (pl.col("team") != "League Average"))
    df = _with_literals(df, season=season, table=table, league=league)
    return _out(df, return_as_pandas)


def bref_standings(
    season: Optional[int] = None,
    league: str = "nba",
    *,
    return_as_pandas: bool = False,
    proxy: Any = None,
    **kwargs: Any,
) -> pl.DataFrame | pd.DataFrame:
    """Conference standings for a season, both conferences stacked.

    Port of hoopR's ``bref_standings()`` (NBA) and wehoop's
    ``bref_wnba_standings()`` (WNBA).

    Args:
        season: Season in 4-digit ending-year format. Defaults to the current
            season.
        league: ``"nba"`` (default) or ``"wnba"``.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.
        proxy: Proxy configuration in the ``requests`` ``proxies=`` shape.
        **kwargs: Forwarded to :func:`sportsdataverse.dl_utils.download`.

    Returns:
        One row per team: ``conference`` (``"E"`` / ``"W"`` for **both** leagues
        -- wehoop emits ``"Eastern"``/``"Western"``), ``team`` (playoff ``*``
        marker stripped), ``playoffs`` (bool, from that marker), ``wins``,
        ``losses``, ``win_loss_pct``, ``gb``, ``pts_per_g``, ``opp_pts_per_g``,
        ``srs``, plus echoed ``season`` / ``league``. Zero rows when neither
        conference table is present.

    Raises:
        ValueError: When ``league`` is not ``"nba"``/``"wnba"``.

    Example:
        Quick start::

            from sportsdataverse.nba.bref import bref_standings

            df = bref_standings(season=2024)
            print(df.shape)

        The WNBA page::

            wnba = bref_standings(season=2024, league="wnba")

        Pipeline next step (one line)::

            df.filter(pl.col("playoffs") == True).sort("srs", descending=True).head()

        See Also:
            * `hoopR`_ -- R sister package; ``bref_standings()``.
            * `wehoop`_ -- R sister package; ``bref_wnba_standings()``.

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _wehoop: https://wehoop.sportsdataverse.org
    """
    league = _check_league(league)
    season = int(season) if season is not None else _default_season(league)

    if league == "wnba":
        path = f"/wnba/years/{season}.html"
    else:
        path = f"/leagues/NBA_{season}_standings.html"
    html = _bref_get(path, proxy=proxy, **kwargs)

    parts: List[pl.DataFrame] = []
    for table_id, conference in _STANDINGS_TABLES[league]:
        part = _finish(_bref_table(html, table_id))
        if part.height == 0:
            continue
        parts.append(part.with_columns(pl.lit(conference).alias("conference")))
    if not parts:
        return _out(pl.DataFrame(), return_as_pandas)

    df = pl.concat(parts, how="diagonal_relaxed")
    df = _rename(df, {"team_name": "team"})
    if "team" in df.columns:
        # The trailing '*' marks playoff teams -- surface it as a flag, clean the name.
        df = df.with_columns(
            pl.col("team").str.contains("*", literal=True).alias("playoffs"),
            pl.col("team").str.replace_all("*", "", literal=True).str.strip_chars().alias("team"),
        )
    df = _with_literals(df, season=season, league=league)
    return _out(df, return_as_pandas)


def bref_awards(
    season: Optional[int] = None,
    *,
    return_as_pandas: bool = False,
    proxy: Any = None,
    **kwargs: Any,
) -> pl.DataFrame | pd.DataFrame:
    """End-of-season award voting, all awards stacked into one frame.

    Port of hoopR's ``bref_awards()``. NBA only -- wehoop wraps no WNBA awards
    page, so none is guessed at here.

    Args:
        season: Season in 4-digit ending-year format. Defaults to the current
            NBA season.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.
        proxy: Proxy configuration in the ``requests`` ``proxies=`` shape.
        **kwargs: Forwarded to :func:`sportsdataverse.dl_utils.download`.

    Returns:
        One row per candidate per award: ``award`` (``mvp``, ``roy``, ``dpoy``,
        ``smoy``, ``mip``, ``clutch_poy``, ``coy``), ``rank``, ``player``,
        ``age``, ``team``, ``votes_first``, ``points_won``, ``points_max``,
        ``award_share``, plus ``season``. Zero rows when the page carries no
        voting table (award voting predates 1956 for none of them).

    Raises:
        ValueError: Never raised directly; a missing page yields a zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.nba.bref import bref_awards

            df = bref_awards(season=2024)
            print(df.shape)

        Pandas output::

            df_pd = bref_awards(season=2024, return_as_pandas=True)

        Pipeline next step (one line)::

            df.filter(pl.col("award") == "mvp").sort("award_share", descending=True).head()

        See Also:
            * `hoopR`_ -- R sister package; ``bref_awards()``.

        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    season = int(season) if season is not None else _default_season("nba")
    awards = ("mvp", "roy", "dpoy", "smoy", "mip", "clutch_poy", "coy")
    voting_cols = (
        "rank",
        "player",
        "name_display",
        "age",
        "team_id",
        "votes_first",
        "points_won",
        "points_max",
        "award_share",
    )

    html = _bref_get(f"/awards/awards_{season}.html", proxy=proxy, **kwargs)
    parts: List[pl.DataFrame] = []
    for award in awards:
        part = _finish(_bref_table(html, award))
        if part.height == 0:
            continue
        keep = [c for c in voting_cols if c in part.columns]
        parts.append(part.select(keep).with_columns(pl.lit(award).alias("award")))
    if not parts:
        return _out(pl.DataFrame(), return_as_pandas)

    df = pl.concat(parts, how="diagonal_relaxed")
    df = _rename(df, {"name_display": "player", "team_id": "team"})
    df = _with_literals(df, season=season)
    return _out(df, return_as_pandas)


def bref_draft(
    season: Optional[int] = None,
    *,
    return_as_pandas: bool = False,
    proxy: Any = None,
    **kwargs: Any,
) -> pl.DataFrame | pd.DataFrame:
    """NBA draft results with each pick's career totals and advanced metrics.

    Port of hoopR's ``bref_draft()``. NBA only.

    Args:
        season: Draft year (e.g. ``2024``). Defaults to the current NBA season.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.
        proxy: Proxy configuration in the ``requests`` ``proxies=`` shape.
        **kwargs: Forwarded to :func:`sportsdataverse.dl_utils.download`.

    Returns:
        One row per pick: ``pick_overall``, ``round``, ``team``, ``player``,
        ``college_name``, ``seasons``, ``g``, ``mp``, ``pts``, ``trb``, ``ast``,
        ``fg_pct`` …, ``ws``, ``ws_per_48``, ``bpm``, ``vorp``, plus ``season``.
        Zero rows when the draft page is absent.

    Raises:
        ValueError: Never raised directly; a missing page yields a zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.nba.bref import bref_draft

            df = bref_draft(season=2024)
            print(df.shape)

        Pandas output::

            df_pd = bref_draft(season=2003, return_as_pandas=True)

        Pipeline next step (one line)::

            df.sort("vorp", descending=True).head()

        See Also:
            * `hoopR`_ -- R sister package; ``bref_draft()``.

        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    season = int(season) if season is not None else _default_season("nba")
    df = _finish(_bref_table(_bref_get(f"/draft/NBA_{season}.html", proxy=proxy, **kwargs), "stats"))
    df = _rename(df, {"name_display": "player", "team_id": "team"})
    df = _with_literals(df, season=season)
    return _out(df, return_as_pandas)


def bref_team_roster(
    team: str,
    season: Optional[int] = None,
    *,
    return_as_pandas: bool = False,
    proxy: Any = None,
    **kwargs: Any,
) -> pl.DataFrame | pd.DataFrame:
    """A team's roster for one season.

    Port of hoopR's ``bref_team_roster()``. NBA only.

    Args:
        team: Basketball-Reference team abbreviation (``BOS``, ``LAL``,
            ``GSW``). Historical franchises use their era code (``NJN``,
            ``SEA``).
        season: Season in 4-digit ending-year format. Defaults to the current
            NBA season.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.
        proxy: Proxy configuration in the ``requests`` ``proxies=`` shape.
        **kwargs: Forwarded to :func:`sportsdataverse.dl_utils.download`.

    Returns:
        One row per rostered player: ``number``, ``player``, ``pos``,
        ``height``, ``weight``, ``birth_date``, ``flag``, ``years_experience``,
        ``college``, plus echoed ``team`` / ``season``. Zero rows when the
        team/season combination has no page.

    Raises:
        ValueError: Never raised directly; an unknown team yields a zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.nba.bref import bref_team_roster

            df = bref_team_roster(team="BOS", season=2024)
            print(df.shape)

        A historical franchise code::

            sonics = bref_team_roster(team="SEA", season=1996)

        Pipeline next step (one line)::

            df.select(["player", "pos", "height", "college"]).head()

        See Also:
            * `hoopR`_ -- R sister package; ``bref_team_roster()``.

        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    season = int(season) if season is not None else _default_season("nba")
    path = f"/teams/{str(team).upper()}/{season}.html"
    df = _finish(_bref_table(_bref_get(path, proxy=proxy, **kwargs), "roster"))
    df = _with_literals(df, team=str(team).upper(), season=season)
    return _out(df, return_as_pandas)


def bref_player_game_log(
    player_id: str,
    season: Optional[int] = None,
    *,
    return_as_pandas: bool = False,
    proxy: Any = None,
    **kwargs: Any,
) -> pl.DataFrame | pd.DataFrame:
    """A player's regular-season game-by-game log.

    Port of hoopR's ``bref_player_game_log()``. NBA only. The playoff log on the
    same page (``player_game_log_post``) is not wrapped, matching the R surface.

    Args:
        player_id: Basketball-Reference player id slug -- the id in the player's
            URL, e.g. ``jokicni01`` from ``/players/j/jokicni01.html``. Use
            :func:`bref_player_bios` as the id dictionary.
        season: Season in 4-digit ending-year format. Defaults to the current
            NBA season.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.
        proxy: Proxy configuration in the ``requests`` ``proxies=`` shape.
        **kwargs: Forwarded to :func:`sportsdataverse.dl_utils.download`.

    Returns:
        One row per regular-season game: ``ranker``,
        ``player_game_num_career``, ``date``, ``team``, ``location`` (``@`` for
        away), ``opp``, ``result``, ``is_starter``, ``mp``, the full shooting /
        box columns, ``game_score``, ``plus_minus``, plus echoed ``player_id`` /
        ``season``. Month-separator and no-date rows are dropped. Zero rows when
        the player did not play that season.

    Raises:
        ValueError: Never raised directly; an unknown slug yields a zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.nba.bref import bref_player_game_log

            df = bref_player_game_log(player_id="jokicni01", season=2024)
            print(df.shape)

        Pandas output::

            df_pd = bref_player_game_log("jamesle01", 2024, return_as_pandas=True)

        Pipeline next step (one line)::

            df.select(["date", "opp", "pts", "trb", "ast"]).head()

        See Also:
            * `hoopR`_ -- R sister package; ``bref_player_game_log()``.

        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    season = int(season) if season is not None else _default_season("nba")
    slug = str(player_id).lower()
    path = f"/players/{slug[:1]}/{slug}/gamelog/{season}/"
    df = _finish(_bref_table(_bref_get(path, proxy=proxy, **kwargs), "player_game_log_reg"))
    df = _rename(
        df,
        {
            "team_name_abbr": "team",
            "opp_name_abbr": "opp",
            "game_location": "location",
            "game_result": "result",
        },
    )
    if "date" in df.columns:
        # drop month-separator / DNP rows carrying no date
        df = df.filter(pl.col("date").cast(pl.Utf8).is_not_null() & (pl.col("date").cast(pl.Utf8) != ""))
    df = _with_literals(df, player_id=slug, season=season)
    return _out(df, return_as_pandas)


def bref_player_bios(
    letter: str = "a",
    *,
    return_as_pandas: bool = False,
    proxy: Any = None,
    **kwargs: Any,
) -> pl.DataFrame | pd.DataFrame:
    """The player index for one last-name initial -- bios plus the id slugs.

    Port of hoopR's ``bref_player_bios()``. NBA only. This doubles as the
    Basketball-Reference **player dictionary**: ``player_id`` is the slug that
    :func:`bref_player_game_log` takes.

    Args:
        letter: Single letter ``a``-``z`` (last-name initial). Only the first
            character is used, case-insensitively.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.
        proxy: Proxy configuration in the ``requests`` ``proxies=`` shape.
        **kwargs: Forwarded to :func:`sportsdataverse.dl_utils.download`.

    Returns:
        One row per player: ``player``, ``player_id`` (e.g. ``jamesle01``),
        ``year_min``, ``year_max``, ``pos``, ``height``, ``weight``,
        ``birth_date``, ``colleges``, plus the echoed ``letter``. ``player_id``
        is omitted when the number of player links on the page does not match
        the number of rows (the same guard the R wrapper applies).

    Raises:
        ValueError: When ``letter`` is not a single ``a``-``z`` character.

    Example:
        Quick start::

            from sportsdataverse.nba.bref import bref_player_bios

            df = bref_player_bios(letter="j")
            print(df.shape)

        Build the id dictionary for the whole alphabet::

            import string
            ids = [bref_player_bios(ch) for ch in string.ascii_lowercase]

        Pipeline next step (one line)::

            df.filter(pl.col("year_max") >= 2024).select(["player", "player_id"]).head()

        See Also:
            * `hoopR`_ -- R sister package; ``bref_player_bios()``.

        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    initial = str(letter).lower()[:1]
    if not initial.isalpha() or not initial.isascii():
        raise ValueError(f"`letter` must be a single letter a-z. You passed {letter!r}.")

    html = _bref_get(f"/players/{initial}/", proxy=proxy, **kwargs)
    raw = _bref_table(html, "players")
    ids = _player_id_slugs(html)
    if ids and raw.height == len(ids):
        raw = raw.with_columns(pl.Series("player_id", ids, dtype=pl.Utf8))
        order = ["player", "player_id"] + [c for c in raw.columns if c not in ("player", "player_id")]
        raw = raw.select([c for c in order if c in raw.columns])

    df = _finish(raw)
    df = _with_literals(df, letter=initial)
    return _out(df, return_as_pandas)


def _player_id_slugs(html: str) -> List[str]:
    """Pull each row's Basketball-Reference id slug off the player-index links.

    The slug is not a ``data-stat`` value -- it only exists in the ``href`` of
    the link inside the row's player cell, so it needs its own pass over the
    (un-commented) page.

    Args:
        html: Full ``/players/{letter}/`` page HTML.

    Returns:
        The slugs in row order (``["abdelal01", "abdulza01", ...]``); empty when
        the table or its links are absent.
    """
    from bs4 import BeautifulSoup

    text = (html or "").replace("<!--", "").replace("-->", "")
    if not text.strip():
        return []
    soup = BeautifulSoup(text, "lxml")
    out: List[str] = []
    for anchor in soup.select("table#players th[data-stat='player'] a"):
        href = anchor.get("href") or ""
        slug = str(href).rsplit("/", 1)[-1]
        if slug.endswith(".html"):
            slug = slug[: -len(".html")]
        if slug:
            out.append(slug)
    return out


def bref_injuries(
    *,
    return_as_pandas: bool = False,
    proxy: Any = None,
    **kwargs: Any,
) -> pl.DataFrame | pd.DataFrame:
    """The current NBA injury report.

    Port of hoopR's ``bref_injuries()``. This is the live report -- there is no
    season argument and no history. hoopR uses it in place of RotoWorld, which
    NBC retired.

    Args:
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.
        proxy: Proxy configuration in the ``requests`` ``proxies=`` shape.
        **kwargs: Forwarded to :func:`sportsdataverse.dl_utils.download`.

    Returns:
        One row per injured player: ``player``, ``team_name``, ``date_update``
        and ``note`` (status plus description). Zero rows when no one is listed
        or the page is unreachable.

    Raises:
        ValueError: Never raised directly; an unreachable page yields a zero-row
            frame.

    Example:
        Quick start::

            from sportsdataverse.nba.bref import bref_injuries

            df = bref_injuries()
            print(df.shape)

        Pandas output::

            df_pd = bref_injuries(return_as_pandas=True)

        Pipeline next step (one line)::

            df.filter(pl.col("note").str.contains("(?i)out")).head()

        See Also:
            * `hoopR`_ -- R sister package; ``bref_injuries()``.

        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    df = _finish(_bref_table(_bref_get("/friv/injuries.fcgi", proxy=proxy, **kwargs), "injuries"))
    return _out(df, return_as_pandas)
