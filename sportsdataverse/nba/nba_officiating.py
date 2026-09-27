"""NBA officiating data from official.nba.com: Last Two Minute reports and referee assignments.

Port of the scraping logic in atlhawksfanatic/L2M (MIT, (c) 2019 atlhawksfanatic).
official.nba.com is S3 behind Akamai Bot Manager: a browser User-Agent is required, and a
403 means two different things -- an S3 XML ``AccessDenied`` body is "no such report"
(``NoDataError``) while an Akamai HTML page is a blocked fetch (``AssetFetchError``).
"""

from __future__ import annotations

import datetime as _dt
import json as _json
import re
from typing import Any, Literal, Union, overload

import pandas as pd
import polars as pl
import requests

from sportsdataverse.dl_utils import download
from sportsdataverse.errors import AssetFetchError, NoDataError

__all__ = [
    "parse_nba_l2m",
    "nba_l2m",
    "parse_nba_l2m_games",
    "nba_l2m_games",
    "L2M_CALLS_SCHEMA",
    "L2M_GAME_SCHEMA",
    "L2M_STATS_SCHEMA",
    "parse_nba_referee_assignments",
    "nba_referee_assignments",
    "NBA_REFEREE_ASSIGN_SCHEMA",
    "NBA_REFEREE_REPLAY_SCHEMA",
]

_OFFICIAL_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/html;q=0.9, */*;q=0.8",
    "Referer": "https://official.nba.com/",
}
# 403 is NOT transient here: it is either "no object" or a WAF block, both definitive.
_RETRY_NO_403 = frozenset({408, 429, 500, 502, 503, 504})


def _official_get(url: str, *, params: dict | None = None, proxy: dict | None = None) -> requests.Response:
    """Fetch a URL from ``official.nba.com`` with the browser UA it requires, classifying 403s.

    Args:
        url: Full ``official.nba.com`` URL to fetch.
        params: Optional query-string parameters, forwarded verbatim to
            :func:`sportsdataverse.dl_utils.download` -- prefer this over
            hand-building the query string onto *url*.
        proxy: Optional proxy dict passed through to
            :func:`sportsdataverse.dl_utils.download`.

    Returns:
        The successful (200) ``requests.Response``.

    Raises:
        NoDataError: The response is a 404 (raised by ``download()``), or a 403 with
            an S3 ``AccessDenied`` XML body -- official.nba.com's way of saying no
            report exists for the request.
        AssetFetchError: The response is any other non-200 status, including an
            Akamai WAF block (403 HTML) or a 5xx that outlived the retry budget;
            also raised (chained via ``from exc``) when ``download()`` exhausts
            its retry budget on a connection-level failure (``ProxyError``,
            ``ConnectionError``, ``Timeout``, ...) -- those never escape as a
            bare ``requests.exceptions.RequestException``.

    Example:
        Fetch a Last Two Minute report::

            from sportsdataverse.nba.nba_officiating import _official_get
            resp = _official_get("https://official.nba.com/l2m/json/0042500405.json")
            payload = resp.json()
    """
    try:
        resp = download(url, params=params, headers=_OFFICIAL_HEADERS, proxy=proxy, retry_statuses=_RETRY_NO_403)
    except requests.exceptions.RequestException as exc:
        raise AssetFetchError(f"official.nba.com fetch failed (transport error) for {url}: {exc}") from exc
    if resp.status_code == 200:
        return resp
    if resp.status_code == 403 and "<Code>AccessDenied</Code>" in resp.text[:500]:
        raise NoDataError(f"official.nba.com has no object at {url}")
    ctype = resp.headers.get("Content-Type", "")
    raise AssetFetchError(f"official.nba.com fetch failed ({resp.status_code}, {ctype!r}) for {url}")


def _official_json(resp: requests.Response, url: str) -> dict:
    """Decode a successful ``official.nba.com`` response as JSON, or raise ``AssetFetchError``.

    A 200 status does not guarantee a JSON body -- an Akamai interstitial or a
    misconfigured edge response can return HTML with a 200 status. ``.json()``
    would raise a raw ``json.JSONDecodeError`` in that case; this reclassifies
    it into the package's error vocabulary instead.

    Args:
        resp: A 200 ``requests.Response`` from :func:`_official_get`.
        url: The URL that was fetched, for the error message.

    Returns:
        The decoded JSON payload (dict).

    Raises:
        AssetFetchError: The body is not valid JSON, or is JSON but not an object
            (``null``, a list, a bare string) -- every official.nba.com payload is one.
    """
    try:
        data = resp.json()
    except ValueError as exc:
        raise AssetFetchError(f"official.nba.com returned a non-JSON 200 body for {url}") from exc
    if not isinstance(data, dict):
        raise AssetFetchError(f"official.nba.com returned a JSON {type(data).__name__}, not an object, for {url}")
    return data


L2M_CALLS_SCHEMA = pl.Schema(
    {
        "game_id": pl.Utf8,
        # Ruling R7: Int64, matching liveData's `period` column, for period+clock joins.
        "period": pl.Int64,
        "period_name": pl.Utf8,
        "pc_time": pl.Utf8,
        "seconds_remaining": pl.Float64,
        "call_type": pl.Utf8,
        "call": pl.Utf8,
        "type": pl.Utf8,
        "committing": pl.Utf8,
        "disadvantaged": pl.Utf8,
        "decision": pl.Utf8,
        "decision_raw": pl.Utf8,
        "comment": pl.Utf8,
        "difficulty": pl.Utf8,
        "video_event_id": pl.Utf8,
        "pos_id": pl.Int64,
        "pos_start": pl.Utf8,
        "pos_end": pl.Utf8,
        "pos_team_id": pl.Int64,
    }
)
L2M_GAME_SCHEMA = pl.Schema(
    {
        "game_id": pl.Utf8,
        "game_date": pl.Date,
        "season_type": pl.Utf8,
        "home_team_id": pl.Int64,
        "away_team_id": pl.Int64,
        "home_team_abbr": pl.Utf8,
        "away_team_abbr": pl.Utf8,
        "home_team_name": pl.Utf8,
        "away_team_name": pl.Utf8,
        "home_score": pl.Int32,
        "away_score": pl.Int32,
        "l2m_comments": pl.Utf8,
    }
)
L2M_STATS_SCHEMA = pl.Schema(
    {
        "game_id": pl.Utf8,
        "stat_name": pl.Utf8,
        "home": pl.Int32,
        "away": pl.Int32,
    }
)

_DECISIONS = {"CC": "CC", "CNC": "CNC", "IC": "IC", "INC": "INC", "NCC": "CNC", "NCI": "INC"}
_SEASON_TYPES = {
    "1": "preseason",
    "2": "regular",
    "3": "all-star",
    "4": "playoffs",
    "5": "play-in",
    "6": "nba-cup-final",
}
_L2M_URL = "https://official.nba.com/l2m/json/{gid}.json"
_LISTING_URL = "https://official.nba.com/{span}-nba-officiating-last-two-minute-reports/"
# Regex, not CSS: the page's selectors changed almost every season upstream.
_LISTING_RE = re.compile(r"L2MReport\.html\?gameId=(?:%0[dD])?(\d{10})[^>]*>([^<]*)</a>")
# H3: a real listing page's <title>/<h1> always carry this phrase. An Akamai
# interstitial, a blank body, or a redesigned page do not -- and zero report
# links is otherwise a silent-but-legitimate outcome (an off-season span), so
# this marker, not the row count, is the fetch-level health check.
_LISTING_MARKER_RE = re.compile(r"last two minute", re.IGNORECASE)
_ASSIGN_URL = "https://official.nba.com/wp-json/api/v1/get-game-officials"
NBA_REFEREE_ASSIGN_SCHEMA = pl.Schema(
    {
        "league": pl.Utf8,
        "game_id": pl.Utf8,
        "game_date": pl.Date,
        "season": pl.Int32,
        "season_type": pl.Utf8,
        "game_code": pl.Utf8,
        "home_team_id": pl.Int64,
        "home_team_abbr": pl.Utf8,
        "away_team_id": pl.Int64,
        "away_team_abbr": pl.Utf8,
        "crew_position": pl.Int32,
        "official_id": pl.Int64,
        "official_name": pl.Utf8,
        "jersey_num": pl.Utf8,
    }
)
NBA_REFEREE_REPLAY_SCHEMA = pl.Schema(
    {"league": pl.Utf8, "game_date": pl.Date, "official_id": pl.Int64, "official_name": pl.Utf8}
)


def _gid(game_id: str | int) -> str:
    """Zero-pad a caller-supplied game id to 10 digits, checked before it builds a URL.

    The one id check for every fetcher here and in :mod:`sportsdataverse.nba.nba_live`.
    Accepts one non-negative integer id: an int, an integral float, or an all-digit
    string. Anything else -- a bool, a negative or fractional number, a string with a
    sign, space, underscore or other non-digit -- raises ``ValueError``; ``int()`` would
    turn it into another game's id or a nonsense URL whose 403 reads as "no data".
    """
    if isinstance(game_id, float) and game_id.is_integer():
        game_id = int(game_id)
    s = str(game_id)  # str(True) == "True", so a bool fails the digit check too
    if not (s.isascii() and s.isdigit()):
        raise ValueError(f"game_id must be a non-negative integer id, got {game_id!r}")
    return s.zfill(10)


def _as_dict(x: Any) -> dict[str, Any]:
    """*x* when it is a dict, else ``{}`` -- keeps the parsers' no-raise contract on malformed envelopes."""
    return x if isinstance(x, dict) else {}


def _records(x: Any) -> list[dict[str, Any]]:
    """The dict elements of *x* when it is a list, else ``[]``."""
    return [r for r in x if isinstance(r, dict)] if isinstance(x, list) else []


def _l2m_gid(raw: Any) -> str | None:
    """Zero-pad a payload ``GameId`` when it's all-digits; otherwise keep it verbatim.

    Guards against malformed upstream JSON (D2): unlike the strict :func:`_gid`
    (used for a caller-supplied id that builds the request URL, where a
    non-numeric id should raise), this tolerates a non-numeric ``GameId`` in the
    *response* body instead of raising ``ValueError``.

    Args:
        raw: The payload's raw ``GameId`` value (``int``, ``str``, or ``None``).

    Returns:
        The zero-padded 10-digit string when *raw* is all-digits, the verbatim
        string otherwise, or ``None`` when *raw* is ``None``.
    """
    if raw is None:
        return None
    s = str(raw)
    return s.zfill(10) if s.isdigit() else s


def _text(v: Any) -> Any:
    """One cell for :func:`_frame`'s fallback: text, nested values as JSON; dates and ``None`` kept."""
    if v is None or isinstance(v, (str, _dt.date)):
        return v
    return _json.dumps(v) if isinstance(v, (dict, list)) else str(v)


def _frame(rows: list[dict[str, Any]], schema: pl.Schema) -> pl.DataFrame:
    """Build a frame with *schema* from row dicts; no rows still carry the schema.

    Never raises on a cell of the wrong type (``""`` or ``"abc"`` in an id column, an
    object where text belongs), per the parser contract: the rows are rebuilt as text
    (nested values as JSON strings, dates kept) under a Utf8 schema and cast back, so a
    value that cannot convert becomes null. Real payloads never take that path.
    """
    if not rows:
        return pl.DataFrame(schema=schema)
    try:
        return pl.DataFrame(rows, schema=schema, strict=False)
    except (TypeError, ValueError, pl.exceptions.PolarsError):
        # ponytail: a float-typed id ("12.0") also nulls on this path; the feeds send ints.
        text = pl.Schema({k: (d if d == pl.Date else pl.Utf8) for k, d in schema.items()})
        rows = [{k: _text(v) for k, v in r.items()} for r in rows]
        return pl.DataFrame(rows, schema=text, strict=False).cast(schema, strict=False)


def parse_nba_l2m(payload: dict | None, *, return_as_pandas: bool = False) -> dict[str, Any]:
    """Parse an NBA Last Two Minute report payload into tidy DataFrames.

    Parses the raw L2M report JSON from official.nba.com into three related
    tables: calls (one row per graded play, with foul details and decisions), game
    metadata (1 row per game), and stats (3 rows per game with error counts).
    Player names and team IDs are preserved verbatim from the source; decision
    tags ("CC", "CNC", "INC") are normalized from raw values.

    Args:
        payload: The JSON payload (dict) from official.nba.com L2M endpoint, or
            ``None`` (treated as an empty payload, as is any other non-object).
        return_as_pandas: If True, return pandas DataFrames instead of polars.

    Returns:
        A dict with three keys: ``"calls"`` (L2M_CALLS_SCHEMA, 19 columns),
        ``"game"`` (L2M_GAME_SCHEMA, 12 columns), ``"stats"`` (L2M_STATS_SCHEMA, 4 columns).
        Each value is a DataFrame with the specified schema. Empty payloads return
        zero-row DataFrames.

    Raises:
        This function does not raise. An empty or malformed payload (not an object,
        rows that are not objects) yields zero-row DataFrames with the documented
        schemas; a missing field, or a cell of the wrong type, becomes null (an object
        or list in a text column is kept as its JSON text).

    Example:
        Parse a real L2M report::

            import json
            from sportsdataverse.nba.nba_officiating import parse_nba_l2m
            with open("l2m.json") as f:
                raw = json.load(f)
            result = parse_nba_l2m(raw)
            calls = result["calls"]
            print(calls.filter(calls["decision"] == "INC").shape)

        Convert to pandas::

            result = parse_nba_l2m(raw, return_as_pandas=True)
            calls_pd = result["calls"]

        See Also:
            * `hoopR`_ -- R package for NBA data access and visualization
            * `atlhawksfanatic/L2M`_ -- L2M report scraper and archive

            .. _hoopR: https://hoopR.sportsdataverse.org
            .. _atlhawksfanatic/L2M: https://github.com/atlhawksfanatic/L2M
    """
    payload = _as_dict(payload)
    game = payload.get("game")
    # A lone object is read as the one game row, as hoopR's parser does.
    game_rows = [game] if isinstance(game, dict) else _records(game)
    g = game_rows[0] if game_rows else {}
    gid = _l2m_gid(g.get("GameId"))
    calls = _frame(
        [
            {
                "game_id": gid,
                "period_name": r.get("PeriodName"),
                "pc_time": r.get("PCTime"),
                "call_type": r.get("CallType"),
                "committing": r.get("CP") or None,
                "disadvantaged": r.get("DP") or None,
                "decision_raw": r.get("CallRatingName"),
                "comment": r.get("Comment"),
                "difficulty": r.get("Difficulty"),
                "video_event_id": None if r.get("VideolLink") is None else str(r["VideolLink"]),
                "pos_id": r.get("posID"),
                "pos_start": r.get("posStart"),
                "pos_end": r.get("posEnd"),
                "pos_team_id": r.get("posTeamId"),
            }
            for r in _records(payload.get("l2m"))
        ],
        L2M_CALLS_SCHEMA,
    )
    t = pl.col("pc_time").str.replace(r"^(\d+):(\d+):(\d+)$", "${1}:${2}.${3}")
    ct = pl.col("call_type").str.replace_all(r"\s+", " ").str.strip_chars()
    calls = calls.with_columns(
        period=pl.col("period_name").str.extract(r"(\d+)", 1).cast(pl.Int64, strict=False),
        seconds_remaining=t.str.extract(r"^(\d+):", 1).cast(pl.Float64) * 60
        + t.str.extract(r":(\d+(?:\.\d+)?)\.?$", 1).cast(pl.Float64),
        call=ct.str.extract(r"^([^:]+):", 1).str.strip_chars().str.to_uppercase(),
        type=ct.str.extract(r":\s*(.+)$", 1).str.to_uppercase(),
        decision=pl.col("decision_raw")
        .str.strip_chars()
        .str.replace(r"\*$", "")
        .replace_strict(_DECISIONS, default=None, return_dtype=pl.Utf8),
    ).select(L2M_CALLS_SCHEMA.names())
    game = _frame(
        [
            {
                "game_id": gid,
                "game_date": _iso_date(g.get("GameDate")),
                # _l2m_gid keeps a non-numeric id verbatim, so it can be shorter than 3.
                "season_type": _SEASON_TYPES.get(gid[2]) if gid and len(gid) > 2 else None,
                "home_team_id": g.get("HomeTeamId"),
                "away_team_id": g.get("AwayTeamId"),
                "home_team_abbr": g.get("Home_team_abbr"),
                "away_team_abbr": g.get("Away_team_abbr"),
                "home_team_name": g.get("Home_team"),
                "away_team_name": g.get("Away_team"),
                "home_score": g.get("HomeTeamScore"),
                "away_score": g.get("VisitorTeamScore"),
                "l2m_comments": g.get("L2M_Comments"),
            }
        ]
        if g
        else [],
        L2M_GAME_SCHEMA,
    )
    stats = _frame(
        [
            {
                "game_id": gid,
                "stat_name": s.get("stats_name"),
                "home": s.get("home"),
                "away": s.get("away"),
            }
            for s in _records(payload.get("stats"))
        ],
        L2M_STATS_SCHEMA,
    )
    out = {"calls": calls, "game": game, "stats": stats}
    return {k: v.to_pandas() for k, v in out.items()} if return_as_pandas else out


def nba_l2m(
    game_id: str | int, *, raw: bool = False, return_as_pandas: bool = False, proxy: dict | None = None
) -> dict[str, Any]:
    """Fetch and parse an NBA Last Two Minute report from official.nba.com.

    Retrieves the L2M report for a given game and returns parsed tables of calls,
    game metadata, and error statistics. The game_id is zero-padded to 10 digits
    (e.g., 42500405 becomes "0042500405"). A report is published for any game that
    is within 3 points (5 points before the 2017-18 season) at any point during the
    last two minutes of the fourth quarter or overtime -- not only playoff games.

    Args:
        game_id: NBA game ID (can be int or str). Automatically zero-padded to 10 digits.
        raw: If True, return the raw JSON payload (dict) instead of parsed DataFrames.
        return_as_pandas: If True, return pandas DataFrames instead of polars.
        proxy: Optional proxy dict passed through to the HTTP layer.

    Returns:
        If ``raw=True``, the raw JSON dict. Otherwise, a dict with keys ``"calls"``,
        ``"game"``, ``"stats"`` mapping to DataFrames as documented in
        :func:`parse_nba_l2m`.

    Raises:
        ValueError: ``game_id`` is not one non-negative integer id -- a bool, a
            negative or fractional number, or a string that is not all digits
            (checked before any request).
        NoDataError: The game has no L2M report (a 404, or S3's ``AccessDenied``
            403). Only games within 3 points (5 before 2017-18) at some point in the
            last two minutes of the fourth quarter or overtime get one, so most games
            have none; a game that just ended may not be graded yet.
        AssetFetchError: The fetch failed (a transport error, a rate limit, an
            Akamai WAF block, or a 5xx that outlived the retry budget), or the 200
            body is not a JSON object whose ``game`` is exactly one row -- an error
            envelope or a changed schema, checked with ``raw=True`` too.

    Example:
        Fetch an L2M report for a playoff game::

            from sportsdataverse.nba.nba_officiating import nba_l2m
            result = nba_l2m("0042500405")
            calls = result["calls"]
            print(f"Game had {calls.height} tracked plays in the L2M window")

        Parse as pandas instead::

            result = nba_l2m("0042500405", return_as_pandas=True)
            calls_pd = result["calls"]

        Access raw JSON::

            payload = nba_l2m("0042500405", raw=True)
            print(payload["game"])

        See Also:
            * `hoopR`_ -- R package for NBA data access and visualization
            * `atlhawksfanatic/L2M`_ -- L2M report scraper and archive

            .. _hoopR: https://hoopR.sportsdataverse.org
            .. _atlhawksfanatic/L2M: https://github.com/atlhawksfanatic/L2M
    """
    gid = _gid(game_id)
    url = _L2M_URL.format(gid=gid)
    payload = _official_json(_official_get(url, proxy=proxy), url)
    # Every report carries exactly one game row. Anything else is an error envelope or
    # a changed schema -- a failed fetch, never an empty report -- and a second row
    # would otherwise be dropped silently by the parser, which reads the first. Checked
    # before the raw return too, so a capture job never stores it.
    game = payload.get("game")
    if not (isinstance(game, list) and len(game) == 1 and isinstance(game[0], dict)):
        raise AssetFetchError(f"official.nba.com returned an L2M body without exactly one game row for {url}")
    # ... and that row must be the game asked for: a missing id, or a cached or
    # misrouted payload for another game, is a failed fetch, not this game's report.
    got = _l2m_gid(game[0].get("GameId"))
    if got != gid:
        raise AssetFetchError(
            f"official.nba.com returned the L2M report for game {got!r} when {gid} was requested ({url})"
        )
    return payload if raw else parse_nba_l2m(payload, return_as_pandas=return_as_pandas)


def parse_nba_l2m_games(
    html: str | bytes | None, season: int, *, return_as_pandas: bool = False
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Parse an NBA season's Last Two Minute games listing from the HTML index page.

    Extracts game IDs and matchup labels from the L2M season index page
    (e.g., official.nba.com/2025-26-nba-officiating-last-two-minute-reports/).
    Duplicates are deduplicated, keeping the first occurrence. The output is ordered
    by appearance on the page.

    Args:
        html: The HTML content of the L2M season listing page. Bytes are decoded as
            UTF-8; ``None`` is read as an empty page.
        season: The NBA season (end year), used to populate the ``season`` column.
        return_as_pandas: If True, return a pandas DataFrame instead of polars.

    Returns:
        A DataFrame with schema ``{"game_id": Utf8, "season": Int32, "season_type": Utf8, "label": Utf8}``,
        one row per unique game ID in page order. If no report links are found, returns
        a zero-row frame with the documented schema.

    Raises:
        This function does not raise. HTML with no report links (or no HTML) yields a
        zero-row frame with the documented schema.

    Example:
        Parse a real season listing::

            from sportsdataverse.nba.nba_officiating import parse_nba_l2m_games
            with open("l2m_listing_2025-26.html") as f:
                html = f.read()
            df = parse_nba_l2m_games(html, 2026)
            print(df.filter(df["season_type"] == "playoffs").height)

        See Also:
            * `hoopR`_ -- R package for NBA data access and visualization
            * `atlhawksfanatic/L2M`_ -- L2M report scraper and archive

            .. _hoopR: https://hoopR.sportsdataverse.org
            .. _atlhawksfanatic/L2M: https://github.com/atlhawksfanatic/L2M
    """
    if isinstance(html, bytes):
        html = html.decode("utf-8", "replace")
    seen: dict[str, str] = {}
    for gid, label in _LISTING_RE.findall(html if isinstance(html, str) else ""):
        seen.setdefault(gid, label.strip())
    df = pl.DataFrame(
        {
            "game_id": list(seen),
            "season": [season] * len(seen),
            "season_type": [_SEASON_TYPES.get(g[2]) for g in seen],
            "label": list(seen.values()),
        },
        schema={"game_id": pl.Utf8, "season": pl.Int32, "season_type": pl.Utf8, "label": pl.Utf8},
    )
    return df.to_pandas() if return_as_pandas else df


@overload
def nba_l2m_games(
    season: int | str, *, return_as_pandas: Literal[False] = False, proxy: dict | None = None
) -> pl.DataFrame: ...


@overload
def nba_l2m_games(season: int | str, *, return_as_pandas: Literal[True], proxy: dict | None = None) -> pd.DataFrame: ...


def nba_l2m_games(
    season: int | str, *, return_as_pandas: bool = False, proxy: dict | None = None
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Fetch the list of games with Last Two Minute reports for an NBA season.

    Retrieves and parses the L2M season index page from official.nba.com,
    returning a table of all games for which L2M reports exist. JSON reports
    exist only from 2019-01-01 onward; earlier seasons' index pages list PDFs,
    which this function ignores. A release loader for historical (PDF-era)
    reports is planned but does not exist yet.

    Args:
        season: The NBA season (end year) as a 4-digit int or string, e.g. 2026 for
            the 2025-26 season.
        return_as_pandas: If True, return a pandas DataFrame instead of polars.
        proxy: Optional proxy dict passed through to the HTTP layer.

    Returns:
        A :class:`polars.DataFrame` (or :class:`pandas.DataFrame` when
        ``return_as_pandas=True``) with schema
        ``{"game_id": Utf8, "season": Int32, "season_type": Utf8, "label": Utf8}``,
        one row per unique game ID in page order.

    Raises:
        ValueError: ``season`` is not a 4-digit year (an int or an all-digit
            string), checked before any request.
        NoDataError: official.nba.com has no listing page for that season (a 404,
            or S3's ``AccessDenied`` 403) -- e.g. a future season, or one from
            before the L2M program began.
        AssetFetchError: The fetch failed (network error, rate limit, or Akamai
            WAF block), or a 200 response that is missing the expected "Last Two
            Minute" page marker (an Akamai interstitial, a blank body, or a
            redesigned page), or that links reports in a shape the parser does not
            read (a redesigned page) -- checked here, not in
            :func:`parse_nba_l2m_games`, so the parser itself never raises.

    Example:
        Fetch the 2025-26 season L2M games::

            from sportsdataverse.nba.nba_officiating import nba_l2m_games
            df = nba_l2m_games(2026)
            print(f"Season had {df.height} games with L2M reports")

        Get playoff games only::

            df = nba_l2m_games(2026)
            playoffs = df.filter(df["season_type"] == "playoffs")

        Convert to pandas::

            df = nba_l2m_games(2026, return_as_pandas=True)

        See Also:
            * `hoopR`_ -- R package for NBA data access and visualization
            * `atlhawksfanatic/L2M`_ -- L2M report scraper and archive

            .. _hoopR: https://hoopR.sportsdataverse.org
            .. _atlhawksfanatic/L2M: https://github.com/atlhawksfanatic/L2M
    """
    # str(True) is "True" and str(2026.0) is "2026.0", so both fail here too.
    if not re.fullmatch(r"[0-9]{4}", str(season)):
        raise ValueError(f"season must be a 4-digit end year such as 2026, got {season!r}")
    year = int(season)
    span = f"{year - 1}-{str(year)[-2:]}"
    url = _LISTING_URL.format(span=span)
    html = _official_get(url, proxy=proxy).text
    if not _LISTING_MARKER_RE.search(html):
        raise AssetFetchError(
            f"official.nba.com L2M listing page for {span} is missing the expected "
            f"'Last Two Minute' marker (Akamai interstitial, blank body, or a "
            f"redesigned page) at {url}"
        )
    # Report links that the regex cannot read mean a redesign, not an empty season.
    if "L2MReport" in html and not _LISTING_RE.search(html):
        raise AssetFetchError(
            f"official.nba.com L2M listing page for {span} links reports in an "
            f"unrecognized shape (a redesigned page) at {url}"
        )
    return parse_nba_l2m_games(html, year, return_as_pandas=return_as_pandas)


def _mdy(s: Any) -> _dt.date | None:
    """Parse a date string in MM/DD/YYYY format; ``None`` when missing, not a string, or malformed (the parser contract)."""
    if not isinstance(s, str) or not s:
        return None
    try:
        return _dt.datetime.strptime(s, "%m/%d/%Y").date()
    except ValueError:
        return None


def _iso_date(s: Any) -> _dt.date | None:
    """Parse the date part of an ISO date/datetime string; ``None`` when missing or malformed."""
    try:
        return _dt.date.fromisoformat(str(s)[:10]) if s else None
    except ValueError:
        return None


def _season_end_year(s: str, league: str) -> int | None:
    """Convert feed season format <type digit><START year> to END year per league.

    NBA and G-League play two-calendar-year seasons (e.g., 2025-26), so season code
    2025 represents the 2026 end year. WNBA plays single-year seasons, so season code
    2026 represents the 2026 season.
    """
    if len(s) != 5 or not s[1:].isdigit():
        return None
    start_year = int(s[1:])
    return start_year + 1 if league in ("nba", "gl") else start_year


def _table_rows(block: Any, table: str) -> list[dict[str, Any]]:
    """The row dicts of ``block[table]["rows"]``; any malformed level yields ``[]`` (parser contract)."""
    t = block.get(table) if isinstance(block, dict) else None
    return _records(t.get("rows") if isinstance(t, dict) else None)


def parse_nba_referee_assignments(
    payload: dict, league: str = "nba", *, return_as_pandas: bool = False
) -> dict[str, Any]:
    """Parse NBA referee assignment payload into tidy DataFrames.

    Parses the raw referee assignment JSON from official.nba.com into two related
    tables: officials (long format, one row per game × filled crew slot -- an empty
    slot yields no row) and replay center (one row per replay-center official per
    date per league; the feed ties these to a date, not a game, so there is no game key).

    The ``crew_position`` column contains the feed's slot order (1–4); slot 1 is
    inferred to be the crew chief from that order, as the NBA does not label roles
    in the API. The ``season`` column converts the feed's format ``<type digit><START year>``
    to an END year: START+1 for NBA/G-League (two-calendar-year seasons) and START
    unchanged for WNBA (single-year seasons).

    Args:
        payload: The JSON payload (dict) from official.nba.com referee assignments endpoint.
        league: The league to extract ("nba", "gl", or "wnba"). Defaults to "nba".
        return_as_pandas: If True, return pandas DataFrames instead of polars.

    Returns:
        A dict with two keys: ``"officials"`` (NBA_REFEREE_ASSIGN_SCHEMA, 14 columns) and
        ``"replay_center"`` (NBA_REFEREE_REPLAY_SCHEMA, 4 columns). Each value is a DataFrame
        with the specified schema. Empty payloads return zero-row DataFrames.

    Raises:
        ValueError: If an unknown league is specified. Nothing else raises: a malformed
            payload or row is skipped, and a cell of the wrong type (a non-string date,
            ``""`` in an id column, an object where text belongs) becomes null.

    Example:
        Parse referee assignments::

            import json
            from sportsdataverse.nba.nba_officiating import parse_nba_referee_assignments
            with open("assignments.json") as f:
                raw = json.load(f)
            result = parse_nba_referee_assignments(raw, "nba")
            officials = result["officials"]
            print(f"Found {officials.height} official slots across games")

        See Also:
            * `hoopR`_ -- R package for NBA data access and visualization

            .. _hoopR: https://hoopR.sportsdataverse.org
    """
    if league not in ("nba", "gl", "wnba"):
        raise ValueError(f"league must be 'nba', 'gl' or 'wnba', got {league!r}")
    block = payload.get(league) if isinstance(payload, dict) else None
    rows = []
    for g in _table_rows(block, "Table"):
        s = str(g.get("season") or "")
        for k in range(1, 5):
            if not g.get(f"official{k}"):
                continue
            rows.append(
                {
                    "league": league,
                    "game_id": _l2m_gid(g.get("game_id")),
                    "game_date": _mdy(g.get("game_date")),
                    "season": _season_end_year(s, league),
                    "season_type": _SEASON_TYPES.get(s[:1]),
                    "game_code": g.get("game_code"),
                    "home_team_id": g.get("home_team_id"),
                    "home_team_abbr": g.get("home_team_abbr"),
                    "away_team_id": g.get("away_team_id"),
                    "away_team_abbr": g.get("away_team_abbr"),
                    "crew_position": k,
                    "official_id": g.get(f"official{k}_code"),
                    "official_name": g.get(f"official{k}"),
                    "jersey_num": g.get(f"official{k}_JNum"),
                }
            )
    replay = [
        {
            "league": league,
            "game_date": _mdy(r.get("game_date")),
            "official_id": r.get("official_code"),
            "official_name": r.get("replaycenter_official"),
        }
        for r in _table_rows(block, "Table1")
    ]
    out = {
        "officials": _frame(rows, NBA_REFEREE_ASSIGN_SCHEMA),
        "replay_center": _frame(replay, NBA_REFEREE_REPLAY_SCHEMA),
    }
    return {k: v.to_pandas() for k, v in out.items()} if return_as_pandas else out


def nba_referee_assignments(
    date: str | _dt.date,
    *,
    league: str = "nba",
    raw: bool = False,
    return_as_pandas: bool = False,
    proxy: dict | None = None,
) -> dict[str, Any]:
    """Fetch and parse NBA referee assignments for a given date from official.nba.com.

    Retrieves one league's referee crew assignments and replay-center officials for a
    date: NBA, G-League or WNBA, picked by ``league`` (``raw=True`` returns all three
    leagues' blocks). The ``crew_position`` column (1–4) represents the feed's slot
    order; slot 1 is inferred to be the crew chief. The ``season`` column converts
    from the feed's format to an END year: START+1 for NBA/G-League
    (two-calendar-year seasons) and START unchanged for WNBA.

    Args:
        date: The date to fetch assignments for (str in "YYYY-MM-DD" format or datetime.date).
        league: The league to extract ("nba", "gl", or "wnba"). Defaults to "nba".
        raw: If True, return the raw JSON payload (dict) with all three leagues instead of parsed DataFrames.
        return_as_pandas: If True, return pandas DataFrames instead of polars.
        proxy: Optional proxy dict passed through to the HTTP layer.

    Returns:
        If ``raw=True``, the raw JSON dict with keys "nba", "gl", "wnba". Otherwise,
        a dict with keys ``"officials"`` and ``"replay_center"`` mapping to DataFrames
        as documented in :func:`parse_nba_referee_assignments`.

    Raises:
        ValueError: If league is not "nba", "gl", or "wnba", or date is not a valid
            "YYYY-MM-DD" date (checked before any request).
        NoDataError: official.nba.com answered 404, or 403 with S3's ``AccessDenied``
            body. A date without games is not this: it comes back as a 200 with empty
            ``rows`` (see the Note).
        AssetFetchError: The fetch failed (network error, rate limit, or Akamai WAF
            block), the 200 body is not a JSON object, or the league's
            ``Table``/``Table1`` rows are missing or malformed (a row without
            ``game_id`` included). With ``raw=True`` all three leagues are checked,
            since the whole payload is returned.

    Note:
        A date with no games for the requested league is not an error -- the endpoint
        always returns a 200 with an empty ``rows`` list for that league's block, so
        ``result["officials"]`` and ``result["replay_center"]`` come back as zero-row
        DataFrames rather than raising ``NoDataError``. Because every league's block is
        always present, a response without it is treated as a failed fetch.

    Example:
        Fetch referee assignments for a date::

            from sportsdataverse.nba.nba_officiating import nba_referee_assignments
            result = nba_referee_assignments("2026-06-13")
            officials = result["officials"]
            print(f"Found {officials.height} official slots")

        Fetch WNBA assignments for the same date::

            result = nba_referee_assignments("2026-06-13", league="wnba")
            wnba_officials = result["officials"]

        See Also:
            * `hoopR`_ -- R package for NBA data access and visualization

            .. _hoopR: https://hoopR.sportsdataverse.org
    """
    if league not in ("nba", "gl", "wnba"):
        raise ValueError(f"league must be 'nba', 'gl' or 'wnba', got {league!r}")
    # datetime.datetime (and pandas.Timestamp, a subclass) is-a datetime.date, so check
    # the more specific type first -- otherwise isoformat() keeps the time-of-day and
    # the URL becomes `date=2026-06-13T19:30:00` instead of `date=2026-06-13`.
    if isinstance(date, _dt.datetime):
        day = date.date().isoformat()
    elif isinstance(date, _dt.date):
        day = date.isoformat()
    else:
        day = str(date)
    # fullmatch pins the shape (3.11+ fromisoformat also accepts "20260613");
    # fromisoformat rejects impossible dates such as "2026-02-31".
    try:
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", day):
            raise ValueError(day)
        _dt.date.fromisoformat(day)
    except ValueError:
        raise ValueError(f"date must be a valid 'YYYY-MM-DD' date, got {date!r}") from None
    resp = _official_get(_ASSIGN_URL, params={"date": day}, proxy=proxy)
    payload = _official_json(resp, f"{_ASSIGN_URL}?date={day}")

    # The feed carries nba, gl and wnba blocks, each with Table and Table1 holding a
    # ``rows`` list, on every date (zero rows on a day without games), so anything
    # else is an error envelope or a changed schema -- never an empty day. Checked
    # before the raw return too, so a raw capture never stores an error envelope.
    def _valid(lg: str) -> bool:
        b = payload.get(lg)
        return (
            isinstance(b, dict)
            and all(
                isinstance(b.get(t), dict)
                and isinstance(b[t].get("rows"), list)
                and all(isinstance(r, dict) for r in b[t]["rows"])
                for t in ("Table", "Table1")
            )
            # A real game id, 10 digits once a numeric id is zero-padded (the
            # documented game_id contract): "not-an-id" or an 11-digit value is not.
            and all(re.fullmatch(r"\d{10}", _l2m_gid(r.get("game_id")) or "") for r in b["Table"]["rows"])
        )

    # raw=True hands back all three leagues, so all three must be well-formed.
    bad = [lg for lg in (("nba", "gl", "wnba") if raw else (league,)) if not _valid(lg)]
    if bad:
        raise AssetFetchError(
            f"official.nba.com referee assignments for {day}: malformed or missing {bad} Table/Table1 rows"
        )
    if raw:
        return payload  # full three-league {nba, gl, wnba} payload
    return parse_nba_referee_assignments(payload, league, return_as_pandas=return_as_pandas)
