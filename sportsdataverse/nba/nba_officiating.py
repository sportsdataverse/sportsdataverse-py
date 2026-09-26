"""NBA officiating data from official.nba.com: Last Two Minute reports and referee assignments.

Port of the scraping logic in atlhawksfanatic/L2M (MIT, (c) 2019 atlhawksfanatic).
official.nba.com is S3 behind Akamai Bot Manager: a browser User-Agent is required, and a
403 means two different things -- an S3 XML ``AccessDenied`` body is "no such report"
(``NoDataError``) while an Akamai HTML page is a blocked fetch (``AssetFetchError``).
"""

from __future__ import annotations

import datetime as _dt
from typing import Any

import polars as pl
import requests

from sportsdataverse.dl_utils import download
from sportsdataverse.errors import AssetFetchError, NoDataError

__all__ = ["parse_nba_l2m", "nba_l2m", "L2M_CALLS_SCHEMA", "L2M_GAME_SCHEMA", "L2M_STATS_SCHEMA"]

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


def _official_get(url: str, *, proxy: dict | None = None) -> requests.Response:
    """Fetch a URL from ``official.nba.com`` with the browser UA it requires, classifying 403s.

    Args:
        url: Full ``official.nba.com`` URL to fetch.
        proxy: Optional proxy dict passed through to
            :func:`sportsdataverse.dl_utils.download`.

    Returns:
        The successful (200) ``requests.Response``.

    Raises:
        NoDataError: The response is a 403 with an S3 ``AccessDenied`` XML body --
            official.nba.com's way of saying no report exists for the request.
        AssetFetchError: The response is any other non-200 status, including an
            Akamai WAF block (403 HTML) or a 5xx that outlived the retry budget.

    Example:
        Fetch a Last Two Minute report::

            from sportsdataverse.nba.nba_officiating import _official_get
            resp = _official_get("https://official.nba.com/l2m/json/0042500405.json")
            payload = resp.json()
    """
    resp = download(url, headers=_OFFICIAL_HEADERS, proxy=proxy, retry_statuses=_RETRY_NO_403)
    if resp.status_code == 200:
        return resp
    if resp.status_code == 403 and "<Code>AccessDenied</Code>" in resp.text[:500]:
        raise NoDataError(f"official.nba.com has no object at {url}")
    ctype = resp.headers.get("Content-Type", "")
    raise AssetFetchError(f"official.nba.com fetch failed ({resp.status_code}, {ctype!r}) for {url}")


L2M_CALLS_SCHEMA = pl.Schema(
    {
        "game_id": pl.Utf8,
        "period": pl.Int32,
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


def _gid(game_id: str | int) -> str:
    """Zero-pad game ID to 10 digits."""
    return str(int(game_id)).zfill(10)


def _frame(rows: list[dict[str, Any]], schema: pl.Schema) -> pl.DataFrame:
    """Create a polars DataFrame from rows with explicit schema; empty rows carry schema."""
    if not rows:
        return pl.DataFrame(schema=schema)
    return pl.DataFrame(rows, schema=schema, strict=False)


def parse_nba_l2m(payload: dict, *, return_as_pandas: bool = False) -> dict[str, Any]:
    """Parse an NBA Last Two Minute report payload into tidy DataFrames.

    Parses the raw L2M report JSON from official.nba.com into three related
    tables: calls (21+ rows per game with foul details and decisions), game
    metadata (1 row per game), and stats (3 rows per game with error counts).
    Player names and team IDs are preserved verbatim from the source; decision
    tags ("CC", "CNC", "INC") are normalized from raw values.

    Args:
        payload: The JSON payload (dict) from official.nba.com L2M endpoint.
        return_as_pandas: If True, return pandas DataFrames instead of polars.

    Returns:
        A dict with three keys: ``"calls"`` (L2M_CALLS_SCHEMA, 19 columns),
        ``"game"`` (L2M_GAME_SCHEMA, 12 columns), ``"stats"`` (L2M_STATS_SCHEMA, 4 columns).
        Each value is a DataFrame with the specified schema. Empty payloads return
        zero-row DataFrames.

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
    game_rows = payload.get("game") or []
    g = game_rows[0] if game_rows else {}
    gid = _gid(g["GameId"]) if g.get("GameId") else None
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
            for r in payload.get("l2m") or []
        ],
        L2M_CALLS_SCHEMA,
    )
    t = pl.col("pc_time").str.replace(r"^(\d+):(\d+):(\d+)$", "${1}:${2}.${3}")
    ct = pl.col("call_type").str.replace_all(r"\s+", " ").str.strip_chars()
    calls = calls.with_columns(
        period=pl.col("period_name").str.extract(r"(\d+)", 1).cast(pl.Int32),
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
                "game_date": _dt.date.fromisoformat(g["GameDate"][:10]) if g.get("GameDate") else None,
                "season_type": _SEASON_TYPES.get(gid[2]) if gid else None,
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
            for s in payload.get("stats") or []
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
    (e.g., 42500405 becomes "0042500405"). Reports are typically available only for
    NBA and playoff games during the last two minutes.

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
        NoDataError: The game has no L2M report (common for regular-season games,
            games that did not reach the final two minutes, or very recent games).
        AssetFetchError: The fetch failed (network error, rate limit, or Akamai
            WAF block).

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
    payload = _official_get(_L2M_URL.format(gid=_gid(game_id)), proxy=proxy).json()
    return payload if raw else parse_nba_l2m(payload, return_as_pandas=return_as_pandas)
