"""sportsdataverse.mlb.mlb_statcast — wrappers for the Baseball Savant / Statcast surface.

**Documentation**:

* Statcast endpoint + 25,000-row truncation guide: https://py.sportsdataverse.org/docs/mlb/statcast
* MLB module overview: https://py.sportsdataverse.org/docs/mlb/

Host: ``baseballsavant.mlb.com``. Spec: ``sdv-internal-refs/mlb/statcast-api.openapi.yaml``.
Sister module for the MLB Stats API at ``statsapi.mlb.com``:
:mod:`sportsdataverse.mlb.mlb_api`.

Three deliverable surfaces:

* :func:`statcast_search` — pitch-by-pitch search (``/statcast_search/csv``).
* :func:`statcast_<leaderboard>` — 9 named leaderboards (xStats, sprint speed,
  outs-above-average, catch probability, arm strength, bat tracking, pop time,
  pitch-arsenal stats, plus the custom builder).
* :func:`statcast_gamefeed` — per-game Statcast JSON feed at ``/gf``.

Gotchas the wrapper handles for you
-----------------------------------

1. **25,000-row truncation**: ``/statcast_search/csv`` caps results at 25,000
   rows per response **with no pagination**. :func:`statcast_search` returns the
   raw CSV; :func:`statcast_search_chunked` auto-splits a date range into
   smaller windows and re-stitches client-side. A response of exactly 25,000
   rows is treated as truncated.
2. **Pipe-separated params with trailing pipes**: Savant filters like
   ``hfSea`` expect ``"2024|2025|"`` (trailing ``|``). The helper
   :func:`_pipe` normalizes any list-of-strings to that shape.
3. **Required-but-may-be-empty params**: Savant returns 500 if some params
   (e.g. ``hfPT``) are omitted entirely but accepts them empty.
   :func:`statcast_search` sends every known filter, defaulting to ``""``
   when not provided.
4. **Player IDs are shared with the Stats API** (MLBAM ids). A ``personId``
   from :mod:`mlb_api` is the same as ``batter`` / ``pitcher`` here.
"""

from __future__ import annotations

from io import StringIO
from typing import Dict, Iterable, Optional, Union

import polars as pl

from sportsdataverse.dl_utils import download

_SAVANT_BASE = "https://baseballsavant.mlb.com"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _pipe(values: Optional[Union[str, Iterable[str]]]) -> str:
    """Format a value (or iterable) as a pipe-separated Savant filter string.

    Always returns a string ending in ``|`` (Savant's trailing-pipe convention).
    A ``None`` or empty input returns ``""``. A single scalar returns ``"x|"``.
    """
    if values is None:
        return ""
    if isinstance(values, str):
        if not values:
            return ""
        return values if values.endswith("|") else values + "|"
    parts = [str(v) for v in values if v is not None and v != ""]
    if not parts:
        return ""
    return "|".join(parts) + "|"


def _get_json(path: str, params: Optional[dict] = None, **kwargs) -> Dict:
    """GET ``{_SAVANT_BASE}{path}`` as JSON. Returns ``{}`` on failure."""
    clean = {k: v for k, v in (params or {}).items() if v is not None}
    url = f"{_SAVANT_BASE}{path}"
    resp = download(url=url, params=clean, **kwargs)
    if resp is None:
        return {}
    try:
        return resp.json()
    except Exception:
        return {}


def _get_csv(path: str, params: Optional[dict] = None, return_as_pandas: bool = False, **kwargs):
    """GET ``{_SAVANT_BASE}{path}`` as CSV. Returns a polars DataFrame (or pandas)."""
    clean = {k: v for k, v in (params or {}).items() if v is not None}
    url = f"{_SAVANT_BASE}{path}"
    resp = download(url=url, params=clean, **kwargs)
    if resp is None:
        return None
    text = resp.text if hasattr(resp, "text") else resp.content.decode("utf-8", errors="replace")
    if not text.strip():
        return None
    try:
        df = pl.read_csv(StringIO(text))
    except Exception:
        return None
    return df.to_pandas() if return_as_pandas else df


# ---------------------------------------------------------------------------
# /statcast_search/csv  — pitch-by-pitch search
# ---------------------------------------------------------------------------

#: Sentinel value used by callers that intend to send a Savant filter empty
#: (e.g. ``hfPT=""``) rather than omitting it. Equivalent to passing ``""``.
STATCAST_EMPTY = ""


def statcast_search(
    start_date: str,
    end_date: str,
    *,
    player_type: str = "batter",
    season: Optional[Union[str, Iterable[str]]] = None,
    game_type: Optional[Union[str, Iterable[str]]] = None,
    batters_lookup: Optional[Union[int, Iterable[int]]] = None,
    pitchers_lookup: Optional[Union[int, Iterable[int]]] = None,
    team: Optional[str] = None,
    opponent: Optional[str] = None,
    home_road: Optional[str] = None,
    stadium: Optional[Union[int, str]] = None,
    pitcher_throws: Optional[str] = None,
    batter_stands: Optional[str] = None,
    position: Optional[Union[str, Iterable[str]]] = None,
    pitch_type: Optional[Union[str, Iterable[str]]] = None,
    count: Optional[Union[str, Iterable[str]]] = None,
    at_bat_result: Optional[Union[str, Iterable[str]]] = None,
    batted_ball_type: Optional[Union[str, Iterable[str]]] = None,
    pitch_result: Optional[Union[str, Iterable[str]]] = None,
    zone: Optional[Union[str, Iterable[str]]] = None,
    outs: Optional[Union[int, Iterable[int]]] = None,
    inning: Optional[Union[int, Iterable[int]]] = None,
    runners_on: Optional[Union[str, Iterable[str]]] = None,
    flag: Optional[Union[str, Iterable[str]]] = None,
    return_as_pandas: bool = False,
    raise_on_truncation: bool = True,
    **kwargs,
):
    """GET /statcast_search/csv — pitch-by-pitch Statcast search.

    Returns a polars DataFrame of pitches matching the filter set. The Savant
    endpoint caps results at **25,000 rows per response with no pagination**;
    if the wrapper detects exactly 25,000 rows in the response and
    ``raise_on_truncation=True`` (default), it raises :class:`RuntimeError`
    rather than silently returning a partial frame. Use
    :func:`statcast_search_chunked` for date ranges that may exceed 25k pitches.

    Most filter args accept either a scalar or an iterable; the wrapper joins
    iterables with Savant's trailing-pipe convention (e.g. ``["FF","SL"]`` →
    ``"FF|SL|"``).

    Args:
        start_date / end_date: ``YYYY-MM-DD``. Inclusive.
        player_type: ``"batter"`` (default) or ``"pitcher"`` — controls which
            side of the matchup ``batters_lookup`` / ``pitchers_lookup`` /
            ``team`` filters apply to.
        season / game_type: pipe-list filters (e.g. ``["2024","2025"]``,
            ``["R","F"]``).
        batters_lookup / pitchers_lookup: list of MLBAM ids.
        team / opponent: 3-letter team codes (e.g. ``"NYY"``).
        home_road: ``"home"`` / ``"road"``.
        stadium: venue id.
        pitch_type: pipe-list of pitch codes (``"FF","SL","CU","CH","SI","FC"``…).
        count: pipe-list of pitcher–batter counts (e.g. ``["00","11"]``).
        at_bat_result: pipe-list of PA outcomes (``"single","home_run","walk"``…).
        batted_ball_type: ``"fly_ball","ground_ball","line_drive","popup"``.
        pitch_result: ``"called_strike","ball","swinging_strike","foul",…``.
        zone: gameday zone (``1``–``14``).
        outs / inning: pipe-list of int.
        runners_on: ``"none","on_first","on_second","on_third","RISP"``…
        flag: special flags (``"is_barrel","is_solidcontact","is_putaway"``…).
        return_as_pandas: convert the returned polars frame to pandas.
        raise_on_truncation: when True (default), raise if the response has
            exactly 25,000 rows.

    Returns:
        polars.DataFrame (or pandas if ``return_as_pandas=True``) with one row
        per pitch, ~90 columns covering pitch tracking, batted-ball metrics,
        Statcast outcomes, and game/play context.
    """
    params = {
        "all": "true",
        "type": "details",
        "min_pitches": 0,
        "min_results": 0,
        "group_by": "name",
        "sort_col": "pitches",
        "player_event_sort": "h_launch_speed",
        "sort_order": "desc",
        "min_pas": 0,
        "game_date_gt": start_date,
        "game_date_lt": end_date,
        "player_type": player_type,
        "hfSea": _pipe(season),
        "hfGT": _pipe(game_type),
        "team": team or "",
        "opponent": opponent or "",
        "home_road": home_road or "",
        "stadium": stadium if stadium is not None else "",
        "pitcher_throws": pitcher_throws or "",
        "batter_stands": batter_stands or "",
        "position": _pipe(position),
        "hfPT": _pipe(pitch_type),
        "hfC": _pipe(count),
        "hfAB": _pipe(at_bat_result),
        "hfBBT": _pipe(batted_ball_type),
        "hfPR": _pipe(pitch_result),
        "hfZ": _pipe(zone),
        "hfOuts": _pipe(outs),
        "hfInn": _pipe(inning),
        "hfRO": _pipe(runners_on),
        "hfFlag": _pipe(flag),
    }
    if batters_lookup is not None:
        # The Savant param literally is `batters_lookup[]`.
        ids = batters_lookup if isinstance(batters_lookup, (list, tuple)) else [batters_lookup]
        params["batters_lookup[]"] = [str(i) for i in ids]
    if pitchers_lookup is not None:
        ids = pitchers_lookup if isinstance(pitchers_lookup, (list, tuple)) else [pitchers_lookup]
        params["pitchers_lookup[]"] = [str(i) for i in ids]

    df = _get_csv("/statcast_search/csv", params=params, return_as_pandas=return_as_pandas, **kwargs)
    if df is None:
        return None
    n = len(df) if hasattr(df, "__len__") else df.height
    if n == 25_000 and raise_on_truncation:
        raise RuntimeError(
            f"statcast_search returned exactly 25,000 rows for "
            f"{start_date}..{end_date} — Savant truncates at 25k with no "
            f"pagination. Use statcast_search_chunked() or narrow the filter.",
        )
    return df


def statcast_search_chunked(
    start_date: str,
    end_date: str,
    *,
    chunk_days: int = 5,
    return_as_pandas: bool = False,
    **kwargs,
):
    """Auto-chunk a date range into ``chunk_days``-day windows and concatenate.

    Wraps :func:`statcast_search` and stitches results client-side. Useful for
    multi-month or full-season pulls that would exceed the 25k row cap in a
    single request.

    Args:
        start_date / end_date: ``YYYY-MM-DD`` (inclusive).
        chunk_days: window size in days (default 5 — typical for the regular
            season; smaller for postseason when there are more high-event games).
        return_as_pandas: convert the concatenated frame to pandas.

    All other ``**kwargs`` are forwarded to :func:`statcast_search`. Each chunk
    runs with ``raise_on_truncation=True`` so a single chunk hitting the cap
    surfaces an error rather than silently undercounting.

    Returns:
        polars.DataFrame (or pandas) of all pitches in the range.
    """
    import datetime as _dt

    s = _dt.date.fromisoformat(start_date)
    e = _dt.date.fromisoformat(end_date)
    frames = []
    cur = s
    while cur <= e:
        chunk_end = min(cur + _dt.timedelta(days=chunk_days - 1), e)
        frame = statcast_search(cur.isoformat(), chunk_end.isoformat(), return_as_pandas=False, **kwargs)
        if frame is not None and (frame.height if hasattr(frame, "height") else len(frame)) > 0:
            frames.append(frame)
        cur = chunk_end + _dt.timedelta(days=1)
    if not frames:
        return None
    out = pl.concat(frames, how="vertical_relaxed")
    return out.to_pandas() if return_as_pandas else out


# ---------------------------------------------------------------------------
# /leaderboard/* — Statcast leaderboards
# ---------------------------------------------------------------------------


def _leaderboard(path: str, params: Optional[dict] = None, csv: bool = False, return_as_pandas: bool = False, **kwargs):
    """Internal: fetch a Savant leaderboard, JSON by default, CSV if ``csv=True``."""
    p = dict(params or {})
    if csv:
        p["csv"] = "true"
        return _get_csv(path, params=p, return_as_pandas=return_as_pandas, **kwargs)
    return _get_json(path, params=p, **kwargs)


def statcast_leaderboard_custom(
    year: Union[int, str],
    type_: str,
    selections: str,
    filter_: Optional[str] = None,
    min_: Optional[Union[int, str]] = "q",
    sort: Optional[str] = None,
    sort_dir: str = "desc",
    csv: bool = False,
    return_as_pandas: bool = False,
    **kwargs,
):
    """GET /leaderboard/custom — build-your-own metric leaderboard.

    Args:
        year: season year.
        type_: leaderboard type (``batter`` / ``pitcher`` / ``fielder``).
        selections: comma-separated metric ids (e.g. ``"xba,xslg,xwoba"``).
        filter_: row filter (e.g. ``"hand_R"``).
        min_: minimum threshold; ``"q"`` for qualified.
        sort: metric to sort by; ``sort_dir`` ``"desc"`` or ``"asc"``.
        csv: when True, request CSV; otherwise JSON.
    """
    return _leaderboard(
        "/leaderboard/custom",
        params={
            "year": year,
            "type": type_,
            "filter": filter_,
            "min": min_,
            "selections": selections,
            "sort": sort,
            "sortDir": sort_dir,
        },
        csv=csv,
        return_as_pandas=return_as_pandas,
        **kwargs,
    )


def statcast_leaderboard_expected_statistics(
    year: Union[int, str],
    type_: str = "batter",
    position: Optional[str] = None,
    team: Optional[str] = None,
    min_: Optional[Union[int, str]] = "q",
    csv: bool = True,
    return_as_pandas: bool = False,
    **kwargs,
):
    """GET /leaderboard/expected_statistics — xBA / xSLG / xwOBA / xISO leaders."""
    return _leaderboard(
        "/leaderboard/expected_statistics",
        params={
            "year": year,
            "type": type_,
            "position": position,
            "team": team,
            "min": min_,
        },
        csv=csv,
        return_as_pandas=return_as_pandas,
        **kwargs,
    )


def statcast_leaderboard_sprint_speed(
    year: Union[int, str],
    position: Optional[str] = None,
    team: Optional[str] = None,
    min_opp: Optional[int] = None,
    csv: bool = True,
    return_as_pandas: bool = False,
    **kwargs,
):
    """GET /leaderboard/sprint_speed — sprint-speed (ft/sec) leaders."""
    return _leaderboard(
        "/leaderboard/sprint_speed",
        params={
            "year": year,
            "position": position,
            "team": team,
            "min_opp": min_opp,
        },
        csv=csv,
        return_as_pandas=return_as_pandas,
        **kwargs,
    )


def statcast_leaderboard_outs_above_average(
    year: Union[int, str],
    pos: Optional[str] = None,
    team: Optional[str] = None,
    min_: Optional[Union[int, str]] = "q",
    csv: bool = True,
    return_as_pandas: bool = False,
    **kwargs,
):
    """GET /leaderboard/outs_above_average — OAA fielding leaderboard."""
    return _leaderboard(
        "/leaderboard/outs_above_average",
        params={
            "year": year,
            "pos": pos,
            "team": team,
            "min": min_,
        },
        csv=csv,
        return_as_pandas=return_as_pandas,
        **kwargs,
    )


def statcast_leaderboard_catch_probability(
    year: Union[int, str],
    csv: bool = True,
    return_as_pandas: bool = False,
    **kwargs,
):
    """GET /leaderboard/catch_probability — outfielder catch-probability leaderboard."""
    return _leaderboard(
        "/leaderboard/catch_probability",
        params={"year": year},
        csv=csv,
        return_as_pandas=return_as_pandas,
        **kwargs,
    )


def statcast_leaderboard_arm_strength(
    year: Union[int, str],
    pos: Optional[str] = None,
    csv: bool = True,
    return_as_pandas: bool = False,
    **kwargs,
):
    """GET /leaderboard/arm-strength — outfielder + infielder arm-strength leaders."""
    return _leaderboard(
        "/leaderboard/arm-strength",
        params={"year": year, "pos": pos},
        csv=csv,
        return_as_pandas=return_as_pandas,
        **kwargs,
    )


def statcast_leaderboard_bat_tracking(
    year: Union[int, str],
    type_: str = "batter-swings",
    min_: Optional[Union[int, str]] = "q",
    attack_zone: Optional[str] = None,
    csv: bool = True,
    return_as_pandas: bool = False,
    **kwargs,
):
    """GET /leaderboard/bat-tracking — swing speed / attack angle (2024+)."""
    return _leaderboard(
        "/leaderboard/bat-tracking",
        params={
            "year": year,
            "type": type_,
            "min": min_,
            "attackZone": attack_zone,
        },
        csv=csv,
        return_as_pandas=return_as_pandas,
        **kwargs,
    )


def statcast_leaderboard_poptime(
    year: Union[int, str],
    min2b: Optional[int] = None,
    min3b: Optional[int] = None,
    csv: bool = True,
    return_as_pandas: bool = False,
    **kwargs,
):
    """GET /leaderboard/poptime — catcher pop-time leaders."""
    return _leaderboard(
        "/leaderboard/poptime",
        params={
            "year": year,
            "min2b": min2b,
            "min3b": min3b,
        },
        csv=csv,
        return_as_pandas=return_as_pandas,
        **kwargs,
    )


def statcast_leaderboard_pitch_arsenal(
    year: Union[int, str],
    team: Optional[str] = None,
    min_: Optional[Union[int, str]] = "q",
    pitch_hand: Optional[str] = None,
    csv: bool = True,
    return_as_pandas: bool = False,
    **kwargs,
):
    """GET /leaderboard/pitch-arsenal-stats — per-pitch outcome stats by pitcher."""
    return _leaderboard(
        "/leaderboard/pitch-arsenal-stats",
        params={
            "year": year,
            "team": team,
            "min": min_,
            "pitch_hand": pitch_hand,
        },
        csv=csv,
        return_as_pandas=return_as_pandas,
        **kwargs,
    )


# ---------------------------------------------------------------------------
# /gf  — Savant per-game feed
# ---------------------------------------------------------------------------


def statcast_gamefeed(game_pk: int, at_bat_number: Optional[int] = None, **kwargs) -> Dict:
    """GET /gf?game_pk=... — Savant per-game JSON feed (richer than the Stats API live feed).

    Returns a dict with ``team_home, team_away, scoreboard, game_status, …`` plus
    per-play pitch tracking and shift positioning details.
    """
    return _get_json(
        "/gf",
        params={
            "game_pk": game_pk,
            "at_bat_number": at_bat_number,
        },
        **kwargs,
    )


# ---------------------------------------------------------------------------
# /savant-player/{playerId}  — embedded-JSON player profile
# ---------------------------------------------------------------------------


def statcast_player_page(player_id: int, stats: Optional[str] = None, **kwargs) -> str:
    """GET /savant-player/{playerId} — Savant player profile page (HTML with embedded JSON).

    Returns the raw HTML text. The page embeds JSON blobs under
    ``<script id="player-data" type="application/json">…</script>`` (and a
    handful of others) that carry the canonical Statcast snapshots for the
    player. Extracting those blobs is a follow-up — for now the wrapper
    returns the full HTML so callers can mine it.

    TODO: add a sibling :func:`statcast_player_data` that does the BS4 /
    regex extraction and returns a typed dict.
    """
    url = f"{_SAVANT_BASE}/savant-player/{player_id}"
    params = {"stats": stats} if stats else None
    resp = download(url=url, params=params, **kwargs)
    if resp is None:
        return ""
    return resp.text if hasattr(resp, "text") else resp.content.decode("utf-8", errors="replace")
