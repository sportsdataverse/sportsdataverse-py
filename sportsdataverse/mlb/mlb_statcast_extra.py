"""Hand-written Statcast wrappers that need logic beyond a passthrough — the
25,000-row search has to be date-chunked + truncation-checked, and the player
page returns HTML with embedded JSON rather than CSV/JSON."""

from __future__ import annotations
import warnings
from datetime import date, timedelta
from typing import TYPE_CHECKING, Any, List, Optional, Tuple, Union

import polars as pl

from sportsdataverse.dl_utils import download
from sportsdataverse.mlb.mlb_statcast_parsers import _csv_to_frame, parse_mlb_statcast_player

if TYPE_CHECKING:  # pragma: no cover -- annotation-only import
    import pandas as pd

_SAVANT_BASE = "https://baseballsavant.mlb.com"
_SEARCH_URL = f"{_SAVANT_BASE}/statcast_search/csv"
#: Minor-league search shares the search core but hits its own CSV route (verified
#: to return the standard 119-column Statcast CSV for MiLB games).
_SEARCH_URL_MINORS = f"{_SAVANT_BASE}/statcast-search-minors/csv"
#: World Baseball Classic search CSV route (same shape; scope with WBC date windows).
_SEARCH_URL_WBC = f"{_SAVANT_BASE}/statcast-search-world-baseball-classic/csv"


def _date_chunks(start: str, end: str, days: int = 7) -> List[Tuple[str, str]]:
    s, e = date.fromisoformat(start), date.fromisoformat(end)
    out: List[Tuple[str, str]] = []
    cur = s
    while cur <= e:
        chunk_end = min(cur + timedelta(days=days - 1), e)
        out.append((cur.isoformat(), chunk_end.isoformat()))
        cur = chunk_end + timedelta(days=1)
    return out


def _pipe(values: Any) -> str:
    """Format a scalar / iterable as a Savant pipe-list with a trailing ``|``.

    Savant multi-value filters expect ``"FF|SL|"`` (trailing pipe); a scalar
    becomes ``"x|"`` and ``None``/empty becomes ``""``.
    """
    if values is None:
        return ""
    if isinstance(values, str):
        return values if (not values or values.endswith("|")) else values + "|"
    parts = [str(v) for v in values if v is not None and v != ""]
    return "|".join(parts) + "|" if parts else ""


#: Friendly kwarg -> Savant query key for pipe-list filters (value goes through ``_pipe``).
_PIPE_FILTERS = {
    "season": "hfSea",
    "game_type": "hfGT",
    "position": "position",
    "pitch_type": "hfPT",
    "count": "hfC",
    "at_bat_result": "hfAB",
    "batted_ball_type": "hfBBT",
    "pitch_result": "hfPR",
    "zone": "hfZ",
    "outs": "hfOuts",
    "inning": "hfInn",
    "runners_on": "hfRO",
    "flag": "hfFlag",
}
#: Friendly kwarg -> Savant query key for scalar filters (value passed as-is).
_SCALAR_FILTERS = {
    "team": "team",
    "opponent": "opponent",
    "home_road": "home_road",
    "stadium": "stadium",
    "pitcher_throws": "pitcher_throws",
    "batter_stands": "batter_stands",
}
#: Friendly kwarg -> Savant ``name[]`` array param (value coerced to a list of strings).
_LIST_FILTERS = {"batters_lookup": "batters_lookup[]", "pitchers_lookup": "pitchers_lookup[]"}


def _translate_filters(filters: dict) -> dict:
    """Map friendly search kwargs to Savant's query params; pass unknowns through.

    Turns readable kwargs (``season``, ``pitch_type``, ``at_bat_result``,
    ``batters_lookup``, ``team``, …) into the cryptic Savant params (``hfSea``,
    ``hfPT``, ``hfAB``, ``batters_lookup[]``, ``team``, …). Any key not in a map
    is forwarded verbatim, so raw Savant params still work for power users.
    """
    out: dict = {}
    for key, val in filters.items():
        if key in _PIPE_FILTERS:
            out[_PIPE_FILTERS[key]] = _pipe(val)
        elif key in _SCALAR_FILTERS:
            out[_SCALAR_FILTERS[key]] = val
        elif key in _LIST_FILTERS:
            ids = val if isinstance(val, (list, tuple)) else [val]
            out[_LIST_FILTERS[key]] = [str(i) for i in ids]
        else:
            out[key] = val
    return out


def _fetch_chunk(gt: str, lt: str, player_type: str, filters: dict, base_url: str = _SEARCH_URL) -> pl.DataFrame:
    params = {"all": "true", "type": "details", "player_type": player_type, "game_date_gt": gt, "game_date_lt": lt}
    params.update(_translate_filters(filters))
    resp = download(base_url, params=params)
    text = getattr(resp, "text", resp if isinstance(resp, str) else "")
    return _csv_to_frame(text)


def _search_core(
    start_dt: str,
    end_dt: str,
    base_url: str,
    label: str,
    player_type: str = "batter",
    chunk_days: int = 7,
    return_as_pandas: bool = False,
    **filters: Any,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Shared date-chunked, truncation-aware Savant search (MLB / MiLB / WBC).

    Splits ``[start_dt, end_dt]`` into ``chunk_days`` windows, fetches each from
    ``base_url``, halving the window for any chunk that hits the 25,000-row cap,
    and warns once at the 1-day floor (where a single day can still truncate).
    """
    frames: list[pl.DataFrame] = []
    for gt, lt in _date_chunks(start_dt, end_dt, days=chunk_days):
        df = _fetch_chunk(gt, lt, player_type, filters, base_url=base_url)
        if df.height >= 25000 and chunk_days > 1:
            # truncated -> recurse on this sub-range with a smaller window
            df = _search_core(
                gt, lt, base_url, label, player_type=player_type, chunk_days=max(1, chunk_days // 2), **filters
            )
        elif df.height >= 25000:
            warnings.warn(
                f"{label}: {gt}..{lt} hit the 25,000-row Savant cap at the "
                f"1-day floor; results for that day may be truncated.",
                stacklevel=2,
            )
        frames.append(df)
    frames = [f for f in frames if f.height]
    out = pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()
    if return_as_pandas:
        return out.to_pandas()
    return out


def mlb_statcast_search(
    start_dt: str,
    end_dt: str,
    *,
    player_type: str = "batter",
    chunk_days: int = 7,
    return_as_pandas: bool = False,
    **filters: Any,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Pitch-by-pitch MLB Statcast search (``/statcast_search/csv``), date-chunked.

    Savant caps a single ``/statcast_search/csv`` response at **25,000 rows with
    no pagination**. This splits the date range into ``chunk_days`` windows,
    halving any window that hits the cap, and stitches the chunks back together.

    Args:
        start_dt / end_dt: ``YYYY-MM-DD`` (inclusive).
        player_type: ``"batter"`` (default) or ``"pitcher"``.
        chunk_days: initial window size in days.
        return_as_pandas: return a pandas DataFrame instead of polars.
        **filters: friendly filter kwargs translated to Savant's params —
            ``season``, ``game_type``, ``pitch_type``, ``at_bat_result``,
            ``batted_ball_type``, ``pitch_result``, ``zone``, ``count``, ``outs``,
            ``inning``, ``runners_on``, ``flag``, ``position`` (pipe-lists);
            ``batters_lookup`` / ``pitchers_lookup`` (MLBAM id or list); ``team``,
            ``opponent``, ``home_road``, ``stadium``, ``pitcher_throws``,
            ``batter_stands``. Any unrecognized key is forwarded verbatim, so raw
            Savant params (``hfPT``, ``hfZ``, …) still work.

    Returns:
        A polars (or pandas) DataFrame, one row per pitch.

    Example:
        Quick start::

            from sportsdataverse.mlb import mlb_statcast_search
            df = mlb_statcast_search("2024-06-15", "2024-06-16", batters_lookup=592450)
    """
    return _search_core(
        start_dt,
        end_dt,
        _SEARCH_URL,
        "mlb_statcast_search",
        player_type=player_type,
        chunk_days=chunk_days,
        return_as_pandas=return_as_pandas,
        **filters,
    )


def mlb_statcast_search_minors(
    start_dt: str,
    end_dt: str,
    *,
    player_type: str = "batter",
    chunk_days: int = 7,
    return_as_pandas: bool = False,
    **filters: Any,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Minor-league Statcast search (``/statcast-search-minors/csv``), date-chunked.

    Same shape, columns, and 25,000-row chunking as :func:`mlb_statcast_search`,
    but against the MiLB CSV route. Scope with ``hfLevel`` (Triple-A/Double-A/…)
    and ``hfSea`` filters.

    Args:
        start_dt / end_dt: ``YYYY-MM-DD`` (inclusive).
        player_type: ``"batter"`` (default) or ``"pitcher"``.
        chunk_days: initial window size in days.
        return_as_pandas: return a pandas DataFrame instead of polars.
        **filters: Savant filter params passed through verbatim.

    Returns:
        A polars (or pandas) DataFrame, one row per minor-league pitch.

    Example:
        Quick start::

            from sportsdataverse.mlb import mlb_statcast_search_minors
            df = mlb_statcast_search_minors("2024-06-01", "2024-06-02")
    """
    return _search_core(
        start_dt,
        end_dt,
        _SEARCH_URL_MINORS,
        "mlb_statcast_search_minors",
        player_type=player_type,
        chunk_days=chunk_days,
        return_as_pandas=return_as_pandas,
        **filters,
    )


def mlb_statcast_search_wbc(
    start_dt: str,
    end_dt: str,
    *,
    player_type: str = "batter",
    chunk_days: int = 7,
    return_as_pandas: bool = False,
    **filters: Any,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """World Baseball Classic Statcast search (``/statcast-search-world-baseball-classic/csv``).

    Same shape, columns, and 25,000-row chunking as :func:`mlb_statcast_search`,
    against the WBC CSV route. Pass WBC date windows (e.g. March of a WBC year).

    Args:
        start_dt / end_dt: ``YYYY-MM-DD`` (inclusive).
        player_type: ``"batter"`` (default) or ``"pitcher"``.
        chunk_days: initial window size in days.
        return_as_pandas: return a pandas DataFrame instead of polars.
        **filters: Savant filter params passed through verbatim.

    Returns:
        A polars (or pandas) DataFrame, one row per WBC pitch.

    Example:
        Quick start::

            from sportsdataverse.mlb import mlb_statcast_search_wbc
            df = mlb_statcast_search_wbc("2023-03-08", "2023-03-22")
    """
    return _search_core(
        start_dt,
        end_dt,
        _SEARCH_URL_WBC,
        "mlb_statcast_search_wbc",
        player_type=player_type,
        chunk_days=chunk_days,
        return_as_pandas=return_as_pandas,
        **filters,
    )


def _player_page_html(player_id: int, stats: Optional[str] = None, **kwargs: Any) -> str:
    """Fetch the raw ``/savant-player/{id}`` HTML (``""`` on transport failure)."""
    url = f"{_SAVANT_BASE}/savant-player/{player_id}"
    params = {"stats": stats} if stats else None
    resp = download(url=url, params=params, **kwargs)
    if resp is None:
        return ""
    return resp.text if hasattr(resp, "text") else resp.content.decode("utf-8", errors="replace")


def mlb_statcast_player(
    player_id: int,
    stats: Optional[str] = None,
    *,
    section: str = "statcast",
    raw: bool = False,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> "Union[pl.DataFrame, pd.DataFrame, str]":
    """GET /savant-player/{player_id} and parse one embedded table into a tidy frame.

    Returns a tidy frame **by default** (the parsed Statcast page); pass
    ``raw=True`` to get the underlying HTML string instead (the page embeds ~12
    other tables you can mine yourself, or feed to
    :func:`sportsdataverse.mlb.parse_mlb_statcast_player` with a different
    ``section``).

    Args:
        player_id: MLBAM player id (shared with the Stats API ``personId``).
        stats: optional ``stats`` query value to scope the embedded payload.
        section: which embedded ``serverVals`` table to flatten (default
            ``"statcast"``, the seasonal aggregate; e.g. ``"statcastGameLogs"``).
        raw: return the raw page HTML string instead of a parsed frame.
        return_as_pandas: return a pandas DataFrame instead of polars.
        **kwargs: forwarded to :func:`sportsdataverse.dl_utils.download`.

    Returns:
        A polars (or pandas) DataFrame of the player's Statcast metrics by default
        (zero rows when the page/section is absent); the raw HTML ``str`` when
        ``raw=True``.

    Example:
        Quick start::

            from sportsdataverse.mlb import mlb_statcast_player
            df = mlb_statcast_player(592450)
            html = mlb_statcast_player(592450, raw=True)
    """
    html = _player_page_html(player_id, stats=stats, **kwargs)
    if raw:
        return html
    return parse_mlb_statcast_player(html, section=section, return_as_pandas=return_as_pandas)
