"""Runtime for the Czech Extraliga (``elh``) wrappers: hokej.cz's S3 bucket + the eSports Media scoreboard.

Two keyless hosts, both throttled to one request a second per host: every request, each retry
included, goes through :data:`sportsdataverse.hockey.euro_hockey_runtime.SESSION`, which calls
:func:`~sportsdataverse.hockey.euro_hockey_runtime._polite_wait` before every attempt:

* ``s3-eu-west-1.amazonaws.com/hokej.cz`` -- a public, listable S3 bucket. ``/`` answers a
  ListObjects V1 **XML** page (returned as text); ``match/{season}/{full,short,mobile}/{id}.json``
  (play-by-play), ``visualization/shots/{id}.json`` and ``visualization/events/{id}.json``
  answer JSON. A missing key answers 404 ``NoSuchKey`` XML.
* ``json.esports.cz/hokejcz/scoreboard/{YYYY-MM-DD}.json`` -- one day, every league, keyed by
  league id (ELH = ``101``). The window is ROLLING: a day outside it (an old day, and also
  today) answers 404 ``text/html``, so it cannot be backfilled.

**Season key.** The public argument is the season's END year (``2027`` = 2026-27), as everywhere
in sportsdataverse. The S3 folder is the START year, so :func:`elh_start_year` subtracts one
at the boundary (the wrappers' ``season`` path argument runs through it).

**Refusals** (the reference capture's checks, ``sdv-internal-refs/hokejcz/tools/capture.py``).
On top of the shared error vocabulary (404 -> :class:`~sportsdataverse.errors.NoDataError`,
400 / 422 -> :class:`ValueError`, any other non-2xx, an empty or non-JSON 2xx, a dead
connection -> :class:`~sportsdataverse.errors.AssetFetchError`), :func:`_get` raises:

* :class:`~sportsdataverse.errors.AssetFetchError` for a 2xx error envelope (a body whose
  keys are only ``error`` / ``message`` / ``code`` / ...), a wrong-scope body (a shots file
  whose ``match.id`` is not the requested game, a scoreboard whose games are dated another
  day), a listing page that is not a ``ListBucketResult``, and a truncated listing page
  (``IsTruncated`` with no key to page on);
* :class:`~sportsdataverse.errors.NoDataError` for a stub body: play-by-play without
  comments, shots without shots, per-minute events without minutes, a scoreboard day without
  an ELH (``101``) game.
"""

from __future__ import annotations

import re
from typing import Any, List, Optional, Tuple, Union
from urllib.parse import urlsplit

import pandas as pd
import polars as pl

from sportsdataverse import _codegen_runtime as _rt
from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.hockey import euro_hockey_runtime as _ehr
from sportsdataverse.hockey.elh.elh_parsers import ELH_LEAGUE_ID, parse_elh_game_ids

__all__ = ["elh_game_ids", "elh_start_year"]

S3 = "https://s3-eu-west-1.amazonaws.com/hokej.cz"
#: A 2xx body whose keys are all in here is an error envelope, not data.
ERROR_KEYS = frozenset({"error", "errors", "message", "code", "detail", "status"})
#: Hard stop for :func:`elh_game_ids` paging (1,000 keys a page; a season folder holds < 7,000).
MAX_PAGES = 50

_PBP = re.compile(r"/match/\d+/(full|short)/\d+\.json$")
_MOBILE = re.compile(r"/match/\d+/mobile/\d+\.json$")
_SHOTS = re.compile(r"/visualization/shots/(\d+)\.json$")
_EVENTS = re.compile(r"/visualization/events/\d+\.json$")
_SCOREBOARD = re.compile(r"/scoreboard/(\d{4})-(\d{2})-(\d{2})\.json$")


def elh_start_year(season: Any) -> int:
    """S3 folder of a season given by its END year: ``2027`` (2026-27) -> ``2026``.

    Args:
        season: the season's END year, e.g. ``2027`` for 2026-27.

    Returns:
        The START year the bucket files the season under.

    Raises:
        ValueError: ``season`` is not a year.

    See Also:
        * :func:`sportsdataverse.hockey.elh.elh_list_objects` - the route this works on.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    try:
        end = int(season)
    except (TypeError, ValueError):
        raise ValueError(f"season must be an END year such as 2027, got {season!r}") from None
    if isinstance(season, bool) or not 1000 <= end <= 9999:
        raise ValueError(f"season must be an END year such as 2027, got {season!r}")
    return end - 1


def _dig(obj: Any, *keys: str) -> Any:
    for key in keys:
        obj = obj.get(key) if isinstance(obj, dict) else None
    return obj


def _rows(obj: Any) -> List[Any]:
    """XML-to-JSON rows: a lone element is an object, repeated ones an array."""
    if isinstance(obj, dict):
        return [obj] if obj else []
    return obj if isinstance(obj, list) else []


def _check_json(url: str, raw: Any) -> None:
    """Raise unless a JSON body is a real, in-scope answer for ``url`` (see the module docstring)."""
    where = _rt._where(url)
    if isinstance(raw, dict) and raw and set(raw) <= ERROR_KEYS:
        raise AssetFetchError(f"{where}: answered an error envelope {sorted(raw)}")
    path = urlsplit(url).path
    if _PBP.search(path):
        rows = _rows(_dig(raw, "comments", "comment"))
        if not rows or not all(isinstance(r, dict) and r for r in rows):
            raise NoDataError(f"{where}: the play-by-play holds no comments")
    elif _MOBILE.search(path):
        rows = _dig(raw, "commentEvents")
        if not (isinstance(rows, list) and rows and all(isinstance(r, dict) and r for r in rows)):
            raise NoDataError(f"{where}: the mobile play-by-play holds no comments")
    elif m := _SHOTS.search(path):
        shots = _dig(raw, "match", "shots")
        if not (isinstance(shots, list) and shots and all(isinstance(r, dict) and r for r in shots)):
            raise NoDataError(f"{where}: the shot file holds no shots")
        got = _dig(raw, "match", "id")
        if str(got) != m.group(1):
            raise AssetFetchError(f"{where}: answered game {got!r} for game {m.group(1)}")
    elif _EVENTS.search(path):
        for side in ("home", "visitor"):
            minutes = _dig(raw, side, "shots")
            if not (isinstance(minutes, dict) and minutes):
                raise NoDataError(f"{where}: the per-minute file holds no {side} minutes")
    elif m := _SCOREBOARD.search(path):
        day = f"{m.group(3)}-{m.group(2)}-{m.group(1)}"  # the body dates games DD-MM-YYYY
        leagues = raw if isinstance(raw, dict) else {}
        dates = {g.get("date") for lg in leagues.values() for g in _rows(_dig(lg, "matches")) if isinstance(g, dict)}
        if dates and dates != {day}:
            raise AssetFetchError(f"{where}: answered games dated {sorted(map(str, dates))} for {day}")
        if not _rows(_dig(leagues, str(ELH_LEAGUE_ID), "matches")):
            raise NoDataError(f"{where}: no Extraliga (league {ELH_LEAGUE_ID}) game that day")


def _listing_root(url: str, text: str) -> Any:
    """Parse a ListObjects page, refusing anything but a ``ListBucketResult``."""
    try:
        from defusedxml.ElementTree import fromstring  # entity-expansion safe, when installed
    except ImportError:  # pragma: no cover - defusedxml ships with the dev environment
        from xml.etree.ElementTree import fromstring  # noqa: S405
    try:
        root = fromstring(text)
    except Exception:  # ParseError, defusedxml's DefusedXmlException
        raise AssetFetchError(f"{_rt._where(url)}: the listing is not XML: {_rt._excerpt(text)}") from None
    if root.tag.rsplit("}", 1)[-1] != "ListBucketResult":
        raise AssetFetchError(f"{_rt._where(url)}: the listing root is not ListBucketResult")
    return root


def _page_state(root: Any) -> Tuple[bool, Optional[str]]:
    """``(IsTruncated, last Key)`` of a listing page."""
    truncated, last = False, None
    for el in root:
        tag = el.tag.rsplit("}", 1)[-1]
        if tag == "IsTruncated":
            truncated = (el.text or "").strip().lower() == "true"
        elif tag == "Contents":
            for kid in el:
                if kid.tag.rsplit("}", 1)[-1] == "Key":
                    last = kid.text
    return truncated, last


def _get_listing(url: str, params: Optional[dict] = None, **kwargs: Any) -> str:
    """GET one ListObjects V1 page as XML text (throttled, error vocabulary, scope-checked)."""
    text = _ehr.get_text(url, params, **kwargs)
    truncated, last = _page_state(_listing_root(url, text))
    if truncated and last is None:
        raise AssetFetchError(f"{_rt._where(url)}: a truncated listing page carries no key to page on")
    return text


def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Any:
    """GET an ELH route: the listing as XML text, everything else as checked JSON.

    Raises:
        NoDataError: the host answered 404 (a missing key, a scoreboard day outside the
            rolling window), or a stub body (see the module docstring).
        ValueError: the host answered 400 / 422.
        AssetFetchError: any other non-2xx after retries, a connection failure, an empty or
            non-JSON 2xx, an error envelope, a wrong-scope body, or a broken listing page.
    """
    if urlsplit(url).path.rstrip("/") == urlsplit(S3).path:
        return _get_listing(url, params, **kwargs)
    raw = _ehr._get(url, params, **kwargs)
    _check_json(url, raw)
    return raw


def elh_game_ids(season: int, *, return_as_pandas: bool = False, **kwargs: Any) -> Union[pl.DataFrame, pd.DataFrame]:
    """Every hokej.cz game id with a play-by-play file in a season, paging the S3 listing.

    Lists ``match/{season - 1}/full/`` (the bucket files a season under its START year) a
    thousand keys at a time, following ``marker`` until the listing is complete.

    Warning:
        The bucket is NOT Extraliga-only. A season folder holds every competition hokej.cz
        shows (Maxa liga, lower leagues, juniors, the national team): about 2,000 to 6,000
        ids against about 400 ELH games, plus files written ahead of a game (a few hundred
        bytes; see ``size``). The listing carries no league, so these ids are NOT filtered:
        check a game's teams with :func:`elh_shots` / :func:`elh_pbp`, or, for a day still in
        the scoreboard's rolling window, keep the ids :func:`elh_scoreboard` lists.

    Args:
        season: the season's END year (``2027`` = 2026-27). Play-by-play starts in 2014-15.
        return_as_pandas: return a pandas DataFrame instead of polars.
        **kwargs: forwarded to the HTTP layer.

    Returns:
        One row per file: ``hokejcz_game_id`` (Int64), ``season`` (END year), ``key``,
        ``last_modified``, ``size``; ordered by game id.

    Raises:
        NoDataError: the season folder holds no play-by-play file.
        AssetFetchError: a page failed, is not a listing, or the listing does not advance.
        ValueError: ``season`` is not a year.

    Example:
        Quick start::

            from sportsdataverse.hockey.elh import elh_game_ids
            ids = elh_game_ids(season=2027)
            print(ids.height)

        Skip the files written ahead of a game (a few hundred bytes)::

            ids.filter(pl.col("size") > 2000)

        See Also:
            * `fastRhockey`_ -- hockey data in R

        .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    start = elh_start_year(season)
    prefix = f"match/{start}/full/"
    pages: List[str] = []
    marker: Optional[str] = None
    for _ in range(MAX_PAGES):
        text = _get_listing(f"{S3}/", {"prefix": prefix, "marker": marker}, **kwargs)
        pages.append(text)
        truncated, last = _page_state(_listing_root(f"{S3}/", text))
        if not truncated:
            break
        if last is None or last == marker:
            raise AssetFetchError(f"{_rt._where(S3)}: the listing for {prefix} does not advance past {marker!r}")
        marker = last
    else:
        raise AssetFetchError(f"{_rt._where(S3)}: the listing for {prefix} did not end within {MAX_PAGES} pages")
    df = parse_elh_game_ids(pages).filter(pl.col("hokejcz_game_id").is_not_null())
    if df.height == 0:
        raise NoDataError(f"{_rt._where(S3)}: no play-by-play files under {prefix} (season {season})")
    df = df.with_columns(pl.lit(int(season), dtype=pl.Int64).alias("season")).select(
        "hokejcz_game_id", "season", "key", "last_modified", "size"
    )
    df = df.unique("hokejcz_game_id", keep="first").sort("hokejcz_game_id")
    return df.to_pandas() if return_as_pandas else df
