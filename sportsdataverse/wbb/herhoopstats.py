"""Her Hoop Stats (herhoopstats.com) authenticated scrapers -- women's basketball.

`Her Hoop Stats <https://herhoopstats.com>`_ is a **subscription** women's
basketball statistics service (NCAA + WNBA). Its member tables sit behind a
Django login, so these wrappers log in with the subscriber's own credentials and
reuse the session cookie -- the shared login / proxy / HTML-table layer in
:mod:`sportsdataverse._subscription_http`, which also drives KenPom
(:mod:`sportsdataverse.mbb.kenpom_runtime`).

This is the Python port of wehoop's ``hhs_*()`` family (``R/hhs.R`` +
``R/hhs_utils.R``). Unlike KenPom, the surface is three functions over an
arbitrary research URL rather than a fixed set of endpoints, so it is
hand-written rather than codegen-generated.

Credentials resolve from ``email=`` / ``password=`` on the call, else from
``HERHOOPSTATS_EMAIL`` / ``HERHOOPSTATS_PW``. Proxy from ``proxy=``,
``SDV_PY_HERHOOPSTATS_PROXY``, or ``SDV_PY_PROXY``.

Verified against a live subscription on 2026-09-02: the Django login (CSRF token
plus a matching ``Referer``) succeeds, ``/stats/ncaa/research/team_single_seasons/``
returns all 362 D-I teams for 2024, and a team page yields both its stat tables and
its roster. wehoop's family was never live-checked, so two of its behaviours were
corrected here -- see :func:`_team_links` for the row-wise link extraction, and
``sportsdataverse._html_tables`` for the ``<audio>`` widget that contaminates
player names.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Optional

import polars as pl

from sportsdataverse._subscription_http import (
    SubscriptionSite,
    get_html,
    has_credentials,
    html_tables,
)
from sportsdataverse._subscription_http import login as _login

if TYPE_CHECKING:  # pragma: no cover -- annotation-only imports
    import pandas as pd
    import requests

__all__ = [
    "HERHOOPSTATS",
    "herhoopstats_login",
    "has_herhoopstats_login",
    "herhoopstats_teams",
    "herhoopstats_team_stats",
    "herhoopstats_team_roster",
]

HERHOOPSTATS = SubscriptionSite(
    name="Her Hoop Stats",
    base_url="https://herhoopstats.com",
    login_url="https://herhoopstats.com/accounts/login/",
    user_field="email",
    password_field="password",
    email_env=("HERHOOPSTATS_EMAIL", "SDV_PY_HERHOOPSTATS_EMAIL"),
    password_env=("HERHOOPSTATS_PW", "SDV_PY_HERHOOPSTATS_PW"),
    proxy_env="SDV_PY_HERHOOPSTATS_PROXY",
    csrf_field="csrfmiddlewaretoken",  # Django
    signup_url="https://herhoopstats.com/subscribe/",
)

_TEAM_SEASONS_PATH = "/stats/ncaa/research/team_single_seasons/"


def herhoopstats_login(
    email: Optional[str] = None,
    password: Optional[str] = None,
    *,
    proxy: Any = None,
) -> requests.Session:
    """Log into herhoopstats.com and return the authenticated session.

    The Python counterpart of wehoop's ``.hhs_login()``. Optional -- every
    wrapper logs in on demand and reuses a cached session -- but it is the
    fastest way to verify credentials or a proxy before a long pull, and the
    returned session can be handed to a wrapper as ``session=``.

    Args:
        email: Subscription e-mail. Falls back to ``HERHOOPSTATS_EMAIL``.
        password: Subscription password. Falls back to ``HERHOOPSTATS_PW``.
        proxy: Proxy URL ``str`` or ``requests`` ``proxies=`` ``dict``. Falls
            back to ``SDV_PY_HERHOOPSTATS_PROXY`` then ``SDV_PY_PROXY``.

    Returns:
        An authenticated :class:`requests.Session`.

    Raises:
        RuntimeError: When credentials cannot be resolved, or the site rejects them.

    Example:
        Quick start::

            from sportsdataverse.wbb import herhoopstats_login

            session = herhoopstats_login(proxy="http://user:pw@proxy.example:8080")
    """
    return _login(HERHOOPSTATS, email, password, proxy=proxy)


def has_herhoopstats_login() -> bool:
    """Whether Her Hoop Stats credentials are set in the environment.

    Returns:
        ``True`` when both an e-mail and a password resolve from the environment.

    Example:
        Gate a live test::

            import pytest
            from sportsdataverse.wbb import has_herhoopstats_login

            pytestmark = pytest.mark.skipif(
                not has_herhoopstats_login(), reason="no Her Hoop Stats login"
            )
    """
    return has_credentials(HERHOOPSTATS)


def _page(path: str, params: Optional[Dict[str, Any]] = None, **kwargs: Any) -> str:
    """GET one authenticated Her Hoop Stats page and return its HTML."""
    return get_html(HERHOOPSTATS, path, params, **kwargs)


def _largest(tables: Dict[str, Any]) -> Any:
    """The table with the most rows, or an empty frame when there are none.

    Her Hoop Stats member pages render several tables (nav, summary, the data);
    wehoop's ``hhs_team_stats()`` picks the tallest, which this mirrors.
    """
    if not tables:
        return pl.DataFrame()
    return max(tables.values(), key=len)


def _team_links(html: str, n_rows: int) -> Optional[list[Optional[str]]]:
    """Per-row team-page hrefs from the research table, or ``None`` if unusable.

    Team pages are reachable ONLY through these links -- there is no id scheme to
    construct a URL from -- so this column is what makes
    :func:`herhoopstats_team_stats` and :func:`herhoopstats_team_roster` callable
    at all. Hrefs look like
    ``/stats/ncaa/team/{season}/natl/{slug}-{uuid}/``.

    wehoop collects every anchor in the tbody and pairs them with rows by
    position, which silently drops the column whenever any row carries a second
    link. This walks row by row and takes each row's first team anchor instead, so
    an extra link in one row cannot shift every later team's URL.

    Args:
        html: The research-page HTML.
        n_rows: Row count of the parsed frame, used to reject a mismatch.

    Returns:
        One href (or ``None``) per row, or ``None`` when the count disagrees with
        the frame -- better no column than a misaligned one.
    """
    from bs4 import BeautifulSoup

    table = BeautifulSoup(html or "", "lxml").find("table")
    if table is None:
        return None
    body = table.find("tbody") or table
    links: list[Optional[str]] = []
    for row in body.find_all("tr"):
        href = next(
            (a.get("href") for a in row.find_all("a") if "/team/" in (a.get("href") or "")),
            None,
        )
        links.append(href)
    return links if len(links) == n_rows else None


def herhoopstats_teams(
    min_season: int,
    max_season: Optional[int] = None,
    division: int = 1,
    *,
    email: Optional[str] = None,
    password: Optional[str] = None,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> pl.DataFrame | pd.DataFrame:
    """NCAA women's team single-season summary table.

    Port of wehoop's ``hhs_teams()``. One row per team-season (record, scoring,
    per-100-possession columns), with the requested ``min_season`` /
    ``max_season`` / ``division`` attached so concatenated pulls stay traceable.

    Args:
        min_season: First season, as a 4-digit ENDING year (2024 = 2023-24).
        max_season: Last season, same convention. Defaults to ``min_season``.
        division: NCAA division -- ``1`` (default), ``2`` or ``3``.
        email: Subscription e-mail; falls back to ``HERHOOPSTATS_EMAIL``.
        password: Subscription password; falls back to ``HERHOOPSTATS_PW``.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.
        **kwargs: ``proxy`` / ``session`` steer auth; the rest reach
            :func:`sportsdataverse.dl_utils.download`.

    Returns:
        One row per team-season, plus a ``team_link`` column carrying each team's
        page path -- the only way to reach :func:`herhoopstats_team_stats` /
        :func:`herhoopstats_team_roster`. A zero-row frame when nothing matched.

    Raises:
        RuntimeError: When credentials cannot be resolved or are rejected.

    Example:
        Quick start::

            from sportsdataverse.wbb import herhoopstats_teams

            teams = herhoopstats_teams(min_season=2024, division=1)

        Multiple seasons, through a proxy::

            teams = herhoopstats_teams(2022, 2024, proxy="http://127.0.0.1:8888")
    """
    max_season = min_season if max_season is None else max_season
    html = _page(
        _TEAM_SEASONS_PATH,
        {
            "min_season": min_season,
            "max_season": max_season,
            "division": division,
            "games": "all",
            "stats_to_show": "summary",
            "submit": "true",
        },
        email=email,
        password=password,
        **kwargs,
    )
    frame = _largest(html_tables(html, min_rows=1))
    if len(frame) == 0:
        return frame.to_pandas() if return_as_pandas else frame
    links = _team_links(html, len(frame))
    if links is not None:
        frame = frame.with_columns(pl.Series("team_link", links, dtype=pl.Utf8))
    frame = frame.with_columns(
        pl.lit(int(min_season), dtype=pl.Int64).alias("min_season"),
        pl.lit(int(max_season), dtype=pl.Int64).alias("max_season"),
        pl.lit(int(division), dtype=pl.Int64).alias("division"),
    )
    return frame.to_pandas() if return_as_pandas else frame


def herhoopstats_team_stats(
    team_link: str,
    *,
    email: Optional[str] = None,
    password: Optional[str] = None,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Dict[str, pl.DataFrame | pd.DataFrame]:
    """Every table on one Her Hoop Stats team page.

    Port of wehoop's ``hhs_team_stats()``, which keeps only the tallest table.
    This returns all of them keyed by the table's HTML ``id`` (else caption, else
    position), so a single fetch covers the whole page.

    Args:
        team_link: A team page path or full URL -- e.g. a link taken from the
            team-page column of :func:`herhoopstats_teams`.
        email: Subscription e-mail; falls back to ``HERHOOPSTATS_EMAIL``.
        password: Subscription password; falls back to ``HERHOOPSTATS_PW``.
        return_as_pandas: Return ``pandas.DataFrame`` values instead of polars.
        **kwargs: ``proxy`` / ``session`` steer auth; the rest reach ``download``.

    Returns:
        ``{table_key: DataFrame}``; empty when the page carried no data table.

    Raises:
        RuntimeError: When credentials cannot be resolved or are rejected.

    Example:
        Quick start::

            from sportsdataverse.wbb import herhoopstats_team_stats

            tables = herhoopstats_team_stats("/stats/ncaa/team/12345/2024/")
            list(tables)
    """
    html = _page(team_link, None, email=email, password=password, **kwargs)
    return html_tables(html, min_rows=1, return_as_pandas=return_as_pandas)


def herhoopstats_team_roster(
    team_link: str,
    *,
    email: Optional[str] = None,
    password: Optional[str] = None,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> pl.DataFrame | pd.DataFrame:
    """The player roster table from one Her Hoop Stats team page.

    Port of wehoop's ``hhs_team_roster()``: picks the table carrying a player /
    name column, falling back to the tallest table on the page.

    Args:
        team_link: A team page path or full URL.
        email: Subscription e-mail; falls back to ``HERHOOPSTATS_EMAIL``.
        password: Subscription password; falls back to ``HERHOOPSTATS_PW``.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.
        **kwargs: ``proxy`` / ``session`` steer auth; the rest reach ``download``.

    Returns:
        One row per player. A zero-row frame when the page carried no roster.

    Raises:
        RuntimeError: When credentials cannot be resolved or are rejected.

    Example:
        Quick start::

            from sportsdataverse.wbb import herhoopstats_team_roster

            roster = herhoopstats_team_roster("/stats/ncaa/team/12345/2024/")
    """
    html = _page(team_link, None, email=email, password=password, **kwargs)
    tables = html_tables(html, min_rows=2)
    roster = next(
        (t for t in tables.values() if any(("player" in c or "name" in c) for c in t.columns)),
        None,
    )
    frame = _largest(tables) if roster is None else roster
    return frame.to_pandas() if return_as_pandas else frame
