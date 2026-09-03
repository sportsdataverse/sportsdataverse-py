"""Runtime getter + parser for the generated KenPom wrappers (:mod:`sportsdataverse.mbb.kenpom`).

`kenpom.com <https://kenpom.com>`_ is Ken Pomeroy's men's college-basketball
ratings site. Every table this module reaches is **subscriber-only**: the pages
are served over plain HTTP with no API, and the gate is a session cookie minted
by a username/password login. The shared login/proxy/HTML layer lives in
:mod:`sportsdataverse._subscription_http`; this module supplies only the
KenPom-specific description of it plus the two callables the generated wrappers
import (``_get`` and ``parse_kenpom_page``).

This is the Python port of hoopR's ``kp_*()`` family (``R/kp_*.R`` +
``login()`` / ``.kp_get_page()`` in ``R/utils.R``), with two deliberate
divergences:

* **One wrapper per URL, all tables returned.** hoopR ships four functions
  (``kp_team_schedule``, ``kp_team_players``, ``kp_team_depth_chart``,
  ``kp_team_lineups``) that each fetch the *same* ``team.php`` page and keep a
  different table off it -- four logins and four fetches for one page. Here
  ``kenpom_team(team=, y=)`` fetches once and returns every table on the page
  keyed by its HTML ``id``. Same for ``player-expanded.php`` (hoopR's
  ``kp_minutes_matrix`` + ``kp_team_player_stats``).
* **No hardcoded header vectors.** hoopR carries ~44 ``header_cols <- c(...)``
  literals, several of them year-conditional, because ``rvest::html_table()``
  flattens KenPom's two-row ``<thead>`` into an unusable single row.
  :func:`sportsdataverse._subscription_http.html_tables` derives the same names
  from the ``MultiIndex`` ``pandas.read_html`` builds, so a KenPom column
  addition widens the frame instead of silently shifting every column.

Credentials come from ``KENPOM_EMAIL`` / ``KENPOM_PW`` (hoopR's ``KP_USER`` /
``KP_PW`` are also accepted, so an existing R environment works unchanged), or
from ``email=`` / ``password=`` on any call. Proxy from ``proxy=``,
``SDV_PY_KENPOM_PROXY``, or ``SDV_PY_PROXY``.
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
    "KENPOM",
    "_get",
    "kenpom_login",
    "has_kenpom_login",
    "parse_kenpom_page",
]

KENPOM = SubscriptionSite(
    name="KenPom",
    base_url="https://kenpom.com",
    login_url="https://kenpom.com/index.php",
    user_field="email",
    password_field="password",
    # KENPOM_* first; KP_USER / KP_PW are hoopR's names, honoured for R parity.
    email_env=("KENPOM_EMAIL", "KP_USER", "SDV_PY_KENPOM_EMAIL"),
    password_env=("KENPOM_PW", "KP_PW", "SDV_PY_KENPOM_PW"),
    proxy_env="SDV_PY_KENPOM_PROXY",
    extra_form={"submit": "Login"},
    default_action="handlers/login_handler.php",
    signup_url="https://kenpom.com/register.php",
)


def kenpom_login(
    email: Optional[str] = None,
    password: Optional[str] = None,
    *,
    proxy: Any = None,
) -> requests.Session:
    """Log into kenpom.com and return the authenticated session.

    The Python counterpart of hoopR's ``login()``. Calling this directly is
    optional -- every wrapper logs in on demand and reuses a cached session --
    but it is the fastest way to verify credentials or a proxy before a long
    pull, and the returned session can be passed to a wrapper as ``session=``.

    Args:
        email: KenPom account e-mail. Falls back to ``KENPOM_EMAIL`` /
            ``KP_USER`` / ``SDV_PY_KENPOM_EMAIL``.
        password: KenPom password. Falls back to ``KENPOM_PW`` / ``KP_PW`` /
            ``SDV_PY_KENPOM_PW``.
        proxy: Proxy URL ``str`` or ``requests`` ``proxies=`` ``dict``. Falls
            back to ``SDV_PY_KENPOM_PROXY`` then ``SDV_PY_PROXY``.

    Returns:
        An authenticated :class:`requests.Session` carrying the subscription
        cookie and the resolved proxy.

    Raises:
        RuntimeError: When credentials cannot be resolved, or KenPom rejects them.

    Example:
        Verify a subscription + proxy before a backfill::

            from sportsdataverse.mbb import kenpom_login

            session = kenpom_login(proxy="http://user:pw@proxy.example:8080")
    """
    return _login(KENPOM, email, password, proxy=proxy)


def has_kenpom_login() -> bool:
    """Whether KenPom credentials are set in the environment.

    The Python counterpart of hoopR's ``has_kp_user_and_pw()``; gates a live
    test without attempting a login.

    Returns:
        ``True`` when both an e-mail and a password resolve from the environment.

    Example:
        Skip a live test cleanly::

            import pytest
            from sportsdataverse.mbb import has_kenpom_login

            pytestmark = pytest.mark.skipif(not has_kenpom_login(), reason="no KenPom login")
    """
    return has_credentials(KENPOM)


def _get(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    **kwargs: Any,
) -> str:
    """GET an authenticated kenpom.com page and return its raw HTML.

    The getter the generated wrappers in :mod:`sportsdataverse.mbb.kenpom` call.
    Auth and proxy kwargs are consumed here; everything else is forwarded to
    :func:`sportsdataverse.dl_utils.download`.

    Args:
        url: Fully-qualified kenpom.com URL built by the wrapper.
        params: Query-string parameters; ``None`` values are dropped.
        headers: Extra request headers merged over the defaults. KenPom's auth is
            the session cookie, not a header, so this is rarely needed -- the
            generated wrappers expose it because the family is marked ``auth: true``.
        **kwargs: ``email``, ``password``, ``proxy`` and ``session`` steer auth;
            the rest (``timeout``, ``num_retries``, ...) reach ``download``.

    Returns:
        The page HTML as ``str``; ``""`` when the request yields no response.

    Raises:
        RuntimeError: When credentials cannot be resolved or are rejected.

    Example:
        Fetch the 2025 ratings page (needs a live subscription)::

            from sportsdataverse.mbb.kenpom_runtime import _get

            html = _get("https://kenpom.com/index.php", {"y": 2025})
    """
    return get_html(KENPOM, url, params, headers=headers, **kwargs)


def _drop_repeated_headers(frame: pl.DataFrame) -> pl.DataFrame:
    """Drop KenPom's in-table header repeats and blank separator rows.

    KenPom re-renders its header row every ~40 data rows so a long table stays
    readable while scrolling. Those repeats land in the body as data (the 2025
    ratings page yields 382 rows for 364 teams: 18 junk rows). hoopR removes them
    with ``filter(!is.na(as.numeric(SOS.OppO)))`` -- a hardcoded column. This finds
    the anchor generically: the first column that parses as a number for at least
    half the rows, then keeps only rows where it parses.

    Args:
        frame: A parsed KenPom table.

    Returns:
        The frame without header-repeat or all-null rows.
    """
    for col in frame.columns:
        if frame.schema[col] != pl.Utf8:
            continue
        parsed = frame[col].str.strip_chars().str.replace(r"^\+", "").cast(pl.Float64, strict=False)
        if len(frame) and parsed.is_not_null().sum() / len(frame) >= 0.5:
            return frame.filter(parsed.is_not_null())
    return frame.filter(~pl.all_horizontal(pl.all().is_null()))


def _split_ncaa_seed(frame: pl.DataFrame) -> pl.DataFrame:
    """Split the NCAA tournament seed off KenPom's team label into its own column.

    KenPom appends a tournament seed (and sometimes a ``*`` marker) to the team
    name, so the flagship ratings table reads ``"Duke 1"``, not ``"Duke"`` -- which
    silently breaks every join on team name. hoopR strips it the same way.

    Args:
        frame: A parsed KenPom table, with or without a ``team`` column.

    Returns:
        The frame with a cleaned ``team`` and an added Int64 ``ncaa_seed``
        (null for unseeded teams). Returned unchanged when there is no ``team``.
    """
    if "team" not in frame.columns or frame.schema["team"] != pl.Utf8:
        return frame
    return frame.with_columns(
        frame["team"].str.extract(r"\s(\d{1,2})\s*\**\s*$", 1).cast(pl.Int64, strict=False).alias("ncaa_seed"),
        frame["team"].str.replace(r"\s*\d{1,2}\s*\**\s*$", "").str.strip_chars().alias("team"),
    )


def _cast_numerics(frame: pl.DataFrame) -> pl.DataFrame:
    """Cast every fully-numeric text column to Int64 or Float64.

    KenPom serves everything as text, and signed values carry a leading ``+``
    (``"+9.46"``) that a bare cast rejects. A column converts only when EVERY
    non-null value parses, so ``w_l`` (``"35-4"``), ``conf`` and ``team`` stay text.

    Args:
        frame: A parsed KenPom table.

    Returns:
        The frame with numeric-looking columns typed.
    """
    out = frame
    for col in frame.columns:
        if frame.schema[col] != pl.Utf8:
            continue
        cleaned = frame[col].str.strip_chars().str.replace(r"^\+", "")
        parsed = cleaned.cast(pl.Float64, strict=False)
        # every non-null string had to parse, and the column must not be all-null
        if parsed.is_null().sum() != frame[col].is_null().sum() or parsed.is_not_null().sum() == 0:
            continue
        integral = parsed.drop_nulls().eq(parsed.drop_nulls().round(0)).all()
        out = out.with_columns((parsed.cast(pl.Int64) if integral else parsed).alias(col))
    return out


def parse_kenpom_page(
    raw: str,
    *,
    return_as_pandas: bool = False,
) -> Dict[str, pl.DataFrame | pd.DataFrame]:
    """Parse a KenPom page into one cleaned DataFrame per HTML table.

    Keys are the table's HTML ``id`` (KenPom's real data tables all carry one --
    ``ratings_table``, ``player_table``, ``schedule_table``, ...), snake-cased.
    ``min_rows=2`` drops the small nav/legend tables KenPom renders alongside
    the data.

    Args:
        raw: The page HTML from :func:`_get`.
        return_as_pandas: Return ``pandas.DataFrame`` values instead of polars.

    Returns:
        ``{table_key: DataFrame}``. Empty when the page carried no data table --
        which for a live fetch means the season/team argument found nothing.

    Example:
        Season ratings (needs a live subscription)::

            from sportsdataverse.mbb import kenpom_ratings

            tables = kenpom_ratings(y=2025)
            tables["ratings_table"].head()
    """
    tables = html_tables(raw, min_rows=2)
    tidied = {k: _cast_numerics(_split_ncaa_seed(_drop_repeated_headers(v))) for k, v in tables.items()}
    if return_as_pandas:
        return {k: v.to_pandas() for k, v in tidied.items()}
    return tidied
