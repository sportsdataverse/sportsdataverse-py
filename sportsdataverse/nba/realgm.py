"""RealGM (basketball.realgm.com) NBA scrapers -- the Python port of hoopR's ``realgm_*()`` family.

RealGM publishes an NBA surface no first-party feed carries: an active-player index with
pre-draft / international detail, the players-abroad list, future free agents *with their
agent*, head coaches and GMs, salary-cap history, the rookie scale, past draft results and
the dated league transactions log.

**This is the one sdv-py source that needs a browser hop per request, and it is slow and
CI-hostile.** RealGM sits behind Cloudflare's managed JavaScript challenge
(``cf-mitigated: challenge``): every plain HTTP client -- ``requests``, ``httpx``, and
therefore :func:`sportsdataverse.dl_utils.download` -- receives HTTP 403 regardless of
headers, because the edge fingerprints the TLS ClientHello (JA3) *and* requires a JS
proof-of-work to mint the ``cf_clearance`` cookie. The only reliable defeat is a real
browser engine, so the fetch drives headless Chromium via the optional **Playwright**
extra (hoopR uses ``{chromote}`` for the same reason). Chrome clears the challenge in
~2 seconds; budget several seconds per *first* page and roughly a second per page after
that within one browser window.

Cost control and pacing (never hardcoded):

======================================  =======  =====================================================
Environment variable                    Default  Meaning
======================================  =======  =====================================================
``SDV_PY_REALGM_TTL``                   300      Idle seconds a launched browser is kept for reuse.
``SDV_PY_REALGM_DELAY``                 1.0      Minimum seconds between two page navigations.
``SDV_PY_REALGM_WAIT``                  25       Max seconds to wait for the challenge to clear.
``SDV_PY_REALGM_POLL``                  1.5      Seconds between ``document.title`` polls.
``SDV_PY_REALGM_PROXY`` / ``SDV_PY_PROXY``  --   Proxy URL when ``proxy=`` is not passed.
======================================  =======  =====================================================

The cf clearance cookie persists for the life of a browser context, so the context/page is
cached in-process and reused: one launch per window, not one per request. Call
:func:`realgm_close_browser` to release it early (it is also released at interpreter exit).

Every public function takes ``fetcher=`` -- a callable ``(path, proxy) -> html`` -- which
defaults to the headless-browser fetch. Injecting one runs the whole module offline with no
browser and no network; the test suite does exactly that.

.. warning::

   **Not verified against live pages.** This port was written without network access: the
   URL paths, the table-selection predicates and the column names below are transcribed
   from hoopR's ``R/realgm_*.R`` wrappers, not observed from live HTML, and no request has
   ever been made through a real Cloudflare challenge from this code. A first live run
   should verify, in order: (1) the challenge actually clears and ``page.content()`` is the
   real page, not the interstitial; (2) each path still resolves (RealGM moves ``/nba/...``
   paths -- ``realgm_coaches`` / :func:`realgm_gms` hardcode staff-role ids ``20`` / ``16``);
   (3) the ``must_have`` column predicates still match, since sdv-py's shared
   :func:`~sportsdataverse._html_tables.html_tables` header flattener does not produce
   byte-identical names to R's ``janitor::clean_names()`` for grouped ``<thead>`` layouts --
   if a predicate misses, the picker silently falls back to the tallest table on the page.
"""

from __future__ import annotations

import atexit
import os
import re
import time
from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Sequence

import polars as pl

from sportsdataverse._html_tables import html_tables

if TYPE_CHECKING:  # pragma: no cover -- annotation-only import
    import pandas as pd

__all__ = [
    "realgm_close_browser",
    "realgm_coaches",
    "realgm_draft",
    "realgm_draft_prospects",
    "realgm_early_entry",
    "realgm_future_free_agents",
    "realgm_gms",
    "realgm_individual_games",
    "realgm_individual_seasons",
    "realgm_player_stats",
    "realgm_players",
    "realgm_players_abroad",
    "realgm_rookie_scale",
    "realgm_salary_cap",
    "realgm_standings",
    "realgm_team_stats",
    "realgm_teams",
    "realgm_transactions",
]

BASE_URL = "https://basketball.realgm.com"
_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
_CHALLENGE_TITLE = re.compile(r"just a moment|attention required|checking your browser", re.IGNORECASE)

#: ``(path, proxy) -> page HTML``. Inject one to run offline.
Fetcher = Callable[[str, Optional[str]], str]

_TTL_ENV = "SDV_PY_REALGM_TTL"
_DELAY_ENV = "SDV_PY_REALGM_DELAY"
_WAIT_ENV = "SDV_PY_REALGM_WAIT"
_POLL_ENV = "SDV_PY_REALGM_POLL"

# In-process browser session: one launch per idle window, shared by every endpoint, because
# the Cloudflare clearance cookie lives on the context and re-earning it costs ~2s a page.
_SESSION: Dict[str, Any] = {}

_DIVISION_CONFERENCE = {
    "atlantic": "Eastern",
    "central": "Eastern",
    "southeast": "Eastern",
    "northwest": "Western",
    "pacific": "Western",
    "southwest": "Western",
}


def _env_float(name: str, default: float) -> float:
    """Read a non-negative float from the environment, falling back to *default*."""
    raw = os.environ.get(name)
    if raw:
        try:
            return max(0.0, float(raw))
        except ValueError:
            pass
    return default


def _resolve_proxy(proxy: Optional[str]) -> Optional[str]:
    """Explicit ``proxy=`` > ``SDV_PY_REALGM_PROXY`` > ``SDV_PY_PROXY`` > ``None``."""
    return proxy or os.environ.get("SDV_PY_REALGM_PROXY") or os.environ.get("SDV_PY_PROXY") or None


def realgm_close_browser() -> None:
    """Close the cached headless browser, if one is open.

    The browser is otherwise kept for ``SDV_PY_REALGM_TTL`` idle seconds and released at
    interpreter exit. Call this to free it early -- after a batch pull, or before a long
    stretch of work that will not touch RealGM.

    Example:
        Pull a few endpoints, then release the browser::

            from sportsdataverse.nba.realgm import realgm_players, realgm_close_browser

            players = realgm_players()
            realgm_close_browser()
    """
    for key in ("page", "context", "browser"):
        obj = _SESSION.get(key)
        if obj is not None:
            try:
                obj.close()
            except Exception:  # noqa: BLE001 -- a dead browser must not break teardown
                pass
    pw = _SESSION.get("playwright")
    if pw is not None:
        try:
            pw.stop()
        except Exception:  # noqa: BLE001
            pass
    _SESSION.clear()


atexit.register(realgm_close_browser)


def _session_page(proxy: Optional[str]) -> Any:
    """Return the shared Playwright page, launching a browser only when needed.

    The session is dropped and relaunched when the proxy changes or the idle window
    (``SDV_PY_REALGM_TTL``) has elapsed; otherwise the existing context -- and its
    hard-won Cloudflare clearance cookie -- is reused.

    Args:
        proxy: Resolved proxy URL, or ``None``.

    Returns:
        A Playwright ``Page``.

    Raises:
        ImportError: When the optional ``playwright`` dependency is not installed.
    """
    now = time.monotonic()
    if _SESSION and (_SESSION.get("proxy") != proxy or now >= _SESSION.get("expires", 0.0)):
        realgm_close_browser()
    if not _SESSION:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError as exc:
            raise ImportError(
                "RealGM is behind a Cloudflare JavaScript challenge, so it can only be read "
                "through a real browser: `pip install sportsdataverse[pff]` (or "
                "`pip install playwright`) then `playwright install chromium`. "
                "Alternatively pass fetcher=(path, proxy) -> html to supply your own transport."
            ) from exc
        pw = sync_playwright().start()
        browser = pw.chromium.launch(
            headless=True,
            **({"proxy": {"server": proxy}} if proxy else {}),
        )
        context = browser.new_context(user_agent=_USER_AGENT)
        _SESSION.update(
            playwright=pw,
            browser=browser,
            context=context,
            page=context.new_page(),
            proxy=proxy,
            last_fetch=None,
        )
    _SESSION["expires"] = now + _env_float(_TTL_ENV, 300.0)
    return _SESSION["page"]


def _playwright_html(path: str, proxy: Optional[str] = None) -> str:
    """Fetch one RealGM page through headless Chromium, clearing the Cloudflare challenge.

    Navigates to ``BASE_URL + path`` and polls ``document.title`` until it stops being the
    challenge interstitial ("Just a moment...") or ``SDV_PY_REALGM_WAIT`` seconds elapse,
    then returns the rendered HTML. Consecutive calls are spaced by at least
    ``SDV_PY_REALGM_DELAY`` seconds and share one browser context.

    Args:
        path: Page path beginning with ``/`` (e.g. ``"/nba/players"``).
        proxy: Proxy URL passed to the browser launch; resolved from the environment
            when ``None``.

    Returns:
        The rendered page HTML (the interstitial itself, if the challenge never cleared).

    Raises:
        ImportError: When the optional ``playwright`` dependency is not installed.

    Example:
        Fetch one page directly (needs Playwright + a residential IP)::

            from sportsdataverse.nba.realgm import _playwright_html

            html = _playwright_html("/nba/players")
    """
    resolved = _resolve_proxy(proxy)
    page = _session_page(resolved)
    delay = _env_float(_DELAY_ENV, 1.0)
    last = _SESSION.get("last_fetch")
    if last is not None and delay:
        remaining = delay - (time.monotonic() - last)
        if remaining > 0:
            time.sleep(remaining)
    wait = _env_float(_WAIT_ENV, 25.0)
    poll = _env_float(_POLL_ENV, 1.5)
    page.goto(f"{BASE_URL}{path}", wait_until="domcontentloaded", timeout=int(wait * 1000) or 30000)
    deadline = time.monotonic() + wait
    while True:
        title = page.title() or ""
        if title and not _CHALLENGE_TITLE.search(title):
            break
        if time.monotonic() >= deadline:
            break
        page.wait_for_timeout(int(poll * 1000))
    html = page.content()
    _SESSION["last_fetch"] = time.monotonic()
    return str(html or "")


def _fetch(path: str, fetcher: Optional[Fetcher], proxy: Optional[str]) -> str:
    """Run *fetcher* (default: the headless-browser fetch) and return the page HTML."""
    return (fetcher or _playwright_html)(path, proxy)


def _tables(html: str, min_rows: int = 1) -> List[pl.DataFrame]:
    """Every parseable ``<table>`` on the page, cleaned, as polars frames.

    RealGM pages carry a small nav / filter / legend table ahead of the real data table,
    which ``min_rows`` filters out. The counterpart of hoopR's ``.realgm_tables()``.
    """
    frames = html_tables(html, min_rows=min_rows)
    return [frame for frame in frames.values() if isinstance(frame, pl.DataFrame)]


def _pick(tables: Sequence[pl.DataFrame], must_have: Sequence[str] = ()) -> Optional[pl.DataFrame]:
    """The tallest table carrying every ``must_have`` column, else the tallest table.

    The counterpart of hoopR's ``.realgm_pick()``: the ``must_have`` predicate is a
    preference, not a filter -- when nothing matches it, the tallest table wins, so a
    header rename upstream degrades to "probably still the right table" rather than empty.
    """
    candidates = list(tables)
    if not candidates:
        return None
    if must_have:
        matching = [t for t in candidates if all(col in t.columns for col in must_have)]
        if matching:
            candidates = matching
    return max(candidates, key=lambda t: t.height)


def _finish(frame: Optional[pl.DataFrame], return_as_pandas: bool) -> Any:
    """Return *frame* (or a zero-row frame when ``None``) as polars or pandas."""
    out = pl.DataFrame() if frame is None else frame
    return out.to_pandas() if return_as_pandas else out


def _single_table(
    path: str,
    must_have: Sequence[str],
    *,
    fetcher: Optional[Fetcher],
    proxy: Optional[str],
    return_as_pandas: bool,
    min_rows: int = 1,
) -> Any:
    """Fetch *path* and return its best data table; a zero-row frame when there is none."""
    html = _fetch(path, fetcher, proxy)
    return _finish(_pick(_tables(html, min_rows=min_rows), must_have), return_as_pandas)


def _stack(frames: Sequence[pl.DataFrame]) -> Optional[pl.DataFrame]:
    """Row-bind frames whose column sets may differ (RealGM stacks near-identical tables)."""
    kept = [f for f in frames if f.width > 0]
    if not kept:
        return None
    if len(kept) == 1:
        return kept[0]
    return pl.concat(kept, how="diagonal_relaxed")


def realgm_players(
    *,
    fetcher: Optional[Fetcher] = None,
    proxy: Optional[str] = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """The active NBA player index from RealGM.

    Port of hoopR's ``realgm_players()``. RealGM's roster of active players, including the
    pre-draft / international club detail the site is uniquely good for (Jokic ->
    "KK Mega Bemax (Serbia)").

    Args:
        fetcher: Callable ``(path, proxy) -> html``. Defaults to the headless-browser
            fetch; inject one to run offline.
        proxy: Proxy URL for the browser launch. Falls back to ``SDV_PY_REALGM_PROXY``
            then ``SDV_PY_PROXY``.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        One row per active player -- ``number``, ``player``, ``pos``, ``ht``, ``wt``,
        ``age``, ``current_team``, ``yos``, ``pre_draft_team``, ``draft_status``,
        ``nationality`` (transcribed from hoopR; unverified against live HTML). A zero-row
        frame when the page carried no data table.

    Raises:
        ImportError: When Playwright is missing and no ``fetcher`` was supplied.

    Example:
        Quick start (launches headless Chromium)::

            from sportsdataverse.nba.realgm import realgm_players

            players = realgm_players()
            print(players.shape)

        Offline / testing -- inject a transport, no browser needed::

            players = realgm_players(fetcher=lambda path, proxy: "<html>...</html>")

        Pipeline next step::

            players.filter(pl.col("nationality") != "United States").head()

        See Also:
            * `hoopR`_ -- the R original (``realgm_players()``)
            * `nba_api`_ -- first-party roster data, without the pre-draft detail

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _nba_api: https://github.com/swar/nba_api
    """
    return _single_table(
        "/nba/players",
        ("player",),
        fetcher=fetcher,
        proxy=proxy,
        return_as_pandas=return_as_pandas,
    )


def realgm_players_abroad(
    *,
    fetcher: Optional[Fetcher] = None,
    proxy: Optional[str] = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """NBA-affiliated players currently playing overseas.

    Port of hoopR's ``realgm_players_abroad()``. Draft picks, two-way and free-agent
    players on international rosters -- a view no first-party NBA/ESPN endpoint provides.

    Args:
        fetcher: Callable ``(path, proxy) -> html``; defaults to the headless-browser fetch.
        proxy: Proxy URL for the browser launch (env fallback ``SDV_PY_REALGM_PROXY``).
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        One row per player -- ``player``, ``pos``, ``ht``, ``wt``, ``nba_status``,
        ``team_s``, ``gp``, ``mpg``, ``ppg``, ``rpg``, ``apg``. Zero rows when the page
        carried no data table.

    Raises:
        ImportError: When Playwright is missing and no ``fetcher`` was supplied.

    Example:
        Quick start::

            from sportsdataverse.nba.realgm import realgm_players_abroad

            abroad = realgm_players_abroad()
            print(abroad.shape)

        See Also:
            * `hoopR`_ -- the R original (``realgm_players_abroad()``)

        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    return _single_table(
        "/nba/players-abroad",
        ("player", "nba_status"),
        fetcher=fetcher,
        proxy=proxy,
        return_as_pandas=return_as_pandas,
    )


def realgm_future_free_agents(
    *,
    fetcher: Optional[Fetcher] = None,
    proxy: Optional[str] = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """RealGM's projected future NBA free-agent classes, with each player's agent.

    Port of hoopR's ``realgm_future_free_agents()``. The ``agent`` column is the
    distinctive one -- no first-party feed publishes it.

    Args:
        fetcher: Callable ``(path, proxy) -> html``; defaults to the headless-browser fetch.
        proxy: Proxy URL for the browser launch (env fallback ``SDV_PY_REALGM_PROXY``).
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        One row per upcoming free agent -- ``player``, ``pos``, ``team``, ``season``,
        ``age``, ``yos``, ``veteran_fa_status``, ``gp``, ``pts``, ``reb``, ``ast``,
        ``per``, ``agent``. Zero rows when the page carried no data table.

    Raises:
        ImportError: When Playwright is missing and no ``fetcher`` was supplied.

    Example:
        Quick start::

            from sportsdataverse.nba.realgm import realgm_future_free_agents

            fas = realgm_future_free_agents()
            print(fas.shape)

        Pipeline next step::

            fas.group_by("agent").agg(pl.len().alias("clients")).sort("clients", descending=True)

        See Also:
            * `hoopR`_ -- the R original (``realgm_future_free_agents()``)

        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    return _single_table(
        "/nba/future_free_agents",
        ("player", "agent"),
        fetcher=fetcher,
        proxy=proxy,
        return_as_pandas=return_as_pandas,
    )


def realgm_coaches(
    *,
    fetcher: Optional[Fetcher] = None,
    proxy: Optional[str] = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Current NBA head coaches.

    Port of hoopR's ``realgm_coaches()`` (staff-role id ``20``).

    Args:
        fetcher: Callable ``(path, proxy) -> html``; defaults to the headless-browser fetch.
        proxy: Proxy URL for the browser launch (env fallback ``SDV_PY_REALGM_PROXY``).
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        One row per coach -- ``staff``, ``team``, ``start_season``, ``years_in_role``,
        ``birth_date``, ``nationality``. Zero rows when the page carried no data table.

    Raises:
        ImportError: When Playwright is missing and no ``fetcher`` was supplied.

    Example:
        Quick start::

            from sportsdataverse.nba.realgm import realgm_coaches

            coaches = realgm_coaches()
            print(coaches.shape)

        See Also:
            * `hoopR`_ -- the R original (``realgm_coaches()``)

        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    return _single_table(
        "/nba/staff-members/20/Head-Coach/Current",
        ("staff", "team"),
        fetcher=fetcher,
        proxy=proxy,
        return_as_pandas=return_as_pandas,
    )


def realgm_gms(
    *,
    fetcher: Optional[Fetcher] = None,
    proxy: Optional[str] = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Current NBA general managers.

    Port of hoopR's ``realgm_gms()`` (staff-role id ``16``).

    Args:
        fetcher: Callable ``(path, proxy) -> html``; defaults to the headless-browser fetch.
        proxy: Proxy URL for the browser launch (env fallback ``SDV_PY_REALGM_PROXY``).
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        One row per general manager -- ``staff``, ``team``, ``start_season``,
        ``years_in_role``, ``birth_date``, ``nationality``. Zero rows when the page carried
        no data table.

    Raises:
        ImportError: When Playwright is missing and no ``fetcher`` was supplied.

    Example:
        Quick start::

            from sportsdataverse.nba.realgm import realgm_gms

            gms = realgm_gms()
            print(gms.shape)

        See Also:
            * `hoopR`_ -- the R original (``realgm_gms()``)

        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    return _single_table(
        "/nba/staff-members/16/General-Manager/Current",
        ("staff", "team"),
        fetcher=fetcher,
        proxy=proxy,
        return_as_pandas=return_as_pandas,
    )


def realgm_standings(
    *,
    fetcher: Optional[Fetcher] = None,
    proxy: Optional[str] = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Current NBA standings, both conferences stacked.

    Port of hoopR's ``realgm_standings()``. The Eastern and Western conference tables are
    row-bound and labelled by a ``conference`` column, assigned **by table order** (first
    qualifying table -> Eastern) exactly as the R original does -- so a RealGM layout
    change that reorders the two tables would mislabel them.

    Args:
        fetcher: Callable ``(path, proxy) -> html``; defaults to the headless-browser fetch.
        proxy: Proxy URL for the browser launch (env fallback ``SDV_PY_REALGM_PROXY``).
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        One row per team -- ``number``, ``team``, ``w``, ``l``, ``pct``, ``gb``, ``l10``,
        ``strk``, ``ppg``, ``oppg``, ``diff``, ``home``, ``away`` plus ``conference``
        (``"Eastern"`` / ``"Western"``). Zero rows when no standings table was found.

    Raises:
        ImportError: When Playwright is missing and no ``fetcher`` was supplied.

    Example:
        Quick start::

            from sportsdataverse.nba.realgm import realgm_standings

            standings = realgm_standings()
            print(standings.shape)

        Pipeline next step::

            standings.filter(pl.col("conference") == "Eastern").head()

        See Also:
            * `hoopR`_ -- the R original (``realgm_standings()``)

        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    html = _fetch("/nba/standings", fetcher, proxy)
    labels = ("Eastern", "Western")
    parts: List[pl.DataFrame] = []
    for table in _tables(html):
        if table.height < 10 or not all(col in table.columns for col in ("team", "w", "l", "pct")):
            continue
        label = labels[min(len(parts), len(labels) - 1)]
        parts.append(table.with_columns(pl.lit(label).alias("conference")))
    return _finish(_stack(parts), return_as_pandas)


def realgm_teams(
    *,
    fetcher: Optional[Fetcher] = None,
    proxy: Optional[str] = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """The NBA team index with division and conference.

    Port of hoopR's ``realgm_teams()``. RealGM renders one small table per division, headed
    by e.g. "Atlantic Division"; the division name comes from that first header and the
    conference from a static division -> conference map.

    Args:
        fetcher: Callable ``(path, proxy) -> html``; defaults to the headless-browser fetch.
        proxy: Proxy URL for the browser launch (env fallback ``SDV_PY_REALGM_PROXY``).
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        One row per team -- ``team``, ``division``, ``conference``. Zero rows (with that
        schema) when no division table was recognised.

    Raises:
        ImportError: When Playwright is missing and no ``fetcher`` was supplied.

    Example:
        Quick start::

            from sportsdataverse.nba.realgm import realgm_teams

            teams = realgm_teams()
            print(teams.shape)

        See Also:
            * `hoopR`_ -- the R original (``realgm_teams()``)

        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    html = _fetch("/nba/teams", fetcher, proxy)
    teams: List[str] = []
    divisions: List[str] = []
    conferences: List[str] = []
    for table in _tables(html, min_rows=3):
        if table.width < 2:
            continue
        first = table.columns[0]
        if not first.endswith("_division"):
            continue
        division = first[: -len("_division")]
        conference = _DIVISION_CONFERENCE.get(division)
        if conference is None:
            continue
        names = [
            str(value).strip()
            for value in table.get_column(table.columns[1]).to_list()
            if value is not None and str(value).strip() != ""
        ]
        teams.extend(names)
        divisions.extend([division.capitalize()] * len(names))
        conferences.extend([conference] * len(names))
    frame = pl.DataFrame(
        {"team": teams, "division": divisions, "conference": conferences},
        schema={"team": pl.Utf8, "division": pl.Utf8, "conference": pl.Utf8},
    )
    return _finish(frame, return_as_pandas)


def _team_sort(stat_type: str) -> str:
    """Default sort key for the team-stats path; RealGM 404s on a key invalid for the type."""
    return "ortg" if stat_type == "Advanced_Stats" else "ppg"


def realgm_player_stats(
    season: Optional[int] = None,
    stat_type: str = "Averages",
    season_type: str = "Regular_Season",
    *,
    fetcher: Optional[Fetcher] = None,
    proxy: Optional[str] = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Season player-statistics leaderboard for one stat family and season segment.

    Port of hoopR's ``realgm_player_stats()``.

    Args:
        season: Season by **ending** year (``2026`` = 2025-26). Defaults to
            :func:`sportsdataverse.nba.nba_schedule.most_recent_nba_season`.
        stat_type: One of ``"Averages"``, ``"Totals"``, ``"Per_48"``, ``"Per_40"``,
            ``"Per_36"``, ``"Per_Minute"``, ``"Advanced_Stats"``, ``"Misc_Stats"``.
        season_type: One of ``"Regular_Season"``, ``"Playoffs"``, ``"Preseason"``,
            ``"Summer_League"``.
        fetcher: Callable ``(path, proxy) -> html``; defaults to the headless-browser fetch.
        proxy: Proxy URL for the browser launch (env fallback ``SDV_PY_REALGM_PROXY``).
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        One row per qualified player, columns varying by ``stat_type`` (for ``"Averages"``:
        ``player``, ``team``, ``gp``, ``mpg``, ``ppg``, ``rpg``, ``apg``, ...), plus the
        echoed ``season`` / ``stat_type`` / ``season_type``. Zero rows when the page
        carried no data table.

    Raises:
        ImportError: When Playwright is missing and no ``fetcher`` was supplied.

    Example:
        Quick start::

            from sportsdataverse.nba.realgm import realgm_player_stats

            stats = realgm_player_stats(season=2025, stat_type="Averages")
            print(stats.shape)

        Pipeline next step::

            stats.sort("ppg", descending=True).head(10)

        See Also:
            * `hoopR`_ -- the R original (``realgm_player_stats()``)

        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    year = int(season) if season is not None else _default_season()
    path = f"/nba/stats/{year}/{stat_type}/Qualified/points/All/desc/1/{season_type}"
    frame = _pick(_tables(_fetch(path, fetcher, proxy)), ("player",))
    return _finish(_label_stats(frame, year, stat_type, season_type), return_as_pandas)


def realgm_team_stats(
    season: Optional[int] = None,
    stat_type: str = "Averages",
    season_type: str = "Regular_Season",
    *,
    fetcher: Optional[Fetcher] = None,
    proxy: Optional[str] = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Season team statistics for one stat family and season segment.

    Port of hoopR's ``realgm_team_stats()``.

    Args:
        season: Season by **ending** year (``2026`` = 2025-26). Defaults to
            :func:`sportsdataverse.nba.nba_schedule.most_recent_nba_season`.
        stat_type: One of ``"Averages"``, ``"Totals"``, ``"Advanced_Stats"``,
            ``"Misc_Stats"``.
        season_type: One of ``"Regular_Season"``, ``"Playoffs"``, ``"Preseason"``,
            ``"Summer_League"``.
        fetcher: Callable ``(path, proxy) -> html``; defaults to the headless-browser fetch.
        proxy: Proxy URL for the browser launch (env fallback ``SDV_PY_REALGM_PROXY``).
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        One row per team (``team``, ``gp``, ``mpg``, ``ppg``, ``rpg``, ``apg``, ... for
        ``"Averages"``) plus the echoed ``season`` / ``stat_type`` / ``season_type``. Zero
        rows when the page carried no data table.

    Raises:
        ImportError: When Playwright is missing and no ``fetcher`` was supplied.

    Example:
        Quick start::

            from sportsdataverse.nba.realgm import realgm_team_stats

            teams = realgm_team_stats(season=2025, stat_type="Advanced_Stats")
            print(teams.shape)

        See Also:
            * `hoopR`_ -- the R original (``realgm_team_stats()``)

        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    year = int(season) if season is not None else _default_season()
    path = f"/nba/team-stats/{year}/{stat_type}/Team_Totals/{season_type}/{_team_sort(stat_type)}/desc"
    frame = _pick(_tables(_fetch(path, fetcher, proxy)), ("team",))
    return _finish(_label_stats(frame, year, stat_type, season_type), return_as_pandas)


def _default_season() -> int:
    """The current NBA season (ending year), imported lazily to avoid an import cycle."""
    from sportsdataverse.nba.nba_schedule import most_recent_nba_season

    return int(most_recent_nba_season())


def _label_stats(
    frame: Optional[pl.DataFrame],
    season: int,
    stat_type: str,
    season_type: str,
) -> Optional[pl.DataFrame]:
    """Echo the request parameters onto a stats frame (no-op when nothing was parsed)."""
    if frame is None:
        return None
    return frame.with_columns(
        pl.lit(season, dtype=pl.Int64).alias("season"),
        pl.lit(stat_type).alias("stat_type"),
        pl.lit(season_type).alias("season_type"),
    )


def realgm_individual_seasons(
    *,
    fetcher: Optional[Fetcher] = None,
    proxy: Optional[str] = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """The all-time best individual NBA seasons leaderboard.

    Port of hoopR's ``realgm_individual_seasons()``.

    Args:
        fetcher: Callable ``(path, proxy) -> html``; defaults to the headless-browser fetch.
        proxy: Proxy URL for the browser launch (env fallback ``SDV_PY_REALGM_PROXY``).
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        One row per player-season -- ``player``, ``season``, ``team``, ``gp``, ``min``,
        ``pts``, shooting splits, ``reb``, ``ast``, ... Zero rows when the page carried no
        data table.

    Raises:
        ImportError: When Playwright is missing and no ``fetcher`` was supplied.

    Example:
        Quick start::

            from sportsdataverse.nba.realgm import realgm_individual_seasons

            best = realgm_individual_seasons()
            print(best.shape)

        See Also:
            * `hoopR`_ -- the R original (``realgm_individual_seasons()``)

        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    return _single_table(
        "/nba/individual-seasons",
        ("player", "season"),
        fetcher=fetcher,
        proxy=proxy,
        return_as_pandas=return_as_pandas,
    )


def realgm_individual_games(
    *,
    fetcher: Optional[Fetcher] = None,
    proxy: Optional[str] = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """The all-time best individual NBA games leaderboard.

    Port of hoopR's ``realgm_individual_games()``.

    Args:
        fetcher: Callable ``(path, proxy) -> html``; defaults to the headless-browser fetch.
        proxy: Proxy URL for the browser launch (env fallback ``SDV_PY_REALGM_PROXY``).
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        One row per player-game -- ``player``, ``date``, ``team``, ``min``, ``pts``,
        ``fgm``, ``fga``, ``reb``, ``ast``, ``stl``, ``blk``, ... Zero rows when the page
        carried no data table.

    Raises:
        ImportError: When Playwright is missing and no ``fetcher`` was supplied.

    Example:
        Quick start::

            from sportsdataverse.nba.realgm import realgm_individual_games

            best = realgm_individual_games()
            print(best.shape)

        See Also:
            * `hoopR`_ -- the R original (``realgm_individual_games()``)

        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    return _single_table(
        "/nba/individual-games",
        ("player", "date"),
        fetcher=fetcher,
        proxy=proxy,
        return_as_pandas=return_as_pandas,
    )


def realgm_draft(
    year: Optional[int] = None,
    *,
    fetcher: Optional[Fetcher] = None,
    proxy: Optional[str] = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Results of one past NBA draft.

    Port of hoopR's ``realgm_draft()``. Every table carrying ``player`` / ``pos`` / ``ht``
    is stacked: the pick tables plus RealGM's listed undrafted players. ``round`` is
    derived from the overall pick number (``> 30`` -> round 2) as in the R original, and is
    null for a table with no ``pick`` column (the undrafted list).

    Args:
        year: Draft year (the calendar year the draft was held). Defaults to the most
            recently completed draft.
        fetcher: Callable ``(path, proxy) -> html``; defaults to the headless-browser fetch.
        proxy: Proxy URL for the browser launch (env fallback ``SDV_PY_REALGM_PROXY``).
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        One row per selection -- ``pick``, ``player``, ``team``, ``draft_trades``, ``pos``,
        ``ht``, ``wt``, ``age``, ``yos``, ``pre_draft_team``, ``class``, ``nationality``,
        plus ``round`` and ``draft_year``. Zero rows when no draft table was found.

    Raises:
        ImportError: When Playwright is missing and no ``fetcher`` was supplied.

    Example:
        Quick start::

            from sportsdataverse.nba.realgm import realgm_draft

            draft = realgm_draft(year=2020)
            print(draft.shape)

        Pipeline next step::

            draft.filter(pl.col("round") == 1).head()

        See Also:
            * `hoopR`_ -- the R original (``realgm_draft()``)

        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    draft_year = int(year) if year is not None else _default_season() - 1
    html = _fetch(f"/nba/draft/past-drafts/{draft_year}", fetcher, proxy)
    parts: List[pl.DataFrame] = []
    for table in _tables(html, min_rows=3):
        if not all(col in table.columns for col in ("player", "pos", "ht")):
            continue
        if "pick" in table.columns:
            picks = pl.col("pick").cast(pl.Float64, strict=False)
            # A null pick falls to the `otherwise` branch -> round 1, matching R's ifelse().
            round_expr = pl.when(picks > 30).then(2).otherwise(1).cast(pl.Int32)
        else:
            round_expr = pl.lit(None, dtype=pl.Int32)
        parts.append(table.with_columns(round_expr.alias("round")))
    stacked = _stack(parts)
    if stacked is not None:
        stacked = stacked.with_columns(pl.lit(draft_year, dtype=pl.Int64).alias("draft_year"))
    return _finish(stacked, return_as_pandas)


def realgm_draft_prospects(
    *,
    fetcher: Optional[Fetcher] = None,
    proxy: Optional[str] = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Current NBA draft-prospect statistics.

    Port of hoopR's ``realgm_draft_prospects()``.

    Args:
        fetcher: Callable ``(path, proxy) -> html``; defaults to the headless-browser fetch.
        proxy: Proxy URL for the browser launch (env fallback ``SDV_PY_REALGM_PROXY``).
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        One row per prospect -- ``player``, ``team`` (school / club), ``gp``, ``mpg``,
        ``ppg``, shooting splits, ``rpg``, ``apg``, ``spg``, ``bpg``. Zero rows when the
        page carried no data table.

    Raises:
        ImportError: When Playwright is missing and no ``fetcher`` was supplied.

    Example:
        Quick start::

            from sportsdataverse.nba.realgm import realgm_draft_prospects

            prospects = realgm_draft_prospects()
            print(prospects.shape)

        See Also:
            * `hoopR`_ -- the R original (``realgm_draft_prospects()``)

        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    return _single_table(
        "/nba/draft/prospects/stats",
        ("player", "ppg"),
        fetcher=fetcher,
        proxy=proxy,
        return_as_pandas=return_as_pandas,
    )


def realgm_early_entry(
    *,
    fetcher: Optional[Fetcher] = None,
    proxy: Optional[str] = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """The current NBA draft early-entrant and withdrawal list.

    Port of hoopR's ``realgm_early_entry()``: RealGM's college and international
    entrant/withdrawal tables stacked into one frame.

    Args:
        fetcher: Callable ``(path, proxy) -> html``; defaults to the headless-browser fetch.
        proxy: Proxy URL for the browser launch (env fallback ``SDV_PY_REALGM_PROXY``).
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        One row per candidate -- ``player``, ``pos``, ``ht``, ``wt``, ``birth_date``,
        ``college`` / ``pre_draft_team``, ``class``, ``draft_status``, ``yos``,
        ``nationality``. Zero rows when no early-entry table was found.

    Raises:
        ImportError: When Playwright is missing and no ``fetcher`` was supplied.

    Example:
        Quick start::

            from sportsdataverse.nba.realgm import realgm_early_entry

            entrants = realgm_early_entry()
            print(entrants.shape)

        See Also:
            * `hoopR`_ -- the R original (``realgm_early_entry()``)

        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    html = _fetch("/nba/draft/early_entry/by_year", fetcher, proxy)
    parts = [table for table in _tables(html, min_rows=3) if all(col in table.columns for col in ("player", "pos"))]
    return _finish(_stack(parts), return_as_pandas)


def realgm_salary_cap(
    *,
    fetcher: Optional[Fetcher] = None,
    proxy: Optional[str] = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """NBA salary-cap history and projections.

    Port of hoopR's ``realgm_salary_cap()``. Dollar figures come back as the formatted
    strings RealGM publishes (``"$140,588,000"``) -- strip non-numeric characters to get
    numerics.

    Args:
        fetcher: Callable ``(path, proxy) -> html``; defaults to the headless-browser fetch.
        proxy: Proxy URL for the browser launch (env fallback ``SDV_PY_REALGM_PROXY``).
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        One row per season -- ``season``, ``salary_cap``, ``luxury_tax``, ``x1st_apron``,
        ``x2nd_apron``, ``bae``, ``non_taxpayer_mle``, ``taxpayer_mle``, ``team_room_mle``.
        Zero rows when the page carried no data table.

    Raises:
        ImportError: When Playwright is missing and no ``fetcher`` was supplied.

    Example:
        Quick start::

            from sportsdataverse.nba.realgm import realgm_salary_cap

            caps = realgm_salary_cap()
            print(caps.shape)

        Pipeline next step -- parse the dollar strings::

            caps.with_columns(pl.col("salary_cap").str.replace_all(r"[^0-9.]", "").cast(pl.Float64))

        See Also:
            * `hoopR`_ -- the R original (``realgm_salary_cap()``)

        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    return _single_table(
        "/nba/info/salary_cap",
        ("season", "salary_cap"),
        fetcher=fetcher,
        proxy=proxy,
        return_as_pandas=return_as_pandas,
    )


def realgm_rookie_scale(
    *,
    fetcher: Optional[Fetcher] = None,
    proxy: Optional[str] = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """The current NBA rookie-scale salary table.

    Port of hoopR's ``realgm_rookie_scale()``. Dollar figures are the formatted strings
    RealGM publishes.

    Args:
        fetcher: Callable ``(path, proxy) -> html``; defaults to the headless-browser fetch.
        proxy: Proxy URL for the browser launch (env fallback ``SDV_PY_REALGM_PROXY``).
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        One row per first-round pick -- ``pick``, the four contract-year amounts, the
        4th-year option increase and the qualifying-offer increase. Zero rows when the page
        carried no data table.

    Raises:
        ImportError: When Playwright is missing and no ``fetcher`` was supplied.

    Example:
        Quick start::

            from sportsdataverse.nba.realgm import realgm_rookie_scale

            scale = realgm_rookie_scale()
            print(scale.shape)

        See Also:
            * `hoopR`_ -- the R original (``realgm_rookie_scale()``)

        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    return _single_table(
        "/nba/info/rookie_scale",
        ("pick",),
        fetcher=fetcher,
        proxy=proxy,
        return_as_pandas=return_as_pandas,
    )


def realgm_transactions(
    *,
    fetcher: Optional[Fetcher] = None,
    proxy: Optional[str] = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """The NBA league transactions log.

    Port of hoopR's ``realgm_transactions()`` -- the one non-tabular RealGM page. RealGM
    publishes transactions as a dated narrative list (``h3`` date heading + ``ul li``
    items), so this parses the DOM rather than a ``<table>``.

    Args:
        fetcher: Callable ``(path, proxy) -> html``; defaults to the headless-browser fetch.
        proxy: Proxy URL for the browser launch (env fallback ``SDV_PY_REALGM_PROXY``).
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        One row per transaction -- ``date`` (:class:`polars.Date`) and ``transaction``
        (text). Zero rows (with that schema) when no dated block parsed.

    Raises:
        ImportError: When Playwright is missing and no ``fetcher`` was supplied.

    Example:
        Quick start::

            from sportsdataverse.nba.realgm import realgm_transactions

            log = realgm_transactions()
            print(log.shape)

        Pipeline next step::

            log.filter(pl.col("transaction").str.contains("(?i)two-way")).head()

        See Also:
            * `hoopR`_ -- the R original (``realgm_transactions()``)

        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    html = _fetch("/nba/transactions/league", fetcher, proxy)
    return _finish(_parse_transactions(html), return_as_pandas)


def _parse_transactions(html: str) -> pl.DataFrame:
    """Parse the transactions page DOM into one row per dated transaction line."""
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html or "", "lxml")
    blocks = soup.select("div.transByMonth div.portal.widget.fullpage") or soup.select("div.portal.widget.fullpage")
    dates: List[Any] = []
    texts: List[str] = []
    for block in blocks:
        heading = block.find("h3")
        if heading is None:
            continue
        day = _parse_date(heading.get_text(strip=True))
        if day is None:
            continue
        for item in block.select("ul li"):
            text = " ".join(item.get_text(" ", strip=True).split())
            if text:
                dates.append(day)
                texts.append(text)
    return pl.DataFrame(
        {"date": dates, "transaction": texts},
        schema={"date": pl.Date, "transaction": pl.Utf8},
    )


def _parse_date(text: str) -> Optional[Any]:
    """Parse RealGM's ``"Aug 26, 2026"`` heading; ``None`` when it is not a date."""
    cleaned = (text or "").strip()
    for fmt in ("%b %d, %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(cleaned, fmt).date()
        except ValueError:
            continue
    return None
