"""Cache-first, proxy-bound fetch layer for stats.ncaa.org (Phase 5f, Task 5f.1).

**Provenance.** Unlike Phases 5a-5e, this module has no Scala (cbb-explorer)
counterpart to port -- the upstream project relied on an external HTTrack
crawl for its HTML fixtures, not a library-level fetch layer. This is
therefore **original sdv-py code**; no cbb-explorer / Apache-2.0 attribution
line applies. It graduates the transport proven in ``dev/ncaa_proxy.py`` (see
``dev/phase5-ncaa-proxy-proof.md`` for the live proof) into a proper library
module, following two **binding user directives** (Saiem, 2026-07-03 /
2026-07-06) that this module's tests assert structurally rather than assume:

1. **Every stats.ncaa.org request routes through a configured proxy.**
   stats.ncaa.org is IP-ban-happy; there is deliberately **no direct-fetch
   mode**. :meth:`NcaaFetcher.fetch_html` raises a clear ``RuntimeError`` if
   no proxy is configured (:func:`test_no_proxy_configured_raises
   <tests.mbb.test_mbb_ncaa_fetch>` in the test module pins this down).
2. **Cache-by-default / fetch-once.** Every fetched page is written to a
   local cache before being returned; every fetch checks the cache first and
   only ``force=True`` re-fetches. A cache hit makes **zero** transport
   calls -- also structurally tested.
3. **Creds hygiene.** This module never reads ``.Renviron``, never hardcodes
   credentials, and never logs them. Config enters via explicit constructor
   args, ``update_config()``, or ``SDV_PY_NCAA_*`` / ``SDV_PY_PROXYBONANZA_*``
   env vars (mirroring ``sportsdataverse/nfl/config.py``'s pattern). The
   ``.Renviron`` convenience reader stays dev-only in ``dev/ncaa_proxy.py``.
   :class:`NcaaFetchConfig`'s ``__repr__`` redacts key/password material.

Transport is `curl_cffi <https://github.com/lexiforest/curl_cffi>`_ with
``impersonate="chrome"`` (stats.ncaa.org TLS/JA3-blocks plain ``requests``,
the same reason ``sportsdataverse/nba/nba_stats_runtime.py`` uses it for
stats.nba.com) -- a **lazy optional import** so importing this module never
requires ``curl_cffi`` to be installed; only the real (non-injected) HTTP
path does. Tests inject a fake transport and never touch the network.

Cache-key scheme: ``{cache_dir}/stats.ncaa.org/{path-segments}/{last}.html``,
mirroring the URL path as nested directories. A query string (e.g. box score
``?period_no=2``) cannot be a directory component, so it is appended,
sanitized, to the final filename: ``..._{safe_query}.html`` where unsafe
characters are replaced with ``_`` -- deterministic and collision-free across
distinct query strings for the same base path. See :func:`cached_path`.

URL shapes (path builders): the modern/legacy split follows stats.ncaa.org's
confirmed dual team-id scheme (hoopR's ``ncaa_mbb_data.R`` documents legacy
``/team/{team_id}/{season_id}`` vs modern ``/teams/{season_team_id}``) plus
the play-by-play/box-score shapes named directly in the Phase 5f plan's
Task 5f.2 live-proof probe list. The roster/schedule shapes below follow the
same modern/legacy convention by analogy but are **not yet independently
confirmed live** (stats.ncaa.org's exact roster path was not in any indexed
source) -- Task 5f.2's live proof should confirm or correct them; this is
fetch-layer plumbing, not a parser, so a wrong path is a one-line fix later.

**Two transports, by page class (proven live 2026-07-07, Task 5f.2/5f.3).**
stats.ncaa.org splits into un-challenged pages and Akamai-``bm-verify``-gated
game-detail pages, so there are two transports:

* **Un-challenged pages** -- landing/index/team pages: ``/`` (~20 KB),
  ``/team/{id}`` (~17 KB), ``/season_divisions`` (~10 KB). The default
  ``curl_cffi`` transport (Chrome-impersonation, **proxy-bound** per directive
  1) fetches these directly.
* **Game-detail pages** (``.../play_by_play``, ``.../individual_stats``,
  ``.../box_score``) -- behind an Akamai BotManager ``bm-verify`` JS
  proof-of-work. ``curl_cffi`` clears the TLS/JA3 edge but **cannot** run the
  sensor. **The suggested method here is the browser transport**
  (:func:`playwright_transport` / :meth:`NcaaFetcher.with_browser`): a real
  Chromium driven by Playwright in **Chrome new-headless** (``--headless=new``)
  clears the challenge, then serves the raw server HTML the Phase 5a-5e parsers
  consume. It runs fully headless (no window) on any host with a real GPU.

**Three load-bearing details, all required together (re-established live 2026-07-16
after Akamai tightened -- the earlier "anti-detect is unnecessary" note was
FALSIFIED).** (1) **new-headless** (``--headless=new``) renders through the real
GPU/ANGLE path; the default ``headless=True``/old ``headless_shell`` uses the
SwiftShader software-GPU renderer (``"Google SwiftShader"``), a textbook Akamai
tell. (2) **patchright, not vanilla Playwright** -- vanilla Playwright leaks
``navigator.webdriver=true`` + the ``Runtime.enable`` CDP tell and gets
*challenged*; patchright patches both. (3) **a real Chrome ``user_agent``** --
new-headless otherwise reports ``HeadlessChrome`` in ``navigator.userAgent``, the
single tell that broke every prior attempt. With all three + a **residential** IP
(datacenter gets an instant edge 403 regardless of browser), bm-verify clears
(proven: 10-game canary, real 100 KB+ pages, ~11 s/page warm). Ceiling: a GPU-less
headless CI box falls back to SwiftShader and is re-detected; run on a real-GPU host.

**patchright stays an OPTIONAL import** (lazy, like ``curl_cffi``): importing
this module never requires it; only :func:`playwright_transport` does, with a
clear ``ImportError`` + install hint on first use. Game-id **discovery**
(scoreboards / ``teams/{id}/game_by_game`` are JS-rendered, no server-side ids)
also needs the browser and remains a producer/scraper concern. The offline
fixture oracle (Phases 5a-5e) is the validated parse path. See
``dev/phase5f-live-proof.md`` for the full live characterization + matrix.
"""

from __future__ import annotations

import json
import logging
import math
import os
import re
import time
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import urlsplit

logger = logging.getLogger(__name__)

NCAA_HOST = "stats.ncaa.org"
NCAA_HOST_URL = f"https://{NCAA_HOST}"

# Substrings observed (or generically expected) in stats.ncaa.org's
# ban/rate-limit/WAF responses. A 200 status with one of these markers present
# is treated as a failed fetch so the caller rotates proxies rather than caching
# a ban page. Markers must be WAF-specific phrases: a bare ``"blocked"`` is a
# false-positive landmine on real content -- basketball play-by-play says
# "layup blocked", so the marker is the full WAF phrase "you have been blocked".
_BAN_MARKERS = (
    "access denied",
    "forbidden",
    "you have been blocked",
    "ip has been blocked",
    "captcha",
    "cf-browser-verification",
    "attention required",
    "rate limit",
)

# An unsolved bm-verify has TWO observed shapes, because Akamai answers a
# navigation differently from an in-page fetch:
#   * navigation  -> the full ~2.3 KB interstitial carrying the sensor markers
#                    (this is what curl_cffi sees, with no cookie at all).
#   * in-page XHR -> a THIN stub when ``_abck`` is missing/invalid. Observed
#                    live as a 15-byte body: just ``NCAA Statistics``.
# The stub carries no markers and no ban text, so marker-matching alone misses
# it -- which is how unsolved challenges were mistaken for successful fetches.
# Any stats.ncaa.org page worth having is >= 10 KB (game pages run 100 KB+), so
# a sub-1 KB body is never content.
_CHALLENGE_MARKERS = ("bm-verify", "_abck")
_CHALLENGE_MAX_BYTES = 20000
_MIN_CONTENT_BYTES = 1000

PoolTransport = Callable[[str, "dict[str, str]"], "tuple[int, str]"]
FetchTransport = Callable[[str, "dict[str, str]", "dict[str, str]"], "tuple[int, str]"]


@dataclass
class NcaaFetchConfig:
    """Runtime configuration for the stats.ncaa.org fetch layer.

    Exactly one proxy source should be configured: either a single explicit
    ``proxy_url`` (``http://login:password@ip:port``), or a ProxyBonanza pool
    via ``proxybonanza_key`` + ``proxybonanza_pkg`` (resolved lazily by
    :class:`NcaaFetcher` via :func:`load_proxybonanza_pool`). Leaving both
    unset is valid at construction time -- it simply means every
    :meth:`NcaaFetcher.fetch_html` call that misses cache will raise (the
    binding no-direct-fetch directive).

    Example:
        Inspect defaults via ``get_config()``::

            from sportsdataverse.mbb.mbb_ncaa_fetch import get_config
            cfg = get_config()
            cfg.cache_dir     # ~/.sportsdataverse/ncaa_cache
            cfg.impersonate   # "chrome"

        Configure a single proxy explicitly (rarely needed -- prefer
        ``update_config`` or the env vars)::

            from sportsdataverse.mbb.mbb_ncaa_fetch import NcaaFetchConfig
            cfg = NcaaFetchConfig(proxy_url="http://user:pass@1.2.3.4:8080")
    """

    # Optional[Path] (not a bare Path default) so the generated __repr__ /
    # Sphinx signature stays JSX-safe -- same rationale as
    # sportsdataverse/nfl/config.py's NflConfig.cache_dir. Always concrete
    # after __post_init__.
    cache_dir: Optional[Path] = None
    proxy_url: Optional[str] = None
    proxybonanza_key: Optional[str] = None
    proxybonanza_pkg: Optional[str] = None
    timeout: int = 45
    impersonate: str = "chrome"
    max_retries: int = 2
    # Seconds to sleep between proxy-rotation attempts (retry path only);
    # paces a ban wave instead of bursting the pool.
    rotation_backoff: float = 1.0
    # PROACTIVE rotation: advance to the next proxy after this many successful
    # fetches (0 = never rotate until something fails). stats.ncaa.org bans per
    # IP and those bans are PERMANENT -- IPs are a consumable budget, so the
    # goal is to never burn one. A single IP was observed serving ~4200 requests
    # before degrading; spreading a season (~11.7k requests) over a 50-proxy
    # pool at 200/proxy keeps every IP an order of magnitude under that.
    rotate_every: int = 200
    # Injection point for tests / alternate transports. ``None`` -> the real
    # curl_cffi-backed transport built lazily by NcaaFetcher.
    transport: Optional[FetchTransport] = None

    def __post_init__(self) -> None:
        if self.cache_dir is None:
            self.cache_dir = Path.home() / ".sportsdataverse" / "ncaa_cache"

    def __repr__(self) -> str:
        """Redact key/password/proxy-auth material -- never log secrets."""
        return (
            "NcaaFetchConfig("
            f"cache_dir={self.cache_dir!r}, "
            f"proxy_url={_redact_proxy_url(self.proxy_url)}, "
            f"proxybonanza_key={'<redacted>' if self.proxybonanza_key else None!r}, "
            f"proxybonanza_pkg={self.proxybonanza_pkg!r}, "
            f"timeout={self.timeout}, impersonate={self.impersonate!r}, "
            f"max_retries={self.max_retries}, "
            f"rotation_backoff={self.rotation_backoff}, "
            f"rotate_every={self.rotate_every}, "
            f"transport={'<custom>' if self.transport else None})"
        )


def _redact_proxy_url(url: Optional[str]) -> str:
    """Mask ``login:password`` in a ``http://login:password@host:port`` URL."""
    if not url:
        return "None"
    m = re.match(r"^(https?://)[^:@/]+:[^@/]+@(.+)$", url)
    if m:
        return f"{m.group(1)}<redacted>:<redacted>@{m.group(2)}"
    return "<redacted>"


def _from_env() -> NcaaFetchConfig:
    """Build an ``NcaaFetchConfig`` from ``SDV_PY_NCAA_*`` / ``SDV_PY_PROXYBONANZA_*``.

    Precedence at runtime is: explicit ``update_config()`` > env var >
    default. Invalid ints are silently ignored (a typo in a shell rc file
    should not break imports); inspect ``get_config()`` to see the effective
    value.
    """
    cfg = NcaaFetchConfig()
    if (v := os.environ.get("SDV_PY_NCAA_CACHE_DIR")) is not None:
        cfg.cache_dir = Path(v).expanduser()
    if (v := os.environ.get("SDV_PY_NCAA_PROXY_URL")) is not None:
        cfg.proxy_url = v
    if (v := os.environ.get("SDV_PY_PROXYBONANZA_KEY")) is not None:
        cfg.proxybonanza_key = v
    if (v := os.environ.get("SDV_PY_PROXYBONANZA_PKG")) is not None:
        cfg.proxybonanza_pkg = v
    if (v := os.environ.get("SDV_PY_NCAA_TIMEOUT")) is not None:
        try:
            cfg.timeout = int(v)
        except ValueError:
            pass
    if (v := os.environ.get("SDV_PY_NCAA_IMPERSONATE")) is not None:
        cfg.impersonate = v
    if (v := os.environ.get("SDV_PY_NCAA_ROTATION_BACKOFF")) is not None:
        try:
            parsed = float(v)
        except ValueError:
            parsed = None
        # Reject non-finite values (inf/nan): they would reach time.sleep() on
        # the retry path and raise OverflowError/ValueError.
        if parsed is not None and math.isfinite(parsed):
            cfg.rotation_backoff = parsed
    if (v := os.environ.get("SDV_PY_NCAA_ROTATE_EVERY")) is not None:
        try:
            cfg.rotate_every = int(v)
        except ValueError:
            pass
    return cfg


_config: NcaaFetchConfig = _from_env()


def get_config() -> NcaaFetchConfig:
    """Return the live ``NcaaFetchConfig`` singleton.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_fetch import get_config
            cfg = get_config()
            print(cfg.cache_dir, cfg.timeout)
    """
    return _config


def update_config(**kwargs: object) -> NcaaFetchConfig:
    """Update the active config in place.

    Args:
        **kwargs: Field name -> new value pairs (``NcaaFetchConfig`` fields).

    Returns:
        The (mutated) global config object.

    Raises:
        ValueError: If a passed key does not correspond to a field.

    Example:
        Point the fetch layer at a single explicit proxy::

            from sportsdataverse.mbb.mbb_ncaa_fetch import update_config
            update_config(proxy_url="http://user:pass@1.2.3.4:8080")
    """
    global _config
    for key, value in kwargs.items():
        if not hasattr(_config, key):
            raise ValueError(f"Unknown config key: {key!r}")
        if key == "cache_dir" and isinstance(value, str):
            value = Path(value).expanduser()
        setattr(_config, key, value)
    return _config


def reset_config() -> NcaaFetchConfig:
    """Reset the active config to its env-var-derived defaults.

    Example:
        Restore defaults after a session of tweaks::

            from sportsdataverse.mbb.mbb_ncaa_fetch import update_config, reset_config
            update_config(timeout=5)
            reset_config()
    """
    global _config
    _config = _from_env()
    return _config


def _default_pool_transport(url: str, headers: "dict[str, str]") -> "tuple[int, str]":
    try:
        import curl_cffi
    except ImportError as exc:  # pragma: no cover - exercised only on the live path
        raise ImportError(
            "Loading the ProxyBonanza pool requires curl_cffi. Install with: "
            "pip install curl_cffi or pip install sportsdataverse[all]"
        ) from exc
    r = curl_cffi.get(url, headers=headers, timeout=30, impersonate="chrome")
    return r.status_code, r.text


def load_proxybonanza_pool(
    api_key: str,
    pkg: str,
    *,
    transport: Optional[PoolTransport] = None,
) -> "list[str]":
    """Resolve a ProxyBonanza package into a list of ``http://login:pass@ip:port`` URLs.

    Graduated from ``dev/ncaa_proxy.py``'s ``load_proxy_pool`` -- same
    endpoint shape, minus the ``.Renviron`` reader (creds are now explicit
    params, per the creds-hygiene directive).

    Endpoint: ``GET https://api.proxybonanza.com/v1/userpackages/{pkg}.json``
    with header ``Authorization: {api_key}``. Response shape::

        {"data": {"login": "...", "password": "...",
                  "ippacks": [{"ip": "1.2.3.4", "port_http": 8080}, ...]}}

    Args:
        api_key: ProxyBonanza API key.
        pkg: ProxyBonanza package id.
        transport: Injectable ``(url, headers) -> (status, text)`` callable
            for offline testing. Defaults to a curl_cffi GET.

    Returns:
        One ``http://login:password@ip:port`` URL per IP in the package.

    Raises:
        RuntimeError: Non-200 response.

    Example:
        Offline (injected transport)::

            def fake(url, headers):
                return 200, '{"data": {"login": "u", "password": "p", "ippacks": []}}'
            pool = load_proxybonanza_pool("key", "pkg", transport=fake)
    """
    url = f"https://api.proxybonanza.com/v1/userpackages/{pkg}.json"
    _transport = transport or _default_pool_transport
    status, text = _transport(url, {"Authorization": api_key})
    if status != 200:
        raise RuntimeError(f"ProxyBonanza pool fetch failed: status={status}")
    data = json.loads(text)["data"]
    login, password = data["login"], data["password"]
    return [f"http://{login}:{password}@{pk['ip']}:{pk['port_http']}" for pk in data["ippacks"]]


def _ban_check(text: str) -> str:
    """Return ``"clean"`` or ``"BAN-SUSPECT:<marker>"`` for a response body."""
    low = text.lower()
    for marker in _BAN_MARKERS:
        if marker in low:
            return f"BAN-SUSPECT:{marker}"
    return "clean"


def _is_challenge(text: str) -> bool:
    """Is *text* an UNSOLVED ``bm-verify`` response -- neither content nor a ban?

    The third response class, and the one that used to slip through: an unsolved
    challenge is HTTP 200 and carries no ban marker, so :func:`_ban_check` calls
    it ``"clean"`` and the fetch layer returned it as a successful fetch. Callers
    then rejected it as too-small/table-less while the fetcher, believing it had
    succeeded, never re-solved and never rotated -- so a single failed solve
    turned into an unbounded run of useless requests.

    Both observed shapes are caught:

    * the ~2.3 KB navigation interstitial (matched by sensor marker + size), and
    * the THIN in-page-fetch stub returned when ``_abck`` is invalid -- observed
      live as 15 bytes of ``NCAA Statistics``, which carries no marker at all and
      is only detectable by size.
    """
    if not text or len(text) > _CHALLENGE_MAX_BYTES:
        return False
    low = text.lower()
    return any(marker in low for marker in _CHALLENGE_MARKERS)


def _browser_response_unsolved(text: str) -> bool:
    """Did an in-page browser fetch come back UNSOLVED?

    Only the browser transport may use this. It knows two things the generic
    fetch layer does not: the response came from an in-page ``fetch()`` against
    stats.ncaa.org, and every page worth having there is >= 10 KB. That licenses
    the size test which catches the marker-less thin stub (15 bytes observed).
    :func:`_is_challenge` deliberately does NOT apply a size floor -- it judges
    responses from every transport, where a small body may be legitimate.
    """
    return _is_challenge(text) or not text or len(text) < _MIN_CONTENT_BYTES


def _normalize_path(path_or_url: str) -> str:
    """Strip a ``https://stats.ncaa.org/...`` URL down to its bare path+query.

    Accepts a bare path (``"contests/123/play_by_play"``, leading ``/``
    optional) or a full URL for ``stats.ncaa.org`` only.

    Raises:
        ValueError: *path_or_url* is a full URL for a different host.
    """
    if path_or_url.startswith("http://") or path_or_url.startswith("https://"):
        parsed = urlsplit(path_or_url)
        if parsed.netloc != NCAA_HOST:
            raise ValueError(f"NcaaFetcher only fetches {NCAA_HOST!r}; got host {parsed.netloc!r}")
        path = parsed.path.lstrip("/")
        return f"{path}?{parsed.query}" if parsed.query else path
    return path_or_url.lstrip("/")


# Query-string characters kept as-is in a cache filename; everything else
# becomes "_". Deterministic across runs and collision-free for the query
# shapes this fetcher builds (``period_no=2``, etc.).
_QUERY_SAFE_RE = re.compile(r"[^A-Za-z0-9=&_.-]")


def cached_path(path: str, *, cache_dir: Optional[Path] = None) -> Path:
    """Return the on-disk cache file path for *path*, without touching it.

    Layout: ``{cache_dir}/stats.ncaa.org/{dirs...}/{last}.html``, where the
    URL path's ``/``-separated segments become nested directories and a
    query string is folded into the final filename as ``__{safe_query}.html``
    (unsafe characters replaced with ``_``). Two different query strings for
    the same base path therefore always produce two distinct cache files.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_fetch import cached_path
            cached_path("contests/4690813/play_by_play")
            # .../stats.ncaa.org/contests/4690813/play_by_play.html
            cached_path("contests/4690813/box_score?period_no=2")
            # .../stats.ncaa.org/contests/4690813/box_score__period_no=2.html
    """
    root = cache_dir if cache_dir is not None else get_config().cache_dir
    assert root is not None  # always concrete after NcaaFetchConfig.__post_init__
    norm = _normalize_path(path)
    base, _, query = norm.partition("?")
    base = base.strip("/")
    segments = base.split("/") if base else ["index"]
    suffix = f"__{_QUERY_SAFE_RE.sub('_', query)}" if query else ""
    segments[-1] = f"{segments[-1]}{suffix}.html"
    return Path(root, NCAA_HOST, *segments)


def is_cached(path: str, *, cache_dir: Optional[Path] = None) -> bool:
    """Return whether *path* already has a cache file on disk."""
    return cached_path(path, cache_dir=cache_dir).exists()


# The in-page fetch that returns the RAW server HTML (not the post-JS DOM the
# parsers would choke on) while carrying the browser's bm-verify-solved cookies.
_RAW_FETCH_JS = (
    "async (u) => { const r = await fetch(u, {credentials:'include'}); "
    "return {status: r.status, text: await r.text()}; }"
)


class _PlaywrightTransport:
    """Stateful `FetchTransport` that drives an anti-detect Chromium (patchright)
    to clear the Akamai ``bm-verify`` challenge, then returns raw server HTML.

    **This is the suggested method for scraping stats.ncaa.org game-detail
    pages** (play-by-play / individual-stats / box-score), which sit behind an
    Akamai BotManager JS proof-of-work that ``curl_cffi`` cannot clear. Built
    via :func:`playwright_transport` and wired through
    :attr:`NcaaFetchConfig.transport` (or the :meth:`NcaaFetcher.with_browser`
    convenience). Re-proven live 2026-07-16 (10-game canary PASS).

    **Three tells must ALL be neutralized** (see the module docstring): (1)
    **new-headless** (``--headless=new``) → real GPU/ANGLE, not the
    ``"Google SwiftShader"`` of old ``headless_shell``; (2) **patchright** patches
    ``navigator.webdriver`` + the ``Runtime.enable`` CDP leak that get *vanilla*
    Playwright challenged; (3) a **real Chrome ``user_agent``** (new-headless
    leaks ``HeadlessChrome`` otherwise). Plus a **residential** proxy -- a
    datacenter IP gets an instant edge 403 no matter how clean the browser is.
    Caveat: a GPU-less headless CI box falls back to SwiftShader and is
    re-detected -- run on a real-GPU host.

    Reuses ONE browser across the whole session: the first fetch navigates to
    mint the ``_abck`` cookie, every fetch (including the first) reads raw
    bytes via an in-page cookie-carrying ``fetch()``. Close it when done
    (context-manager, :meth:`close`, or the ``atexit`` safety net).
    """

    def __init__(
        self,
        *,
        headless_new: bool = True,
        challenge_wait_ms: int = 8000,
        nav_timeout_ms: int = 45000,
        user_agent: Optional[str] = None,
        solve_attempts: int = 3,
        relaunch_backoff: float = 2.0,
    ) -> None:
        self.headless_new = headless_new
        self.challenge_wait_ms = challenge_wait_ms
        self.nav_timeout_ms = nav_timeout_ms
        # Seconds to let a rotated-off context fully tear down before relaunching a
        # new one. A proxy rotation relaunches only the browser CONTEXT (not the
        # Playwright driver); rapid back-to-back relaunches with no settle crashed
        # patchright's driver (EPIPE) during a whole-season backfill. See _ensure_page.
        self.relaunch_backoff = max(0.0, relaunch_backoff)
        # How many times to (re-)run the bm-verify sensor before giving up and
        # letting the fetch layer rotate. The solve is PROVEN by the in-page
        # fetch, never assumed -- see __call__. Kept LOW on purpose: each
        # attempt costs a navigation + challenge_wait_ms, and when an IP simply
        # is not passing the sensor, rotating to a fresh proxy (fresh browser,
        # fresh sensor run) recovers far faster than re-waiting on the same one.
        self.solve_attempts = max(1, solve_attempts)
        # A REAL Chrome UA is load-bearing: new-headless otherwise reports
        # "HeadlessChrome" in navigator.userAgent, and that single tell is what made
        # every pre-2026-07-16 solve attempt fail. Overriding it (any real Chrome UA)
        # is half the fix; anti-detect patchright (webdriver=false) is the other half.
        self.user_agent = user_agent or (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
        )
        # Any (not object): these hold optional-import Playwright handles; the
        # module never imports playwright at type-check time.
        self._pw: Any = None
        self._browser: Any = None
        self._page: Any = None
        # Browser profile dir, tied to this transport's lifetime -- removed in
        # close() so a per-launch temp dir does not leak across proxy rotations.
        self._temp_dir: Any = None
        self._challenge_solved = False
        # The proxy this browser was LAUNCHED with. Playwright binds the proxy at
        # launch, so honoring a rotation means relaunching -- see _ensure_page.
        self._current_proxy: Optional[str] = None

    def _ensure_page(self, proxies: "dict[str, str]") -> None:
        proxy = proxies.get("http") or proxies.get("https") or None
        if self._page is not None:
            if proxy == self._current_proxy:
                return
            # The caller rotated the proxy. A live context is pinned to its LAUNCH
            # proxy, so reusing it would keep egressing from the old IP. Close only
            # the CONTEXT (keeping the Playwright driver alive), let it settle, then
            # relaunch on the new IP -- stop/starting the whole driver per rotation
            # crashed patchright (EPIPE) under a backfill's many rotations.
            logger.info("browser proxy rotated -> %s (relaunching context)", _redact_proxy_url(proxy))
            self._close_context()
            if self.relaunch_backoff:
                time.sleep(self.relaunch_backoff)
        if self._pw is None:  # start the Playwright DRIVER once; reuse across rotations
            try:
                from patchright.sync_api import sync_playwright
            except ImportError as exc:  # pragma: no cover - exercised only without patchright
                raise ImportError(
                    "The NCAA browser transport requires patchright -- an anti-detect "
                    "Playwright fork. Game-detail pages sit behind Akamai bm-verify, and "
                    "vanilla Playwright's navigator.webdriver=true is flagged (only "
                    "patchright, which patches that + the Runtime.enable CDP leak, clears "
                    "it). Install with: pip install patchright && patchright install "
                    "chromium. patchright is intentionally NOT a hard sportsdataverse "
                    "dependency."
                ) from exc
            import atexit

            self._pw = sync_playwright().start()
            atexit.register(self.close)
        import tempfile

        # launch_persistent_context (not launch + new_context) is the more stealthy
        # path patchright recommends. new-headless renders through the real GPU/ANGLE
        # (not the SwiftShader tell). The user_agent override is load-bearing (see
        # __init__). Proven live 2026-07-16: this exact shape clears bm-verify where
        # vanilla Playwright and a leaky-UA browser both failed.
        # Retain the profile dir on the instance so close() can remove it; a bare
        # mkdtemp leaked one dir per launch, and the browser relaunches on every
        # proxy rotation. TemporaryDirectory also GC-finalizes as a backstop.
        self._temp_dir = tempfile.TemporaryDirectory(prefix="ncaa_pw_")
        launch_kwargs: "dict[str, object]" = {
            "user_data_dir": self._temp_dir.name,
            "user_agent": self.user_agent,
            "no_viewport": True,
        }
        launch_kwargs.update(
            {"headless": False, "args": ["--headless=new"]} if self.headless_new else {"headless": True}
        )
        proxy = proxies.get("http") or proxies.get("https")
        if proxy:  # honor a configured RESIDENTIAL proxy; datacenter IPs get an edge 403
            parts = urlsplit(proxy)
            launch_kwargs["proxy"] = {
                "server": f"{parts.scheme}://{parts.hostname}:{parts.port}",
                **({"username": parts.username} if parts.username else {}),
                **({"password": parts.password} if parts.password else {}),
            }
        # launch_persistent_context returns a BrowserContext (closeable) -- kept in
        # self._browser so close() works unchanged.
        self._browser = self._pw.chromium.launch_persistent_context(**launch_kwargs)
        self._page = self._browser.pages[0] if self._browser.pages else self._browser.new_page()
        self._current_proxy = proxy

    def _close_context(self) -> None:
        """Close ONLY the browser context + its profile dir, keeping the Playwright
        DRIVER (``self._pw``) alive so a proxy rotation relaunches just the context.
        Repeatedly stop/starting the driver process is what crashed patchright
        (EPIPE) mid-backfill."""
        try:
            if self._browser is not None:
                self._browser.close()
        except Exception:  # noqa: BLE001 - best-effort teardown
            pass
        if self._temp_dir is not None:
            try:
                self._temp_dir.cleanup()
            except Exception:  # noqa: BLE001 - best-effort; a lingering file lock may defer it
                pass
            self._temp_dir = None
        self._browser = self._page = None
        self._challenge_solved = False
        self._current_proxy = None

    def _solve_challenge(self, url: str) -> None:
        """Run a real navigation so the bm-verify sensor executes and mints ``_abck``.

        Gives the sensor up to :attr:`solve_attempts` cycles to clear the
        interstitial. Note this only observes the NAVIGATION; whether the token
        is actually usable is proven by the in-page fetch in :meth:`__call__`.
        """
        page = self._page
        page.goto(url, wait_until="domcontentloaded", timeout=self.nav_timeout_ms)
        for _ in range(self.solve_attempts):
            page.wait_for_timeout(self.challenge_wait_ms)
            if "bm-verify" not in page.content():
                break  # interstitial cleared

    def __call__(self, url: str, proxies: "dict[str, str]", headers: "dict[str, str]") -> "tuple[int, str]":
        self._ensure_page(proxies)
        status, text = 0, ""
        for attempt in range(1, self.solve_attempts + 1):
            if not self._challenge_solved:
                self._solve_challenge(url)
                self._challenge_solved = True
            result = self._page.evaluate(_RAW_FETCH_JS, url)
            status, text = int(result["status"]), str(result["text"])
            if not _browser_response_unsolved(text):
                return status, text
            # The solve did NOT take -- either it never passed, or Akamai
            # re-challenged once _abck aged out. The old code set
            # _challenge_solved=True after a BLIND wait and never checked, so a
            # failed solve latched "solved" and every later fetch returned an
            # unsolved response forever (1485 of them in one live run). The
            # fetch itself is the only honest proof; force a real re-solve.
            logger.info(
                "bm-verify not passed (%d-byte response) -- re-solving, attempt %d/%d",
                len(text),
                attempt,
                self.solve_attempts,
            )
            self._challenge_solved = False
        # Still unsolved after every attempt. Raise so the fetch layer's
        # rotate-on-transport-error path moves us to a fresh proxy -- which
        # relaunches the browser and runs a genuinely fresh sensor. Returning
        # the stub instead is what let the fetcher mistake it for a success.
        raise RuntimeError(
            f"bm-verify not passed after {self.solve_attempts} attempts ({len(text)}-byte response): {url}"
        )

    def close(self) -> None:
        """Full teardown: close the context, stop the Playwright driver, remove the
        profile dir. Idempotent."""
        self._close_context()  # context + temp dir + solved/proxy state
        try:
            if self._pw is not None:
                self._pw.stop()
        except Exception:  # noqa: BLE001 - best-effort teardown
            pass
        self._pw = None

    def __enter__(self) -> "_PlaywrightTransport":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


def playwright_transport(
    *,
    headless_new: bool = True,
    challenge_wait_ms: int = 8000,
    nav_timeout_ms: int = 45000,
    user_agent: Optional[str] = None,
    solve_attempts: int = 3,
    relaunch_backoff: float = 2.0,
) -> "_PlaywrightTransport":
    """Build the **suggested** stats.ncaa.org game-detail scraping transport.

    Drives a real Chromium via Playwright in Chrome's new-headless mode
    (``--headless=new``) to clear the Akamai ``bm-verify`` challenge that
    ``curl_cffi`` cannot, then serves raw server HTML for the 5a-5e parsers.
    Playwright is a **lazy optional import** (not a hard dependency); a clear
    ``ImportError`` fires on first use if it is missing.

    Args:
        headless_new: Use ``--headless=new`` (real-GPU render, no window) --
            the default and the proven-working mode. ``False`` runs old
            headless (``headless_shell``), which Akamai flags -- avoid.
        challenge_wait_ms: Milliseconds to let the bm-verify sensor run after
            the first navigation.
        nav_timeout_ms: Per-navigation timeout.
        user_agent: Override the Chrome UA string.

    Returns:
        A stateful, callable ``FetchTransport`` reusing one browser for the
        session. Close it when done (it is a context manager, has ``close()``,
        and registers an ``atexit`` safety net).

    Example:
        Scrape a game end-to-end (the recommended path)::

            from sportsdataverse.mbb.mbb_ncaa_fetch import NcaaFetcher
            with NcaaFetcher.with_browser() as fetcher:
                pbp = fetcher.fetch_game_pbp("1613299")               # raw PBP HTML
                box = fetcher.fetch_game_individual_stats("1613299")  # raw box HTML
            # -> feed to get_box_lineup / create_lineup_data (mbb_ncaa_*_parser)
    """
    return _PlaywrightTransport(
        headless_new=headless_new,
        challenge_wait_ms=challenge_wait_ms,
        nav_timeout_ms=nav_timeout_ms,
        user_agent=user_agent,
        solve_attempts=solve_attempts,
        relaunch_backoff=relaunch_backoff,
    )


class NcaaFetcher:
    """Cache-first stats.ncaa.org fetcher, proxy-bound per the binding directive.

    Example:
        Scrape game-detail data (the **suggested** path -- browser transport
        clears the Akamai bm-verify wall; see :meth:`with_browser`)::

            from sportsdataverse.mbb.mbb_ncaa_fetch import NcaaFetcher
            with NcaaFetcher.with_browser() as fetcher:
                pbp = fetcher.fetch_game_pbp("1613299")               # raw PBP HTML
                box = fetcher.fetch_game_individual_stats("1613299")  # raw box HTML

        Un-challenged pages (landing / team) via the curl_cffi proxy path::

            from sportsdataverse.mbb.mbb_ncaa_fetch import NcaaFetcher, update_config
            update_config(proxy_url="http://user:pass@1.2.3.4:8080")
            fetcher = NcaaFetcher()
            html = fetcher.fetch_team_schedule("391")  # cached after this call

        Offline (injected transport + explicit pool, no network/env needed)::

            def fake(url, proxies, headers):
                return 200, "<html>...</html>"
            from sportsdataverse.mbb.mbb_ncaa_fetch import NcaaFetchConfig
            cfg = NcaaFetchConfig(cache_dir=tmp_path, transport=fake)
            fetcher = NcaaFetcher(cfg, proxy_pool=["http://u:p@1.1.1.1:1"])
    """

    def __init__(
        self,
        config: Optional[NcaaFetchConfig] = None,
        *,
        proxy_pool: Optional["list[str]"] = None,
    ) -> None:
        self.config = config or get_config()
        if proxy_pool is not None:
            self._pool: "list[str]" = list(proxy_pool)
        elif self.config.proxy_url:
            self._pool = [self.config.proxy_url]
        elif self.config.proxybonanza_key and self.config.proxybonanza_pkg:
            self._pool = load_proxybonanza_pool(self.config.proxybonanza_key, self.config.proxybonanza_pkg)
        else:
            self._pool = []
        self._proxy_idx = 0
        # Successful fetches on the CURRENT proxy (drives config.rotate_every).
        self._since_rotate = 0
        # Proxies that returned a ban this session. stats.ncaa.org's per-IP bans
        # are permanent, so a burned proxy must never be handed out again --
        # otherwise rotation cycles straight back into a known-dead IP.
        self._dead: "set[str]" = set()
        self._session: object = None  # lazy curl_cffi.Session (real transport only)

    @classmethod
    def with_browser(
        cls,
        config: Optional[NcaaFetchConfig] = None,
        *,
        proxy_pool: Optional["list[str]"] = None,
        **browser_opts: object,
    ) -> "NcaaFetcher":
        """Build a fetcher wired to the **suggested** browser transport.

        The go-to constructor for scraping game-detail pages
        (play-by-play / individual-stats / box-score): it attaches a
        :func:`playwright_transport` (Chrome new-headless, clears bm-verify)
        so those pages -- unreachable via the curl_cffi path -- become
        fetchable. No proxy pool required (the browser runs residential-direct;
        pass a *residential* ``proxy_pool`` if you need one -- datacenter IPs
        defeat bm-verify). Returns a context manager so the browser is closed
        on exit. ``browser_opts`` pass through to :func:`playwright_transport`
        (``headless_new=``, ``challenge_wait_ms=``, ...).
        """
        base = config or get_config()
        cfg = replace(base, transport=playwright_transport(**browser_opts))  # type: ignore[arg-type]
        return cls(cfg, proxy_pool=proxy_pool)

    def __enter__(self) -> "NcaaFetcher":
        return self

    def __exit__(self, *exc: object) -> None:
        """Close a browser transport if one is attached (no-op otherwise)."""
        closer = getattr(self.config.transport, "close", None)
        if callable(closer):
            closer()

    def _default_transport(self, url: str, proxies: "dict[str, str]", headers: "dict[str, str]") -> "tuple[int, str]":
        try:
            import curl_cffi
        except ImportError as exc:  # pragma: no cover - exercised only on the live path
            raise ImportError(
                "Live stats.ncaa.org fetches require curl_cffi (the host "
                "TLS/JA3-blocks plain requests). Install with: pip install curl_cffi "
                "or pip install sportsdataverse[all]"
            ) from exc
        if self._session is None:
            session = curl_cffi.Session(impersonate=self.config.impersonate)
            session.headers.update({"Referer": NCAA_HOST_URL + "/", "Accept-Language": "en-US,en;q=0.9"})
            self._session = session
        r = self._session.get(url, proxies=proxies, timeout=self.config.timeout)  # type: ignore[attr-defined]
        return r.status_code, r.text

    def _get_with_rotation(self, url: str) -> str:
        transport = self.config.transport or self._default_transport
        headers = {"Referer": NCAA_HOST_URL + "/", "Accept-Language": "en-US,en;q=0.9"}
        # Empty pool is only reachable with a custom (browser / unblocker)
        # transport -- the default curl transport is guarded proxy-bound in
        # fetch_html. A sentinel "" => one attempt with no proxy (direct),
        # which the custom transport manages itself.
        pool = self._pool or [""]
        attempts = len(pool) + self.config.max_retries
        last_err = "no proxies in pool"
        for i in range(attempts):
            if i and self.config.rotation_backoff > 0:
                time.sleep(self.config.rotation_backoff)
            proxy = self._pick_proxy(pool)
            if proxy is None:
                raise RuntimeError(
                    f"NCAA fetch failed: every proxy in the pool is banned "
                    f"({len(self._dead)}/{len(pool)}): {url}: {last_err}"
                )
            proxies = {"http": proxy, "https": proxy} if proxy else {}
            try:
                status, text = transport(url, proxies, headers)
            except Exception as exc:  # noqa: BLE001 - rotate on any transport failure
                last_err = str(exc)
                self._rotate("transport error")
                continue
            if status == 200 and _is_challenge(text):
                # Third response class: a 200 with no ban marker that is still an
                # unsolved bm-verify shell. The browser transport already tried a
                # re-solve, so this IP is not currently clearing the sensor. NOT a
                # ban -- the IP may be perfectly healthy -- so rotate to a fresh
                # proxy (which relaunches the browser = a fresh solve) but do NOT
                # retire it. Returning this shell is what made the fetcher believe
                # it had succeeded and hammer one IP into a permanent ban.
                last_err = "bm-verify challenge not cleared"
                self._rotate("challenge not cleared")
                continue
            if status == 200 and _ban_check(text) == "clean":
                self._since_rotate += 1
                if self.config.rotate_every and self._since_rotate >= self.config.rotate_every:
                    # Proactive: retire this IP while it is still HEALTHY. Waiting
                    # for a ban means the ban already happened -- and it's permanent.
                    self._rotate(f"{self._since_rotate} fetches on this proxy")
                return text
            marker = _ban_check(text)
            last_err = f"status={status} {marker}"
            if proxy and (status == 403 or marker != "clean"):
                # Burned: never hand this IP out again this session.
                self._dead.add(proxy)
                logger.warning(
                    "proxy %s looks banned (%s) -- retiring it for this session (%d/%d dead)",
                    _redact_proxy_url(proxy),
                    last_err,
                    len(self._dead),
                    len(pool),
                )
            self._rotate("ban/failed response")
        raise RuntimeError(f"NCAA fetch failed after rotating proxies: {url}: {last_err}")

    def _rotate(self, why: str) -> None:
        """Advance to the next proxy and reset the per-proxy fetch counter."""
        self._proxy_idx += 1
        self._since_rotate = 0
        logger.debug("rotating proxy: %s", why)

    def _pick_proxy(self, pool: "list[str]") -> Optional[str]:
        """Next non-dead proxy, or ``None`` when every proxy is burned.

        The ``[""]`` sentinel (custom transport, no pool) is never marked dead.
        """
        for _ in range(len(pool)):
            proxy = pool[self._proxy_idx % len(pool)]
            if proxy not in self._dead:
                return proxy
            self._proxy_idx += 1
        return None

    def fetch_html(self, path: str, *, force: bool = False) -> str:
        """Fetch *path* (bare path or full stats.ncaa.org URL), cache-first.

        Args:
            path: e.g. ``"contests/4690813/play_by_play"`` or a full
                ``https://stats.ncaa.org/...`` URL.
            force: Bypass the cache and re-fetch, overwriting the cache file.

        Returns:
            The response HTML, decoded as UTF-8.

        Raises:
            ValueError: *path* is a full URL for a host other than
                ``stats.ncaa.org``.
            RuntimeError: No proxy is configured (the binding directive --
                there is no direct-fetch mode), or every proxy in the pool
                failed / looked banned.
        """
        cache_file = cached_path(path, cache_dir=self.config.cache_dir)
        if cache_file.exists() and not force:
            return cache_file.read_text(encoding="utf-8")
        if self.config.transport is None and not self._pool:
            raise RuntimeError(
                "NcaaFetcher has no proxy configured. The default curl_cffi transport "
                "must route through a proxy (set NcaaFetchConfig.proxy_url, or "
                "proxybonanza_key + proxybonanza_pkg / SDV_PY_NCAA_PROXY_URL / "
                "SDV_PY_PROXYBONANZA_KEY+_PKG) -- there is no direct-fetch mode. "
                "For game-detail scraping use the browser transport "
                "(NcaaFetcher.with_browser() / transport=playwright_transport()), "
                "which manages its own network and needs no proxy pool."
            )
        url = f"{NCAA_HOST_URL}/{_normalize_path(path)}"
        text = self._get_with_rotation(url)
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        cache_file.write_text(text, encoding="utf-8")
        return text

    def fetch_game_pbp(self, contest_id: object, *, legacy: bool = False, force: bool = False) -> str:
        """Fetch a game's play-by-play page."""
        path = f"game/play_by_play/{contest_id}" if legacy else f"contests/{contest_id}/play_by_play"
        return self.fetch_html(path, force=force)

    def fetch_game_box(self, contest_id: object, period: int = 1, *, legacy: bool = False, force: bool = False) -> str:
        """Fetch a game's box-score *landing* page for *period* (1-indexed).

        Note: on current (2026) stats.ncaa.org this page is the team-stats /
        game-leaders view -- the per-player box the box-score parser consumes
        split out into :meth:`fetch_game_individual_stats`. Kept for the
        team-stats surface and the legacy layout.
        """
        if legacy:
            path = f"game/box_score/{contest_id}?period_no={period}"
        else:
            path = f"contests/{contest_id}/box_score?period_no={period}"
        return self.fetch_html(path, force=force)

    def fetch_game_individual_stats(self, contest_id: object, *, legacy: bool = False, force: bool = False) -> str:
        """Fetch a game's per-player box (the ``individual_stats`` tab).

        This is the page :func:`~sportsdataverse.mbb.mbb_ncaa_boxscore_parser
        .get_box_lineup` parses on current markup (``format_version=1``): two
        ``table.dataTable.small_font#competitor_*`` per-team player tables.
        The server ignores ``?period_no`` here (returns the full-game box), so
        no period arg -- see ``dev/phase5f-live-proof.md``.

        ponytail: the modern box split out of ``box_score`` into this tab; the
        legacy (pre-2018) layout has no separate individual-stats page, so
        ``legacy=True`` falls back to the legacy ``box_score`` path.
        """
        path = f"game/box_score/{contest_id}" if legacy else f"contests/{contest_id}/individual_stats"
        return self.fetch_html(path, force=force)

    def fetch_team_roster(self, team_id: object, year_id: object, *, legacy: bool = False, force: bool = False) -> str:
        """Fetch a team's roster page for *year_id*.

        ponytail: URL shape by analogy to the confirmed team-id scheme, not
        independently live-confirmed -- see module docstring; fix in Task
        5f.2 if the real path differs.
        """
        path = f"team/{team_id}/{year_id}/roster" if legacy else f"teams/{team_id}/roster/{year_id}"
        return self.fetch_html(path, force=force)

    def fetch_team_schedule(self, team_id: object, *, legacy: bool = False, force: bool = False) -> str:
        """Fetch a team's game-by-game schedule page.

        Modern shape (``teams/{id}/game_by_game``) is confirmed by
        ``dev/phase5-ncaa-proxy-proof.md``; the legacy shape is by analogy
        (see :meth:`fetch_team_roster`'s note).
        """
        path = f"team/{team_id}" if legacy else f"teams/{team_id}/game_by_game"
        return self.fetch_html(path, force=force)


__all__ = [
    "NCAA_HOST",
    "NCAA_HOST_URL",
    "NcaaFetchConfig",
    "NcaaFetcher",
    "get_config",
    "update_config",
    "reset_config",
    "load_proxybonanza_pool",
    "playwright_transport",
    "cached_path",
    "is_cached",
]
