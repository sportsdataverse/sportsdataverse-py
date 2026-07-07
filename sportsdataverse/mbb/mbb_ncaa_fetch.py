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
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional
from urllib.parse import urlsplit

NCAA_HOST = "stats.ncaa.org"
NCAA_HOST_URL = f"https://{NCAA_HOST}"

# Graduated verbatim from dev/ncaa_proxy.py's ``ban_check`` -- substrings
# observed (or generically expected) in stats.ncaa.org's ban/rate-limit/WAF
# responses. A 200 status with one of these markers present is treated as a
# failed fetch so the caller rotates proxies rather than caching a ban page.
_BAN_MARKERS = (
    "access denied",
    "forbidden",
    "blocked",
    "captcha",
    "cf-browser-verification",
    "attention required",
    "rate limit",
)

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


class NcaaFetcher:
    """Cache-first stats.ncaa.org fetcher, proxy-bound per the binding directive.

    Example:
        Quick start (real transport, single explicit proxy)::

            from sportsdataverse.mbb.mbb_ncaa_fetch import NcaaFetcher, update_config
            update_config(proxy_url="http://user:pass@1.2.3.4:8080")
            fetcher = NcaaFetcher()
            html = fetcher.fetch_game_pbp("4690813")   # cached after this call
            html2 = fetcher.fetch_game_pbp("4690813")  # cache hit, no request

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
        self._session: object = None  # lazy curl_cffi.Session (real transport only)

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
        attempts = max(len(self._pool), 1) + self.config.max_retries
        last_err = "no proxies in pool"
        for _ in range(attempts):
            proxy = self._pool[self._proxy_idx % len(self._pool)]
            proxies = {"http": proxy, "https": proxy}
            try:
                status, text = transport(url, proxies, headers)
            except Exception as exc:  # noqa: BLE001 - rotate on any transport failure
                last_err = str(exc)
                self._proxy_idx += 1
                continue
            if status == 200 and _ban_check(text) == "clean":
                return text
            last_err = f"status={status} {_ban_check(text)}"
            self._proxy_idx += 1
        raise RuntimeError(f"NCAA fetch failed after rotating proxies: {url}: {last_err}")

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
        if not self._pool:
            raise RuntimeError(
                "NcaaFetcher has no proxy configured. stats.ncaa.org requests must "
                "route through a proxy (set NcaaFetchConfig.proxy_url, or "
                "proxybonanza_key + proxybonanza_pkg / SDV_PY_NCAA_PROXY_URL / "
                "SDV_PY_PROXYBONANZA_KEY+_PKG) -- there is no direct-fetch mode."
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
        """Fetch a game's box-score page for *period* (1-indexed)."""
        if legacy:
            path = f"game/box_score/{contest_id}?period_no={period}"
        else:
            path = f"contests/{contest_id}/box_score?period_no={period}"
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
    "cached_path",
    "is_cached",
]
