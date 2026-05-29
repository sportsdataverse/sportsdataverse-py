"""sportsdataverse.cache — tiered TTL response cache.

The package-wide HTTP layer (:func:`sportsdataverse.dl_utils.download`)
optionally checks this cache before hitting the network. Cached
responses are keyed by ``(url, params)`` and tagged with the wall-clock
time they were stored; reads return the cached body when it's still
within its TTL.

Three modes
~~~~~~~~~~~

* ``"off"`` (default) — no caching, every call hits the network.
  Behaviour is identical to pre-0.0.51.
* ``"memory"`` — in-process dict. Lost on interpreter restart.
* ``"filesystem"`` — JSON files under
  ``~/.cache/sportsdataverse/`` (or ``$SDV_PY_CACHE_DIR`` if set).
  Survives across notebook sessions.

Switch modes via :func:`set_cache_mode`::

    import sportsdataverse as sdv
    sdv.set_cache_mode("filesystem")            # persist to disk
    sdv.set_cache_mode("memory")                # in-process only
    sdv.set_cache_mode("off")                   # disable entirely

Tiered TTL
~~~~~~~~~~

Different endpoints have wildly different staleness tolerances. The
:func:`pick_ttl` helper inspects the URL and assigns one of six tiers:

============  =========================  =====================================
Tier          TTL                        Endpoints
============  =========================  =====================================
IMMUTABLE     30 days                    PBP / boxscore / summary for past
                                         games, NHL Records, glossaries
REFERENCE     7 days                     venues, franchises, league meta,
                                         divisions, seasons, draft picks
SLOW          24 hours                   team rosters, athlete profiles,
                                         season-summary standings
MODERATE      1 hour (default)           leaders, season-to-date stats,
                                         current standings
FAST          5 minutes                  news, injuries
LIVE          0 (no cache)               ``/scoreboard/now``,
                                         in-progress PBP, live boxscore
============  =========================  =====================================

Override per call via ``cache_ttl=`` kwarg on
:func:`sportsdataverse.dl_utils.download`, or globally via
:func:`set_default_ttl`.

Invalidation
~~~~~~~~~~~~

::

    sdv.clear_cache()                       # everything
    sdv.clear_cache(pattern="*roster*")     # filename-glob subset
    sdv.clear_cache(url="https://...")      # one exact URL+params combo
"""

from __future__ import annotations

import fnmatch
import hashlib
import json
import os
import re
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Union

# ---------------------------------------------------------------------------
# TTL tiers
# ---------------------------------------------------------------------------

IMMUTABLE = timedelta(days=30)
REFERENCE = timedelta(days=7)
SLOW = timedelta(hours=24)
MODERATE = timedelta(hours=1)
FAST = timedelta(minutes=5)
LIVE = timedelta(seconds=0)  # 0 = don't cache

#: Default TTL when the URL doesn't match any tiered pattern.
DEFAULT_TTL = MODERATE


# Compiled patterns for URL → tier classification. Patterns are tried
# in order; first match wins.
_TIER_RULES = [
    # LIVE — never cache anything that's actively updating
    (re.compile(r"/scoreboard/now\b"), LIVE),
    (re.compile(r"/score/now\b"), LIVE),
    (re.compile(r"/standings/now\b"), LIVE),
    (re.compile(r"/playerstatusupdate\b"), LIVE),
    # IMMUTABLE — completed games never change
    (re.compile(r"/gamecenter/\d+/(play-by-play|boxscore|landing|right-rail)"), IMMUTABLE),
    (re.compile(r"/scoreboard.*dates=(\d{8})"), None),  # special-cased below
    (re.compile(r"/glossary\b"), IMMUTABLE),
    (re.compile(r"/award\b"), IMMUTABLE),
    (re.compile(r"records\.nhl\.com/site/api/(player|coach|draft|attendance|franchise)"), IMMUTABLE),
    # FAST — frequent churn (must come before REFERENCE's broad /sports/ match)
    (re.compile(r"/news\b"), FAST),
    (re.compile(r"/injuries\b"), FAST),
    # SLOW — change with trades / call-ups / weekly games
    (re.compile(r"/teams/\d+/(schedule|roster)\b"), SLOW),
    (re.compile(r"/roster\b"), SLOW),
    (re.compile(r"/(player|athlete)/\d+/landing"), SLOW),
    # REFERENCE — change a few times a year
    (re.compile(r"/draft/picks/\d{4}"), REFERENCE),
    (re.compile(r"/(venues?|franchises?|divisions?|seasons?)\b"), REFERENCE),
    # Catchalls: stats / leaders / standings → MODERATE (default)
]


def pick_ttl(url: str, today: Optional[datetime] = None) -> timedelta:
    """Inspect a URL and return the appropriate cache TTL.

    Args:
        url: The full request URL.
        today: For testability — override "now" so date-comparison logic
            ( e.g. is this scoreboard for a past date? ) is deterministic.

    Returns:
        ``timedelta(0)`` means "do not cache" (LIVE tier). Otherwise the
        cached body is considered fresh until ``saved_at + ttl`` is
        reached.

    Examples::

        >>> pick_ttl("https://api-web.nhle.com/v1/scoreboard/now")
        timedelta(seconds=0)                              # LIVE

        >>> pick_ttl("https://api-web.nhle.com/v1/gamecenter/2023030417/play-by-play")
        timedelta(days=30)                                # IMMUTABLE

        >>> pick_ttl("https://site.api.espn.com/.../news?limit=5")
        timedelta(seconds=300)                            # FAST
    """
    today = today or datetime.now(timezone.utc)
    today_yyyymmdd = today.strftime("%Y%m%d")

    for pattern, tier in _TIER_RULES:
        m = pattern.search(url)
        if not m:
            continue
        if tier is None:
            # Special-cased: ESPN scoreboard with explicit dates=YYYYMMDD
            # — past dates are immutable, today/future are LIVE.
            date_match = m.group(1)
            try:
                if int(date_match) < int(today_yyyymmdd):
                    return IMMUTABLE
            except (ValueError, IndexError):
                pass
            return LIVE
        return tier
    return DEFAULT_TTL


# ---------------------------------------------------------------------------
# Mode + key helpers
# ---------------------------------------------------------------------------

_VALID_MODES = ("off", "memory", "filesystem")
_MODE: str = "off"
_MEMORY_CACHE: Dict[str, Dict[str, Any]] = {}
_DEFAULT_TTL_OVERRIDE: Optional[timedelta] = None


def set_cache_mode(mode: str) -> None:
    """Switch the global cache mode.

    Args:
        mode: One of ``"off"``, ``"memory"``, ``"filesystem"``.

    Raises:
        ValueError: If ``mode`` is not one of the three valid options.
    """
    global _MODE
    if mode not in _VALID_MODES:
        raise ValueError(
            f"Invalid cache mode {mode!r}. Choose one of {list(_VALID_MODES)}.",
        )
    _MODE = mode


def get_cache_mode() -> str:
    """Return the current cache mode."""
    return _MODE


def set_default_ttl(ttl: Optional[Union[timedelta, int]]) -> None:
    """Override the default TTL for endpoints not matched by the tier rules.

    Args:
        ttl: A ``timedelta``, an integer (interpreted as seconds), or
            ``None`` to reset to the built-in :data:`DEFAULT_TTL`
            (``MODERATE`` = 1 hour).
    """
    global _DEFAULT_TTL_OVERRIDE
    if ttl is None:
        _DEFAULT_TTL_OVERRIDE = None
    elif isinstance(ttl, timedelta):
        _DEFAULT_TTL_OVERRIDE = ttl
    elif isinstance(ttl, (int, float)):
        _DEFAULT_TTL_OVERRIDE = timedelta(seconds=ttl)
    else:
        raise TypeError(f"ttl must be timedelta or seconds, got {type(ttl)}")


def _resolve_ttl(url: str, override: Optional[timedelta] = None) -> timedelta:
    """Apply override → default-override → tier-rule fallback chain."""
    if override is not None:
        return override
    ttl = pick_ttl(url)
    # Only apply default-override when the URL fell through to DEFAULT_TTL
    if ttl == DEFAULT_TTL and _DEFAULT_TTL_OVERRIDE is not None:
        return _DEFAULT_TTL_OVERRIDE
    return ttl


def _cache_dir() -> Path:
    """Resolve the filesystem cache directory.

    Honours ``$SDV_PY_CACHE_DIR`` for users who want to put the cache
    on a fast SSD / tmpfs / project-local directory. Defaults to
    ``~/.cache/sportsdataverse/``.
    """
    override = os.environ.get("SDV_PY_CACHE_DIR")
    if override:
        return Path(override).expanduser()
    return Path.home() / ".cache" / "sportsdataverse"


def _cache_key(url: str, params: Optional[Dict[str, Any]] = None) -> str:
    """Stable filename-safe hash of (url, sorted-params)."""
    payload = url
    if params:
        # Sort to make the key order-insensitive
        items = sorted(params.items())
        payload += "?" + "&".join(f"{k}={v}" for k, v in items if v is not None)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]


# ---------------------------------------------------------------------------
# Cached-response shim — quacks like requests.Response
# ---------------------------------------------------------------------------


class CachedResponse:
    """Minimal ``requests.Response``-shaped object served from the cache.

    Implements the four attributes/methods every sportsdataverse wrapper
    touches: ``.json()``, ``.status_code``, ``.text``, ``.url``. The
    cache stores parsed-JSON bodies (not raw bytes), so ``.json()`` is a
    no-op return and ``.text`` is a re-serialization.
    """

    def __init__(
        self,
        body: Any,
        url: str = "",
        status_code: int = 200,
        from_cache: bool = True,
    ) -> None:
        self._body = body
        self.url = url
        self.status_code = status_code
        self.from_cache = from_cache
        self.headers: Dict[str, str] = {"X-From-Cache": "1"}

    def json(self) -> Any:
        return self._body

    @property
    def text(self) -> str:
        return json.dumps(self._body)

    @property
    def content(self) -> bytes:
        return self.text.encode("utf-8")

    def __repr__(self) -> str:
        return f"<CachedResponse [{self.status_code}] from-cache={self.from_cache}>"


# ---------------------------------------------------------------------------
# Read / write
# ---------------------------------------------------------------------------


def cache_get(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    *,
    ttl: Optional[timedelta] = None,
) -> Optional[Dict[str, Any]]:
    """Fetch a cached body. Returns ``None`` on miss / expiry / mode=off."""
    if _MODE == "off":
        return None
    effective_ttl = _resolve_ttl(url, ttl)
    if effective_ttl.total_seconds() <= 0:
        return None  # LIVE — never serve from cache

    key = _cache_key(url, params)
    if _MODE == "memory":
        entry = _MEMORY_CACHE.get(key)
    else:  # filesystem
        path = _cache_dir() / f"{key}.json"
        if not path.exists():
            return None
        try:
            entry = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return None

    if not entry:
        return None
    saved_str = entry.get("saved_at")
    if not saved_str:
        return None
    try:
        saved = datetime.fromisoformat(saved_str)
    except ValueError:
        return None
    age = datetime.now(timezone.utc) - saved
    if age > effective_ttl:
        return None
    return entry.get("body")


def cache_set(
    url: str,
    params: Optional[Dict[str, Any]],
    body: Any,
    *,
    ttl: Optional[timedelta] = None,
) -> None:
    """Persist a body to the cache. No-op when mode=off or TTL=LIVE."""
    if _MODE == "off":
        return
    effective_ttl = _resolve_ttl(url, ttl)
    if effective_ttl.total_seconds() <= 0:
        return

    key = _cache_key(url, params)
    entry = {
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "url": url,
        "body": body,
    }
    if _MODE == "memory":
        _MEMORY_CACHE[key] = entry
    else:  # filesystem
        cache_dir = _cache_dir()
        try:
            cache_dir.mkdir(parents=True, exist_ok=True)
            path = cache_dir / f"{key}.json"
            path.write_text(json.dumps(entry, ensure_ascii=False), encoding="utf-8")
        except OSError:
            # Disk full / permission denied — fail silently so a cache
            # write never breaks the calling wrapper.
            pass


def clear_cache(
    *,
    url: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
    pattern: Optional[str] = None,
) -> int:
    """Invalidate cached entries.

    Args:
        url: If given, clear only the entry for this exact (url, params)
            combo.
        params: Companion to ``url=``.
        pattern: Filename glob (e.g. ``"*"``, ``"a*"``, ``"abc??"``) tested
            against the cache key. Use ``"*"`` to nuke everything; this
            is what the bare ``clear_cache()`` does internally.

    Returns:
        Number of entries removed.
    """
    if url is not None:
        key = _cache_key(url, params)
        return _drop_one(key)

    glob = pattern or "*"
    return _drop_matching(glob)


def _drop_one(key: str) -> int:
    if _MODE == "memory":
        if key in _MEMORY_CACHE:
            del _MEMORY_CACHE[key]
            return 1
        return 0
    if _MODE == "filesystem":
        path = _cache_dir() / f"{key}.json"
        if path.exists():
            path.unlink()
            return 1
    return 0


def _drop_matching(glob: str) -> int:
    if _MODE == "memory":
        keys = [k for k in _MEMORY_CACHE if fnmatch.fnmatch(k, glob)]
        for k in keys:
            del _MEMORY_CACHE[k]
        return len(keys)
    if _MODE == "filesystem":
        cache_dir = _cache_dir()
        if not cache_dir.exists():
            return 0
        if glob == "*":
            # Nuke fast path — remove the whole directory tree.
            count = sum(1 for _ in cache_dir.glob("*.json"))
            shutil.rmtree(cache_dir, ignore_errors=True)
            return count
        count = 0
        for path in cache_dir.glob("*.json"):
            if fnmatch.fnmatch(path.stem, glob):
                path.unlink()
                count += 1
        return count
    return 0


def cache_stats() -> Dict[str, Any]:
    """Return a snapshot of the cache for debugging / inspection.

    Returns a dict with ``mode``, ``entries``, and ``disk_bytes`` (only
    populated when mode=filesystem). Cheap — doesn't read the cached
    bodies, just counts + sizes.
    """
    out: Dict[str, Any] = {"mode": _MODE, "entries": 0, "disk_bytes": 0}
    if _MODE == "memory":
        out["entries"] = len(_MEMORY_CACHE)
    elif _MODE == "filesystem":
        cache_dir = _cache_dir()
        if cache_dir.exists():
            files = list(cache_dir.glob("*.json"))
            out["entries"] = len(files)
            out["disk_bytes"] = sum(f.stat().st_size for f in files)
            out["cache_dir"] = str(cache_dir)
    return out


__all__ = [
    # Mode control
    "set_cache_mode",
    "get_cache_mode",
    "set_default_ttl",
    # TTL tiers
    "IMMUTABLE",
    "REFERENCE",
    "SLOW",
    "MODERATE",
    "FAST",
    "LIVE",
    "DEFAULT_TTL",
    "pick_ttl",
    # Read/write
    "cache_get",
    "cache_set",
    "clear_cache",
    "cache_stats",
    "CachedResponse",
]
