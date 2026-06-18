"""Cache backend for sdv-py NFL loaders.

Three modes:

- ``memory``: per-process dict, never invalidated within a process unless
  ``clear_cache()`` is called or the duration TTL expires.
- ``filesystem``: persisted to disk under ``config.cache_dir`` as parquet
  files. Cross-process / cross-session reuse.
- ``off``: no caching; every call hits the network.

The cache is wired into loaders via the ``@cached_loader`` decorator. The
decorator is intentionally minimal so it can be lifted to a cross-sport
``sportsdataverse.cache`` module later without churn — no NFL-specific
assumptions live in here.

Cache key is derived from ``(qualified_name, args, sorted_kwargs)`` and
hashed with sha256. The key deliberately excludes ``return_as_pandas`` so
a memory or disk hit serves both polars and pandas callers from a single
stored polars frame.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import time
from functools import wraps
from pathlib import Path
from typing import Any, Callable, TypeVar

import polars as pl

from sportsdataverse.nfl.config import get_config

F = TypeVar("F", bound=Callable[..., pl.DataFrame])

# Memory cache: {key: (timestamp, frame)}. The timestamp is when the frame
# was inserted, used to compare against ``config.cache_duration`` on read.
_MEMORY: dict[str, tuple[float, pl.DataFrame]] = {}


def _cache_key(func: Callable[..., Any], args: tuple, kwargs: dict) -> str:
    """Stable sha256 of ``(qualified_name, args, sorted_kwargs)``.

    JSON serialization with ``sort_keys=True`` makes the hash invariant to
    kwarg insertion order. ``default=str`` lets us serialize non-JSON
    types (e.g. ``range`` objects, ``Path``, custom enums) without
    blowing up.
    """
    payload = {
        "func": f"{func.__module__}.{func.__name__}",
        "args": args,
        "kwargs": dict(sorted(kwargs.items())),
    }
    return hashlib.sha256(json.dumps(payload, default=str, sort_keys=True).encode()).hexdigest()


def _filesystem_path(key: str) -> Path:
    """Resolve the on-disk parquet path for a given cache key."""
    cache_dir = get_config().cache_dir
    return cache_dir / f"{key}.parquet"


def _is_expired(timestamp: float) -> bool:
    """Return ``True`` if ``timestamp`` is older than the configured TTL."""
    return (time.time() - timestamp) > get_config().cache_duration


def cache_get(key: str) -> pl.DataFrame | None:
    """Return a cached frame for *key*, or ``None`` on miss or expiry.

    Honors the active :class:`~sportsdataverse.nfl.config.NflConfig` cache
    mode (``memory``, ``filesystem``, ``off``).  An expired entry is treated
    as a MISS (returns ``None``).  In ``memory`` mode the expired key is
    dropped from the in-process dict on read; in ``filesystem`` mode the
    stale parquet file is NOT auto-deleted from disk — call
    :func:`clear_cache` to reclaim it.

    Args:
        key: Cache key string (e.g. from ``_game_cache_key()``).

    Returns:
        polars.DataFrame if the key is present and unexpired; ``None``
        otherwise.
    """
    cfg = get_config()
    if cfg.cache_mode == "memory":
        entry = _MEMORY.get(key)
        if entry is not None:
            ts, frame = entry
            if not _is_expired(ts):
                return frame
            del _MEMORY[key]
    elif cfg.cache_mode == "filesystem":
        path = _filesystem_path(key)
        if path.exists() and not _is_expired(path.stat().st_mtime):
            try:
                return pl.read_parquet(path)
            except Exception:
                # Corrupt / unreadable parquet: best-effort cleanup. ``unlink``
                # can itself raise (permissions, Windows file lock), but
                # ``cache_get`` is opaque infra and must return ``None`` on any
                # cache issue rather than propagate.
                try:
                    path.unlink(missing_ok=True)
                except Exception:
                    pass
    return None


def cache_put(key: str, frame: pl.DataFrame) -> None:
    """Persist *frame* to the active cache backend under *key*.

    Honors the active :class:`~sportsdataverse.nfl.config.NflConfig` cache
    mode.  Write failures on the filesystem backend are silently swallowed —
    the data is still returned to the caller; the cache is opaque infra.

    Args:
        key: Cache key string.
        frame: polars DataFrame to store.
    """
    cfg = get_config()
    if cfg.cache_mode == "memory":
        _MEMORY[key] = (time.time(), frame)
    elif cfg.cache_mode == "filesystem":
        cache_dir = cfg.cache_dir
        cache_dir.mkdir(parents=True, exist_ok=True)
        path = _filesystem_path(key)
        try:
            frame.write_parquet(path)
        except Exception:
            pass  # Cache write failure is non-fatal; data still returned.


def cached_loader(func: F) -> F:
    """Decorator that adds caching to a ``load_nfl_*`` function.

    Honors the active ``NflConfig.cache_mode``:

    - ``memory``: dict-based per-process cache.
    - ``filesystem``: parquet-based cross-process cache under ``cache_dir``.
    - ``off``: no caching, function runs every time.

    The cache key is the hash of ``(qualified_name, args, kwargs)`` with
    ``return_as_pandas`` excluded so memory / disk hits work regardless of
    which return shape the caller asked for. The cache always stores the
    polars frame internally and converts to pandas on read when requested.

    Example:
        Decorate a custom loader::

            import polars as pl
            from sportsdataverse.nfl.cache import cached_loader

            @cached_loader
            def load_my_thing(season: int, return_as_pandas: bool = False):
                # ... fetch parquet, build a polars frame ...
                return pl.DataFrame({"season": [season]})

            df1 = load_my_thing(2024)            # network hit, populates cache
            df2 = load_my_thing(2024)            # served from cache
            df_pd = load_my_thing(2024, return_as_pandas=True)
            # `return_as_pandas` is excluded from the cache key, so the
            # polars hit is reused and converted to pandas on the way out.

        Switch caching modes at runtime::

            from sportsdataverse.nfl import clear_cache, update_config

            update_config(cache_mode="filesystem")  # parquet-on-disk reuse
            df3 = load_my_thing(2024)               # writes parquet under cache_dir
            clear_cache()                           # wipe both memory + filesystem
            update_config(cache_mode="off")         # bypass cache entirely

        See Also:
            * :func:`functools.lru_cache` -- standard-library alternative.
              ``cached_loader`` exists separately to add a TTL
              (``cache_duration``), a filesystem persistence mode, and a
              polars/pandas return-type round-trip on top of the same
              ``(qualname, args, sorted_kwargs)`` key shape. See
              https://docs.python.org/3/library/functools.html#functools.lru_cache.
    """

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> pl.DataFrame:
        cfg = get_config()
        if cfg.cache_mode == "off":
            return func(*args, **kwargs)

        # Cache key excludes return_as_pandas — see module docstring.
        key_kwargs = {k: v for k, v in kwargs.items() if k != "return_as_pandas"}
        key = _cache_key(func, args, key_kwargs)
        return_as_pandas = kwargs.get("return_as_pandas", False)

        if cfg.cache_mode == "memory":
            cached = _MEMORY.get(key)
            if cached is not None:
                ts, frame = cached
                if not _is_expired(ts):
                    return frame.to_pandas() if return_as_pandas else frame
                # Expired — drop and refetch.
                del _MEMORY[key]
            # Miss — always materialize as polars internally.
            inner_kwargs = {**kwargs, "return_as_pandas": False}
            frame = func(*args, **inner_kwargs)
            _MEMORY[key] = (time.time(), frame)
            return frame.to_pandas() if return_as_pandas else frame

        if cfg.cache_mode == "filesystem":
            path = _filesystem_path(key)
            if path.exists() and not _is_expired(path.stat().st_mtime):
                try:
                    frame = pl.read_parquet(path)
                    return frame.to_pandas() if return_as_pandas else frame
                except Exception:
                    # Corrupt cache file — fall through to refetch and
                    # overwrite. We don't propagate the error because the
                    # cache is opaque infra and the caller asked for data.
                    path.unlink(missing_ok=True)
            # Miss / expired / corrupt — refetch.
            inner_kwargs = {**kwargs, "return_as_pandas": False}
            frame = func(*args, **inner_kwargs)
            cache_dir = cfg.cache_dir
            cache_dir.mkdir(parents=True, exist_ok=True)
            try:
                frame.write_parquet(path)
            except Exception:
                # If writing the cache fails (e.g. read-only fs), still
                # return the data; the next call will simply refetch.
                pass
            return frame.to_pandas() if return_as_pandas else frame

        # Unknown mode — defensively skip cache.
        return func(*args, **kwargs)

    return wrapper  # type: ignore[return-value]


def clear_cache() -> None:
    """Clear both memory and filesystem caches.

    Memory: empties the in-process dict.
    Filesystem: removes all entries under ``config.cache_dir``. The
    directory itself is preserved so subsequent writes succeed without
    needing ``mkdir``.

    The ``models/`` subdirectory is **deliberately preserved** — it holds
    download-on-demand model artifacts (e.g. the ~34 MB ``xyac_model.ubj``)
    that are expensive to re-fetch. Clearing the *data* cache should not force
    a model re-download; delete ``<cache_dir>/models/`` by hand to drop those.

    Example:
        Force a fresh fetch after upstream changes::

            from sportsdataverse.nfl import clear_cache, load_nfl_pbp
            clear_cache()
            pbp = load_nfl_pbp(seasons=[2024])

        Pair with a cache-mode switch::

            from sportsdataverse.nfl import clear_cache, update_config
            update_config(cache_mode="filesystem")
            # ... lots of cached calls accumulate parquet files on disk ...
            clear_cache()  # wipe disk + memory together

        See Also:
            * :func:`sportsdataverse.nfl.update_config` -- toggle cache mode/duration.
            * :func:`sportsdataverse.nfl.cache.cached_loader` -- decorator that reads/writes the cache.
    """
    _MEMORY.clear()
    cache_dir = get_config().cache_dir
    if cache_dir.exists():
        for child in cache_dir.iterdir():
            # Preserve the download-on-demand model cache; a data-cache clear
            # must not force an expensive model re-download.
            if child.name == "models":
                continue
            if child.is_file():
                child.unlink()
            elif child.is_dir():
                shutil.rmtree(child)
