"""sdv-py NFL configuration.

Mirrors nflreadpy's config surface so users coming from nflreadpy have a
near-drop-in replacement. Env-var prefix is ``SDV_PY_NFL_*`` (NFL-scoped
for now; future lift to ``SDV_PY_*`` cross-sport is intentional and will
be additive — old vars will keep working).

Environment variables (all optional):

- ``SDV_PY_NFL_CACHE``        — cache mode (``memory``, ``filesystem``, ``off``)
- ``SDV_PY_NFL_CACHE_DIR``    — directory for filesystem cache
- ``SDV_PY_NFL_CACHE_DURATION`` — cache duration in seconds (int)
- ``SDV_PY_NFL_VERBOSE``      — verbose output (1/0, true/false, yes/no)
- ``SDV_PY_NFL_TIMEOUT``      — HTTP timeout in seconds (int)
- ``SDV_PY_NFL_USER_AGENT``   — custom user-agent string

Programmatic example::

    from sportsdataverse.nfl import update_config, get_config
    update_config(cache_mode="filesystem", cache_duration=3600)
    config = get_config()
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

CacheMode = Literal["memory", "filesystem", "off"]


@dataclass
class NflConfig:
    """Runtime configuration for sdv-py NFL loaders.

    Fields mirror nflreadpy's ``NflreadpyConfig`` so users can swap engines
    without changing call sites. The defaults are conservative: in-memory
    caching with a 24-hour TTL, verbose progress bars on, 30-second
    HTTP timeout.

    Example:
        Inspect defaults via ``get_config()``::

            from sportsdataverse.nfl import get_config
            cfg = get_config()  # NflConfig instance
            cfg.cache_mode      # "memory"
            cfg.cache_duration  # 86400 (24h)
            cfg.timeout         # 30 (seconds)

        Construct a fresh instance directly (rarely needed -- prefer
        ``update_config``)::

            from sportsdataverse.nfl import NflConfig
            cfg = NflConfig(cache_mode="off", timeout=10)
    """

    cache_mode: CacheMode = "memory"
    cache_dir: Path = field(default_factory=lambda: Path.home() / ".cache" / "sportsdataverse" / "nfl")
    cache_duration: int = 86400  # seconds (24h)
    verbose: bool = True
    timeout: int = 30
    user_agent: str = "sportsdataverse-py-nfl"


def _from_env() -> NflConfig:
    """Build an ``NflConfig`` from ``SDV_PY_NFL_*`` environment variables.

    Precedence at runtime is: explicit ``update_config()`` > env var > default.
    Invalid values for typed fields (``cache_duration``, ``timeout``) are
    silently ignored so a typo in a shell rc file doesn't take down imports;
    the user can still observe the effective config via ``get_config()``.
    """
    cfg = NflConfig()

    if (v := os.environ.get("SDV_PY_NFL_CACHE")) is not None:
        v_lower = v.lower()
        if v_lower in ("memory", "filesystem", "off"):
            cfg.cache_mode = v_lower  # type: ignore[assignment]
    if (v := os.environ.get("SDV_PY_NFL_CACHE_DIR")) is not None:
        cfg.cache_dir = Path(v).expanduser()
    if (v := os.environ.get("SDV_PY_NFL_CACHE_DURATION")) is not None:
        try:
            cfg.cache_duration = int(v)
        except ValueError:
            pass
    if (v := os.environ.get("SDV_PY_NFL_VERBOSE")) is not None:
        cfg.verbose = v.lower() in ("1", "true", "yes")
    if (v := os.environ.get("SDV_PY_NFL_TIMEOUT")) is not None:
        try:
            cfg.timeout = int(v)
        except ValueError:
            pass
    if (v := os.environ.get("SDV_PY_NFL_USER_AGENT")) is not None:
        cfg.user_agent = v

    return cfg


# Module-level singleton — initialized from env at import time.
_config: NflConfig = _from_env()


def get_config() -> NflConfig:
    """Return the live ``NflConfig`` singleton.

    The same object is returned on every call; mutate via ``update_config``
    rather than reassigning fields directly so future hooks (e.g. logging
    on config change) have a single choke point.

    Example:
        Inspect the active config::

            from sportsdataverse.nfl import get_config
            cfg = get_config()
            print(cfg.cache_mode, cfg.cache_duration, cfg.cache_dir)

        Pair with ``update_config`` to verify a change took effect::

            from sportsdataverse.nfl import update_config, get_config
            update_config(cache_mode="off")
            assert get_config().cache_mode == "off"
    """
    return _config


def update_config(**kwargs: object) -> NflConfig:
    """Update the active config in place.

    Pass keyword arguments matching ``NflConfig`` fields::

        update_config(cache_mode="filesystem", cache_duration=3600)

    String values for ``cache_dir`` are coerced to ``pathlib.Path`` and
    ``~`` is expanded for convenience.

    Args:
        **kwargs: Field name → new value pairs.

    Returns:
        The (mutated) global config object, for chaining or inspection.

    Raises:
        ValueError: If a passed key does not correspond to an
            ``NflConfig`` field.

    Example:
        Switch to filesystem caching with a 1-hour TTL::

            from sportsdataverse.nfl import update_config
            update_config(cache_mode="filesystem", cache_duration=3600)

        Disable caching for development::

            update_config(cache_mode="off")

        Point cache at a custom directory::

            update_config(cache_dir="~/sdv-cache")

        See Also:
            * :func:`sportsdataverse.nfl.reset_config` -- undo a chain of updates.
            * :func:`sportsdataverse.nfl.clear_cache` -- wipe cached entries.
    """
    global _config
    for key, value in kwargs.items():
        if not hasattr(_config, key):
            raise ValueError(f"Unknown config key: {key!r}")
        if key == "cache_dir" and isinstance(value, str):
            value = Path(value).expanduser()
        setattr(_config, key, value)
    return _config


def reset_config() -> NflConfig:
    """Reset the active config to its env-var-derived defaults.

    Convenience for tests / interactive sessions that want to undo a chain
    of ``update_config()`` calls without restarting the interpreter.

    Example:
        Restore defaults after a session of tweaks::

            from sportsdataverse.nfl import update_config, reset_config
            update_config(cache_mode="off", timeout=5)
            # ... do work ...
            reset_config()  # back to env-derived defaults
    """
    global _config
    _config = _from_env()
    return _config
