"""Women's college basketball cache-first, proxy-bound fetch layer for stats.ncaa.org.

Thin shim over :mod:`sportsdataverse.mbb.mbb_ncaa_fetch`. **This is ORIGINAL
sdv-py code -- there is NO upstream/third-party attribution for this module**
(unlike the parser-layer shims elsewhere in ``sportsdataverse.wbb``, the
fetch layer has no ``cbb-explorer`` / ``cbb-on-off-analyzer`` ancestor; see
that module's docstring for the full provenance note). stats.ncaa.org is
league-agnostic transport-wise -- the women's index pages share the exact
same URL shapes, HTML structure, and Akamai bot-management behavior as the
men's pages -- so the men's fetch layer serves the women's side unchanged.
This module re-exports the mbb core **by reference** (not a copy) so
``sportsdataverse.wbb`` callers get the identical implementation.

**Shared config singleton.** :func:`get_config` / :func:`update_config` /
:func:`reset_config` operate on the SAME module-level ``NcaaFetchConfig``
instance as :mod:`sportsdataverse.mbb.mbb_ncaa_fetch` -- there is only ever
one stats.ncaa.org fetch config process-wide, which is correct because the
host, proxy pool, and cache directory are league-agnostic. Calling
``sportsdataverse.wbb.wbb_ncaa_fetch.update_config(...)`` mutates the exact
config the mbb side reads, and vice versa; do not assume a separate wbb
config exists.

Example:
    Quick start::

        from sportsdataverse.wbb.wbb_ncaa_fetch import NcaaFetcher, update_config

        update_config(proxy_url="http://user:pass@1.2.3.4:8080")
        fetcher = NcaaFetcher()
        html = fetcher.fetch_game_pbp("4690813")

See Also:
    * `wehoop`_ -- R-side women's college basketball data + on/off analysis.
    * `hoopR`_ -- men's college basketball counterpart.

.. _wehoop: https://wehoop.sportsdataverse.org
.. _hoopR: https://hoopR.sportsdataverse.org
"""

from __future__ import annotations

from sportsdataverse.mbb.mbb_ncaa_fetch import (
    NCAA_HOST,
    NCAA_HOST_URL,
    NcaaFetchConfig,
    NcaaFetcher,
    cached_path,
    get_config,
    is_cached,
    load_proxybonanza_pool,
    reset_config,
    update_config,
)

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
