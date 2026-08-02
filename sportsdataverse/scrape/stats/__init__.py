"""stats.nba.com / stats.wnba.com sweep engine (shared by the stats-raw twins).

Lifted verbatim (Phase 1 of the shared-engine extraction) from
``hoopR-nba-stats-raw`` / ``wehoop-wnba-stats-raw``, where these modules had
drifted to byte-identical-or-nearly (0–6 diff lines each):

    * :mod:`~sportsdataverse.scrape.stats.proxy` — round-robin ProxyBonanza
      pool with quarantine + outcome classification.
    * :mod:`~sportsdataverse.scrape.stats.session_transport` — thread-local
      sticky-session ``curl_cffi`` transport (Chrome impersonation; the hosts
      TLS/JA3-block plain ``requests`` with a silent hang).
    * :mod:`~sportsdataverse.scrape.stats.observability` — sweep bookkeeping
      (endpoint outcome ledger, degradation windows, progress heartbeat).

Import the modules directly (``from sportsdataverse.scrape.stats import
observability``); there are deliberately no wildcard re-exports here —
``proxy`` and ``observability`` each define their own ``classify`` for
different layers, and flattening them would shadow one.

League-specific behavior (endpoint sets, season formats, store roots) stays in
the consuming ``-raw`` repos; Phase 2 moves it behind a ``LeagueConfig``.
"""
