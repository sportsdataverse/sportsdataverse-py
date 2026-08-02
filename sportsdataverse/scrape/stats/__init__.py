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

Phase 2 added the league-parameterized capture layer:

    * :mod:`~sportsdataverse.scrape.stats.league_config` — frozen per-league
      identity (:data:`~sportsdataverse.scrape.stats.league_config.NBA` /
      :data:`~sportsdataverse.scrape.stats.league_config.WNBA`).
    * :mod:`~sportsdataverse.scrape.stats.endpoints` — signature-derived capture
      registry; season-string spelling (NBA span ``"2023-24"`` vs WNBA bare
      year) keys off ``league_id``.
    * :mod:`~sportsdataverse.scrape.stats.season_capture` — atomic, resumable
      season-level captures (validity-guarded writes).
    * :mod:`~sportsdataverse.scrape.stats.periods` — league- and era-aware
      per-period window math (NBA 12-min quarters; WNBA halves→quarters 2006).
    * :mod:`~sportsdataverse.scrape.stats.refill` — repair pass for the
      empty-``{}`` payload incident class, driven by a ``LeagueConfig``.

The owning ``-raw`` repos keep only thin league-binding shims + drivers.
"""
