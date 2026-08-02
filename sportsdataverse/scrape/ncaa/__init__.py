"""stats.ncaa.org college-basketball sweep engine (shared by the NCAA hoops twins).

Lifted from ``ncaa-mbb-hoops-raw`` / ``ncaa-wbb-hoops-raw``, whose ``python/``
trees had diverged by only ~90 lines across ~4,000 LOC of production code — the
two repos were the same pipeline twice, ported by hand.

Stages, in run order:

    * :mod:`~sportsdataverse.scrape.ncaa.discover` — team/season crosswalk →
      contest ids → the season ``schedule_master``.
    * :mod:`~sportsdataverse.scrape.ncaa.capture` — the 3-page bundle per
      contest, sharded, disk-is-checkpoint, ban-aware.
    * :mod:`~sportsdataverse.scrape.ncaa.parse` — bundle → pbp / shots /
      lineups frames (period model + three-point arc selected by league).
    * :mod:`~sportsdataverse.scrape.ncaa.rosters` — per-team roster pages.
    * :mod:`~sportsdataverse.scrape.ncaa.datasets` — season aggregates + the
      tree writers.
    * :mod:`~sportsdataverse.scrape.ncaa.identity` /
      :mod:`~sportsdataverse.scrape.ncaa.espn_game_xwalk` — id enrichment and
      the ESPN crosswalk.
    * :mod:`~sportsdataverse.scrape.ncaa.bundle` — bundle read/write +
      ``is_captured`` (the real resume check).
    * :mod:`~sportsdataverse.scrape.ncaa.canary` — pre-flight transport probe.

``league`` is a **required keyword** on every public entry point; see
:mod:`~sportsdataverse.scrape.ncaa.league_config` for why defaulting it is
unsafe. Each ``-raw`` repo keeps a thin shim that binds its league, plus its
launchers and its test suite (the suites are the parity harness for this
engine and deliberately stay repo-side).
"""
