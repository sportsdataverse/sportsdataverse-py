"""Shared scrape-engine primitives for the SDV ``-raw`` producer repos.

This subpackage is the library home for scraping machinery that was previously
copy-pasted between producer repos (the 2026-08-02 pipeline audit's twin-repo
finding). It is **operational tooling for the data-producer pipelines**, not
part of the tidy-data API surface — nothing here is re-exported at the
top-level ``sportsdataverse`` namespace.

Families:
    * ``sportsdataverse.scrape.stats`` — stats.nba.com / stats.wnba.com sweep
      engine (proxy pool, sticky-session transport, sweep observability).
"""
