---
title: Israeli Premier League
sidebar_label: Israeli Premier League
description: "sdv-py Israeli Premier League: endpoint references, dataset loaders and parsers for Israeli Premier League in the SportsDataverse Python package."
---
# Israeli Premier League (`sportsdataverse.basketball.ibba`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [IBBA (Israeli Premier League) and Segev Stats](#ibba-israeli-premier-league-and-segev-stats) | `basket.co.il`, `stats.segevstats.com` | 22 | none |
| [Additional functions](reference/additional) | hand-written wrappers & helpers | 1 | — |

## IBBA (Israeli Premier League) and Segev Stats {#ibba-israeli-premier-league-and-segev-stats}

| Reference | Functions |
|---|---:|
| [IBBA ASMX web service (basket.co.il, Israel)](reference/ibba) | 16 |
| [Segev Stats live-stats API (stats.segevstats.com, Israel play-by-play)](reference/ibba_segev) | 6 |
## Tools and helpers

### IDs and crosswalks {#ids-and-crosswalks}

- [`ibba_segev_game_crosswalk`](reference/additional#ibba_segev_game_crosswalk)


## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)
