---
title: SJHL
sidebar_label: SJHL
description: "sdv-py SJHL: endpoint references, dataset loaders and parsers for SJHL in the SportsDataverse Python package."
---
# SJHL (`sportsdataverse.sjhl`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [HockeyTech / LeagueStat](#hockeytech-leaguestat) | `lscluster.hockeytech.com` | 12 | per-league public key (SDV_<LEAGUE>_API_KEY) |
| [Additional functions](reference/additional) | hand-written wrappers & helpers | 14 | — |

## HockeyTech / LeagueStat {#hockeytech-leaguestat}

| Reference | Functions |
|---|---:|
| [Hand-written wrappers](reference/additional) | 12 |
## Tools and helpers

### Dates and seasons {#dates-and-seasons}

- [`most_recent_sjhl_season`](reference/additional#most_recent_sjhl_season)
- [`sjhl_season_id`](reference/additional#sjhl_season_id)


## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)
