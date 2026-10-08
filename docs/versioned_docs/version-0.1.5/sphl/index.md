---
title: SPHL
sidebar_label: SPHL
description: "sdv-py SPHL: endpoint references, dataset loaders and parsers for SPHL in the SportsDataverse Python package."
---
# SPHL (`sportsdataverse.sphl`)

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

- [`most_recent_sphl_season`](reference/additional#most_recent_sphl_season)
- [`sphl_season_id`](reference/additional#sphl_season_id)


## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)
