---
title: KIJHL
sidebar_label: KIJHL
description: "sdv-py KIJHL: endpoint references, dataset loaders and parsers for KIJHL in the SportsDataverse Python package."
---
# KIJHL (`sportsdataverse.kijhl`)

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

- [`kijhl_season_id`](reference/additional#kijhl_season_id)
- [`most_recent_kijhl_season`](reference/additional#most_recent_kijhl_season)


## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)
