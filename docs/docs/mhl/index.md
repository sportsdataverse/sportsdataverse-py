---
title: MHL
sidebar_label: MHL
description: "sdv-py MHL: endpoint references, dataset loaders and parsers for MHL in the SportsDataverse Python package."
---
# MHL (`sportsdataverse.mhl`)

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

- [`mhl_season_id`](reference/additional#mhl_season_id)
- [`most_recent_mhl_season`](reference/additional#most_recent_mhl_season)


## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)
