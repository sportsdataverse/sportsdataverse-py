---
title: OJHL
sidebar_label: OJHL
description: "sdv-py OJHL: endpoint references, dataset loaders and parsers for OJHL in the SportsDataverse Python package."
---
# OJHL (`sportsdataverse.ojhl`)

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

- [`most_recent_ojhl_season`](reference/additional#most_recent_ojhl_season)
- [`ojhl_season_id`](reference/additional#ojhl_season_id)


## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)
