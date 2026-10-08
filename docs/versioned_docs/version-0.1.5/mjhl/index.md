---
title: MJHL
sidebar_label: MJHL
description: "sdv-py MJHL: endpoint references, dataset loaders and parsers for MJHL in the SportsDataverse Python package."
---
# MJHL (`sportsdataverse.mjhl`)

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

- [`mjhl_season_id`](reference/additional#mjhl_season_id)
- [`most_recent_mjhl_season`](reference/additional#most_recent_mjhl_season)


## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)
