---
title: VIJHL
sidebar_label: VIJHL
description: "sdv-py VIJHL: endpoint references, dataset loaders and parsers for VIJHL in the SportsDataverse Python package."
---
# VIJHL (`sportsdataverse.vijhl`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [HockeyTech / LeagueStat](#hockeytech-leaguestat) | `lscluster.hockeytech.com` | 11 | per-league public key (SDV_<LEAGUE>_API_KEY) |
| [Additional functions](reference/additional) | hand-written wrappers & helpers | 14 | — |

## HockeyTech / LeagueStat {#hockeytech-leaguestat}

| Reference | Functions |
|---|---:|
| [Hand-written wrappers](reference/additional) | 11 |
## Tools and helpers

### Play-by-play processing {#play-by-play-processing}

- [`build_family`](reference/additional#build_family)

### Dates and seasons {#dates-and-seasons}

- [`most_recent_vijhl_season`](reference/additional#most_recent_vijhl_season)
- [`vijhl_season_id`](reference/additional#vijhl_season_id)


## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)
