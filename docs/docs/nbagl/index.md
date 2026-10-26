---
title: NBA G League
sidebar_label: NBA G League
description: "sdv-py NBA G League: endpoint references, dataset loaders and parsers for NBA G League in the SportsDataverse Python package."
---
# NBA G League (`sportsdataverse.nbagl`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [ESPN](#espn) | `site.api.espn.com`, `site.web.api.espn.com`, `sports.core.api.espn.com` | 112 | none |
| [G League Stats API](#g-league-stats-api) | `stats.nba.com` | 94 | none (curl_cffi chrome TLS impersonation) |

## ESPN {#espn}

| Reference | Functions |
|---|---:|
| [ESPN site API (v2)](reference/site) | 24 |
| [ESPN web API (v3)](reference/web) | 5 |
| [ESPN core API (v2)](reference/core) | 82 |
| [ESPN FPI API (fitt v3)](reference/fitt) | 1 |

## G League Stats API {#g-league-stats-api}

| Reference | Functions |
|---|---:|
| [G League Stats API (stats.nba.com, LeagueID=20)](reference/nbagl_stats) | 94 |

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)
