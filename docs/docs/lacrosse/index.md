---
title: Lacrosse
sidebar_label: Lacrosse
description: "sdv-py Lacrosse: endpoint references, dataset loaders and parsers for Lacrosse in the SportsDataverse Python package."
---
# Lacrosse (`sportsdataverse.lacrosse`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [ESPN](#espn) | `site.api.espn.com`, `sports.core.api.espn.com` | 35 | none |
| [Premier Lacrosse League stats API](#premier-lacrosse-league-stats-api) | `stats.premierlacrosseleague.com` | 19 | none (no key; the proxy answers only the stats site's own Referer, which the runtime sends) |

## ESPN {#espn}

| Reference | Functions |
|---|---:|
| [ESPN site API (v2)](reference/site) | 10 |
| [ESPN core API (v2)](reference/core) | 25 |

## Premier Lacrosse League stats API {#premier-lacrosse-league-stats-api}

| Reference | Functions |
|---|---:|
| [Premier Lacrosse League stats API (stats.premierlacrosseleague.com; runtime only, no key, the site's Referer)](reference/pll) | 19 |

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)
