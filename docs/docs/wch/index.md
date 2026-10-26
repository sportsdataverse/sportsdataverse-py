---
title: WCH
sidebar_label: WCH
description: "sdv-py WCH: endpoint references, dataset loaders and parsers for WCH in the SportsDataverse Python package."
---
# WCH (`sportsdataverse.hockey.wch`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [ESPN](#espn) | `site.api.espn.com`, `site.web.api.espn.com`, `sports.core.api.espn.com` | 118 | none |

## ESPN {#espn}

| Reference | Functions |
|---|---:|
| [ESPN site API (v2)](reference/site) | 25 |
| [ESPN web API (v3)](reference/web) | 5 |
| [ESPN core API (v2)](reference/core) | 87 |
| [ESPN FPI API (fitt v3)](reference/fitt) | 1 |

## See also

- [sdvplot](https://sdvplot.sportsdataverse.org/) — `surface("ncaa_whockey")` draws the NCAA rink and `team_colors("ncaa_whockey", ...)` the school colors
- [sportypy](https://sportypy.sportsdataverse.org/) — the regulation NCAA rink sdvplot draws with

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)
