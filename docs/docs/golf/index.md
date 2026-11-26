---
title: Golf
sidebar_label: Golf
description: "sdv-py Golf: endpoint references, dataset loaders and parsers for Golf in the SportsDataverse Python package."
---
# Golf (`sportsdataverse.golf`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [ESPN](#espn) | `site.api.espn.com`, `site.web.api.espn.com`, `sports.core.api.espn.com` | 46 | none |
| [PGA TOUR GraphQL API](#pga-tour-graphql-api) | `orchestrator.pgatour.com` | 21 | public frontend key (read from pgatour.com on first use; $PGA_API_KEY to supply one) |

## ESPN {#espn}

| Reference | Functions |
|---|---:|
| [ESPN site API (v2)](reference/site) | 9 |
| [ESPN web API (v3)](reference/web) | 2 |
| [ESPN core API (v2)](reference/core) | 35 |

## PGA TOUR GraphQL API {#pga-tour-graphql-api}

| Reference | Functions |
|---|---:|
| [PGA TOUR GraphQL API (orchestrator.pgatour.com; runtime only, public frontend key or $PGA_API_KEY)](reference/pgatour) | 21 |

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)
