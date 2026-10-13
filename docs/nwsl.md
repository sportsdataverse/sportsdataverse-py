# NWSL

> sdv-py NWSL: endpoint references, dataset loaders and parsers for NWSL in the SportsDataverse Python package.

# NWSL (`sportsdataverse.nwsl`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [ESPN](#espn) | `site.api.espn.com`, `site.web.api.espn.com`, `sports.core.api.espn.com` | 112 | none |
| [NWSL official web API](#nwsl-official-web-api) | `api-sdp.nwslsoccer.com` | 9 | none |

## ESPN {#espn}

| Reference | Functions |
|---|---:|
| [ESPN site API (v2)](reference/site) | 24 |
| [ESPN web API (v3)](reference/web) | 5 |
| [ESPN core API (v2)](reference/core) | 82 |
| [ESPN FPI API (fitt v3)](reference/fitt) | 1 |

## NWSL official web API {#nwsl-official-web-api}

| Reference | Functions |
|---|---:|
| [NWSL official web API (StatsPerform SDP)](reference/nwsl_api) | 9 |

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)
