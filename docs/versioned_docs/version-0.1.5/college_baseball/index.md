---
title: COLLEGE_BASEBALL
sidebar_label: COLLEGE_BASEBALL
description: "sdv-py COLLEGE_BASEBALL: endpoint references, dataset loaders and parsers for COLLEGE_BASEBALL in the SportsDataverse Python package."
---
# COLLEGE_BASEBALL (`sportsdataverse.college_baseball`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [ESPN](#espn) | `cdn.espn.com`, `site.api.espn.com`, `site.web.api.espn.com` +1 more | 122 | none |
| [stats.ncaa.org](#stats-ncaa-org) | `stats.ncaa.org` | 3 | none (Terms gate + rate rotation) |
| [Additional functions](reference/additional) | hand-written wrappers & helpers | 4 | — |

## ESPN {#espn}

| Reference | Functions |
|---|---:|
| [ESPN site API (v2)](reference/site) | 25 |
| [ESPN web API (v3)](reference/web) | 5 |
| [ESPN core API (v2)](reference/core) | 87 |
| [ESPN FPI API (fitt v3)](reference/fitt) | 1 |
| [ESPN CDN API (cdn.espn.com)](reference/cdn) | 4 |

## stats.ncaa.org {#stats-ncaa-org}

| Reference | Functions |
|---|---:|
| [Hand-written wrappers](reference/additional) | 3 |
## Tools and helpers

### Play-by-play processing {#play-by-play-processing}

- [`decompose_college_baseball_plays`](reference/additional#decompose_college_baseball_plays)


## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)
