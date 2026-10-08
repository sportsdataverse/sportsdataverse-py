---
title: MCH
sidebar_label: MCH
description: "sdv-py MCH: endpoint references, dataset loaders and parsers for MCH in the SportsDataverse Python package."
---
# MCH (`sportsdataverse.mch`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [ESPN](#espn) | `site.api.espn.com`, `site.web.api.espn.com`, `sports.core.api.espn.com` | 118 | none |
| [Additional functions](reference/additional) | hand-written wrappers & helpers | 1 | — |

## ESPN {#espn}

| Reference | Functions |
|---|---:|
| [ESPN site API (v2)](reference/site) | 25 |
| [ESPN web API (v3)](reference/web) | 5 |
| [ESPN core API (v2)](reference/core) | 87 |
| [ESPN FPI API (fitt v3)](reference/fitt) | 1 |
## Tools and helpers

### Models and calculators {#models-and-calculators}

- [`mch_ratings`](reference/additional#mch_ratings)


## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)
