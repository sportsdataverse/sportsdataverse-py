---
title: Betting odds
sidebar_label: Betting odds
description: "sdv-py Betting odds: endpoint references, dataset loaders and parsers for Betting odds in the SportsDataverse Python package."
---
# Betting odds (`sportsdataverse.odds`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [The Odds API](#the-odds-api) | `the-odds-api.com` | 11 | API key (SDV_PY_ODDS_API_KEY) |
| [Polymarket](#polymarket) | `gamma-api.polymarket.com` | 8 | none (read-only Gamma metadata, CLOB order-book and Polymarket US gateway routes) |
| [Kalshi](#kalshi) | `api.elections.kalshi.com` | 9 | none (keyless Trade API v2 market-data routes) |
| [Additional functions](reference/additional) | hand-written wrappers & helpers | 14 | — |

## The Odds API {#the-odds-api}

| Reference | Functions |
|---|---:|
| [Hand-written wrappers](reference/additional) | 11 |

## Polymarket {#polymarket}

| Reference | Functions |
|---|---:|
| [Polymarket read APIs (gamma-api + clob.polymarket.com)](reference/polymarket) | 8 |

## Kalshi {#kalshi}

| Reference | Functions |
|---|---:|
| [Kalshi Trade API v2 market data (api.elections.kalshi.com)](reference/kalshi) | 9 |
## Tools and helpers

### IDs and crosswalks {#ids-and-crosswalks}

- [`MarketLeague`](reference/additional#MarketLeague)
- [`join_espn_kalshi`](reference/additional#join_espn_kalshi)
- [`join_espn_polymarket`](reference/additional#join_espn_polymarket)


## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)
- [Betting odds tutorial](../tutorials/12_odds_intro.md)
