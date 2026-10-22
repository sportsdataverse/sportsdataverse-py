# ODDS

> sdv-py ODDS: endpoint references, dataset loaders and parsers for ODDS in the SportsDataverse Python package.

# ODDS (`sportsdataverse.odds`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [The Odds API](#the-odds-api) | `the-odds-api.com` | 11 | API key (SDV_PY_ODDS_API_KEY) |
| [Polymarket](#polymarket) | `gamma-api.polymarket.com` | 8 | none (read-only Gamma metadata + CLOB order-book routes) |
| [Kalshi](#kalshi) | `api.elections.kalshi.com` | 9 | none (keyless Trade API v2 market-data routes) |
| [Additional functions](reference/additional) | hand-written wrappers & helpers | 11 | — |

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

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)
- [Betting odds tutorial](../tutorials/12_odds_intro.md)
