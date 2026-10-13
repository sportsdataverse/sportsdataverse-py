# AJHL

> sdv-py AJHL: endpoint references, dataset loaders and parsers for AJHL in the SportsDataverse Python package.

# AJHL (`sportsdataverse.ajhl`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [HockeyTech / LeagueStat](#hockeytech-leaguestat) | `lscluster.hockeytech.com` | 12 | per-league public key (SDV__API_KEY) |
| [Additional functions](reference/additional) | hand-written wrappers & helpers | 14 | — |

## HockeyTech / LeagueStat {#hockeytech-leaguestat}

| Reference | Functions |
|---|---:|
| [Hand-written wrappers](reference/additional) | 12 |
## Tools and helpers

### Dates and seasons {#dates-and-seasons}

- [`ajhl_season_id`](reference/additional#ajhl_season_id)
- [`most_recent_ajhl_season`](reference/additional#most_recent_ajhl_season)

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)
