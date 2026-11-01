# USHL

> sdv-py USHL: endpoint references, dataset loaders and parsers for USHL in the SportsDataverse Python package.

# USHL (`sportsdataverse.hockey.ushl`)

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

- [`most_recent_ushl_season`](reference/additional#most_recent_ushl_season)
- [`ushl_season_id`](reference/additional#ushl_season_id)

## See also

- [sdvplot](https://sdvplot.sportsdataverse.org/) — `surface("ushl")` draws the rink in a team's colors; `team_colors()` takes HockeyTech team ids
- [sportypy](https://sportypy.sportsdataverse.org/) — the rink sdvplot draws with

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)
