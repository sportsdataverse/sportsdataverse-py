# EuroLeague

> sdv-py EuroLeague: endpoint references, dataset loaders and parsers for EuroLeague in the SportsDataverse Python package.

# EuroLeague (`sportsdataverse.basketball.euroleague`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [EuroLeague Competition Engine](#euroleague-competition-engine) | `api-live.euroleague.net`, `live.euroleague.net` | 15 | none |

## EuroLeague Competition Engine {#euroleague-competition-engine}

| Reference | Functions |
|---|---:|
| [EuroLeague APIs (api-live.euroleague.net v2 + v3, live.euroleague.net/api)](reference/euroleague) | 15 |

## See also

- [hoopR](https://hoopR.sportsdataverse.org) — the R home of SportsDataverse basketball, where the EuroLeague wrappers land next
- [sdvplot](https://sdvplot.sportsdataverse.org/) — draws the FIBA court the shot chart's hoop-origin centimeter coordinates land on
- [sdvplotR](https://sdvplotR.sportsdataverse.org/) — the R twin, same FIBA court
- [euroleague-api](https://github.com/giasemidis/euroleague_api) — community Python client for the same EuroLeague and EuroCup APIs

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)

## Python ↔ R parity

Each `sportsdataverse` function and its equivalent in the sister R package, [`hoopR`](https://hoopR.sportsdataverse.org). Same-named where possible; the R column links the package's pkgdown reference.

| `sportsdataverse.basketball.euroleague` (Python) | `hoopR` (R) |
|---|---|
| [`euroleague_clubs`](reference/euroleague/other#euroleague_clubs) | [`euroleague_clubs`](https://hoopR.sportsdataverse.org/reference/euroleague_clubs.html) |
| [`euroleague_competitions`](reference/euroleague/other#euroleague_competitions) | [`euroleague_competitions`](https://hoopR.sportsdataverse.org/reference/euroleague_competitions.html) |
| [`euroleague_game_boxscore`](reference/euroleague/game#euroleague_game_boxscore) | [`euroleague_game_boxscore`](https://hoopR.sportsdataverse.org/reference/euroleague_game_boxscore.html) |
| [`euroleague_game_header`](reference/euroleague/game#euroleague_game_header) | [`euroleague_game_header`](https://hoopR.sportsdataverse.org/reference/euroleague_game_header.html) |
| [`euroleague_game_pbp`](reference/euroleague/game#euroleague_game_pbp) | [`euroleague_game_pbp`](https://hoopR.sportsdataverse.org/reference/euroleague_game_pbp.html) |
| [`euroleague_game_points`](reference/euroleague/game#euroleague_game_points) | [`euroleague_game_points`](https://hoopR.sportsdataverse.org/reference/euroleague_game_points.html) |
| [`euroleague_game_report`](reference/euroleague/game#euroleague_game_report) | [`euroleague_game_report`](https://hoopR.sportsdataverse.org/reference/euroleague_game_report.html) |
| [`euroleague_game_stats`](reference/euroleague/game#euroleague_game_stats) | [`euroleague_game_stats`](https://hoopR.sportsdataverse.org/reference/euroleague_game_stats.html) |
| [`euroleague_games`](reference/euroleague/other#euroleague_games) | [`euroleague_games`](https://hoopR.sportsdataverse.org/reference/euroleague_games.html) |
| [`euroleague_people`](reference/euroleague/other#euroleague_people) | [`euroleague_people`](https://hoopR.sportsdataverse.org/reference/euroleague_people.html) |
| [`euroleague_player_stats`](reference/euroleague/other#euroleague_player_stats) | [`euroleague_player_stats`](https://hoopR.sportsdataverse.org/reference/euroleague_player_stats.html) |
| [`euroleague_rounds`](reference/euroleague/other#euroleague_rounds) | [`euroleague_rounds`](https://hoopR.sportsdataverse.org/reference/euroleague_rounds.html) |
| [`euroleague_seasons`](reference/euroleague/other#euroleague_seasons) | [`euroleague_seasons`](https://hoopR.sportsdataverse.org/reference/euroleague_seasons.html) |
| [`euroleague_standings`](reference/euroleague/other#euroleague_standings) | [`euroleague_standings`](https://hoopR.sportsdataverse.org/reference/euroleague_standings.html) |
| [`euroleague_team_stats`](reference/euroleague/other#euroleague_team_stats) | [`euroleague_team_stats`](https://hoopR.sportsdataverse.org/reference/euroleague_team_stats.html) |
