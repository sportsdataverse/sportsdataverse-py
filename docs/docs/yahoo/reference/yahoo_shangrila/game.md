---
title: "YAHOO — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Game"
sidebar_label: "Game"
sidebar_position: 3
description: "YAHOO — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Game — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# YAHOO — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Game

## yahoo_game_prop_bets

Yahoo shangrila persisted query `gamePropBets` -> one row per `games` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/gamePropBets`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/gamePropBets](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/gamePropBets)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameId` | `game_id` |  |  | `Y` | gameId query parameter. |

### Returns {#yahoo_game_prop_bets-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `alias_path` | character | Site-relative path portion of the entity's Yahoo alias (e.g., "/ncaaf/teams/tcu/"). |
| `alias_lang` | character | Language/locale tag attached to the entity's Yahoo alias (e.g., "en-US"). |
| `alias_url` | character | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `alias_domain` | character | Host the entity's Yahoo alias resolves against (e.g., "sports.yahoo.com"). |
| `game_id` | character | Unique game identifier. |
| `status` | character | Status label. |
| `away_team_team_id` | character | Yahoo composite team id of the away team (e.g., "ncaaf.t.29"). |
| `away_team_primary_color` | character | Primary brand color of the away team, as a hex RGB string without the leading hash. |
| `away_team_display_name` | character | Away team full display name; `team_detail = TRUE` only. |
| `home_team_team_id` | character | Yahoo composite team id of the home team (e.g., "ncaaf.t.29"). |
| `home_team_primary_color` | character | Primary brand color of the home team, as a hex RGB string without the leading hash. |
| `home_team_display_name` | character | Home team full display name; `team_detail = TRUE` only. |
| `active_prop_bets` | character | JSON-encoded list of the prop-bet markets currently open for the game. |
| `game_props` | character | JSON-encoded list of player and game prop markets offered on the game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_game_prop_bets-example}

```python
yahoo_game_prop_bets()
```

_Last validated n/a._

## yahoo_game_stats_leaders

Yahoo shangrila persisted query `gameStatsLeaders` -> one row per `games` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/gameStatsLeaders`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/gameStatsLeaders](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/gameStatsLeaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameId` | `game_id` |  |  | `Y` | gameId query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `seasonPhases` | `season_phases` |  |  | `Y` | seasonPhases query parameter. |
| `qualified` | `qualified` |  |  | `Y` | qualified query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `isPregame` | `is_pregame` |  |  | `Y` | isPregame query parameter. |
| `teamImageHeight` | `team_image_height` |  |  | `Y` | teamImageHeight query parameter. |
| `teamImageWidth` | `team_image_width` |  |  | `Y` | teamImageWidth query parameter. |
| `playerImageHeight` | `player_image_height` |  |  | `Y` | playerImageHeight query parameter. |
| `playerImageWidth` | `player_image_width` |  |  | `Y` | playerImageWidth query parameter. |
| `baseballLeaderSortStat0` | `baseball_leader_sort_stat0` |  |  | `Y` | baseballLeaderSortStat0 query parameter. |
| `baseballLeaderSortStat1` | `baseball_leader_sort_stat1` |  |  | `Y` | baseballLeaderSortStat1 query parameter. |
| `baseballLeaderSortStat2` | `baseball_leader_sort_stat2` |  |  | `Y` | baseballLeaderSortStat2 query parameter. |
| `baseballLeaderSortStat3` | `baseball_leader_sort_stat3` |  |  | `Y` | baseballLeaderSortStat3 query parameter. |
| `baseballLeaderSortStat4` | `baseball_leader_sort_stat4` |  |  | `Y` | baseballLeaderSortStat4 query parameter. |
| `baseballLeaderStatIds0` | `baseball_leader_stat_ids0` |  |  | `Y` | baseballLeaderStatIds0 query parameter. |
| `baseballLeaderStatIds1` | `baseball_leader_stat_ids1` |  |  | `Y` | baseballLeaderStatIds1 query parameter. |
| `baseballLeaderStatIds2` | `baseball_leader_stat_ids2` |  |  | `Y` | baseballLeaderStatIds2 query parameter. |
| `baseballLeaderStatIds3` | `baseball_leader_stat_ids3` |  |  | `Y` | baseballLeaderStatIds3 query parameter. |
| `baseballLeaderStatIds4` | `baseball_leader_stat_ids4` |  |  | `Y` | baseballLeaderStatIds4 query parameter. |
| `baseballPlayerStatIds0` | `baseball_player_stat_ids0` |  |  | `Y` | baseballPlayerStatIds0 query parameter. |
| `baseballPlayerStatIds1` | `baseball_player_stat_ids1` |  |  | `Y` | baseballPlayerStatIds1 query parameter. |
| `baseballTeamSortStat0` | `baseball_team_sort_stat0` |  |  | `Y` | baseballTeamSortStat0 query parameter. |
| `baseballTeamSortStat1` | `baseball_team_sort_stat1` |  |  | `Y` | baseballTeamSortStat1 query parameter. |
| `baseballTeamSortStat2` | `baseball_team_sort_stat2` |  |  | `Y` | baseballTeamSortStat2 query parameter. |
| `baseballTeamSortStat3` | `baseball_team_sort_stat3` |  |  | `Y` | baseballTeamSortStat3 query parameter. |
| `baseballTeamSortStat4` | `baseball_team_sort_stat4` |  |  | `Y` | baseballTeamSortStat4 query parameter. |
| `baseballTeamSortStat5` | `baseball_team_sort_stat5` |  |  | `Y` | baseballTeamSortStat5 query parameter. |
| `baseballTeamSortStat6` | `baseball_team_sort_stat6` |  |  | `Y` | baseballTeamSortStat6 query parameter. |
| `baseballTeamSortStat7` | `baseball_team_sort_stat7` |  |  | `Y` | baseballTeamSortStat7 query parameter. |
| `baseballTeamSortStat8` | `baseball_team_sort_stat8` |  |  | `Y` | baseballTeamSortStat8 query parameter. |
| `baseballTeamSortStat9` | `baseball_team_sort_stat9` |  |  | `Y` | baseballTeamSortStat9 query parameter. |
| `baseballTeamSortStat10` | `baseball_team_sort_stat10` |  |  | `Y` | baseballTeamSortStat10 query parameter. |
| `baseballTeamSortStat11` | `baseball_team_sort_stat11` |  |  | `Y` | baseballTeamSortStat11 query parameter. |
| `baseballTeamStatIds0` | `baseball_team_stat_ids0` |  |  | `Y` | baseballTeamStatIds0 query parameter. |
| `baseballTeamStatIds1` | `baseball_team_stat_ids1` |  |  | `Y` | baseballTeamStatIds1 query parameter. |
| `baseballTeamStatIds2` | `baseball_team_stat_ids2` |  |  | `Y` | baseballTeamStatIds2 query parameter. |
| `baseballTeamStatIds3` | `baseball_team_stat_ids3` |  |  | `Y` | baseballTeamStatIds3 query parameter. |
| `baseballTeamStatIds4` | `baseball_team_stat_ids4` |  |  | `Y` | baseballTeamStatIds4 query parameter. |
| `baseballTeamStatIds5` | `baseball_team_stat_ids5` |  |  | `Y` | baseballTeamStatIds5 query parameter. |
| `baseballTeamStatIds6` | `baseball_team_stat_ids6` |  |  | `Y` | baseballTeamStatIds6 query parameter. |
| `baseballTeamStatIds7` | `baseball_team_stat_ids7` |  |  | `Y` | baseballTeamStatIds7 query parameter. |
| `baseballTeamStatIds8` | `baseball_team_stat_ids8` |  |  | `Y` | baseballTeamStatIds8 query parameter. |
| `baseballTeamStatIds9` | `baseball_team_stat_ids9` |  |  | `Y` | baseballTeamStatIds9 query parameter. |
| `baseballTeamStatIds10` | `baseball_team_stat_ids10` |  |  | `Y` | baseballTeamStatIds10 query parameter. |
| `baseballTeamStatIds11` | `baseball_team_stat_ids11` |  |  | `Y` | baseballTeamStatIds11 query parameter. |
| `basketballLeaderSortStat0` | `basketball_leader_sort_stat0` |  |  | `Y` | basketballLeaderSortStat0 query parameter. |
| `basketballLeaderSortStat1` | `basketball_leader_sort_stat1` |  |  | `Y` | basketballLeaderSortStat1 query parameter. |
| `basketballLeaderSortStat2` | `basketball_leader_sort_stat2` |  |  | `Y` | basketballLeaderSortStat2 query parameter. |
| `basketballLeaderSortStat3` | `basketball_leader_sort_stat3` |  |  | `Y` | basketballLeaderSortStat3 query parameter. |
| `basketballLeaderSortStat4` | `basketball_leader_sort_stat4` |  |  | `Y` | basketballLeaderSortStat4 query parameter. |
| `basketballLeaderStatIds0` | `basketball_leader_stat_ids0` |  |  | `Y` | basketballLeaderStatIds0 query parameter. |
| `basketballLeaderStatIds1` | `basketball_leader_stat_ids1` |  |  | `Y` | basketballLeaderStatIds1 query parameter. |
| `basketballLeaderStatIds2` | `basketball_leader_stat_ids2` |  |  | `Y` | basketballLeaderStatIds2 query parameter. |
| `basketballLeaderStatIds3` | `basketball_leader_stat_ids3` |  |  | `Y` | basketballLeaderStatIds3 query parameter. |
| `basketballLeaderStatIds4` | `basketball_leader_stat_ids4` |  |  | `Y` | basketballLeaderStatIds4 query parameter. |
| `basketballPlayerStatIds0` | `basketball_player_stat_ids0` |  |  | `Y` | basketballPlayerStatIds0 query parameter. |
| `basketballTeamSortStat0` | `basketball_team_sort_stat0` |  |  | `Y` | basketballTeamSortStat0 query parameter. |
| `basketballTeamSortStat1` | `basketball_team_sort_stat1` |  |  | `Y` | basketballTeamSortStat1 query parameter. |
| `basketballTeamSortStat2` | `basketball_team_sort_stat2` |  |  | `Y` | basketballTeamSortStat2 query parameter. |
| `basketballTeamSortStat3` | `basketball_team_sort_stat3` |  |  | `Y` | basketballTeamSortStat3 query parameter. |
| `basketballTeamSortStat4` | `basketball_team_sort_stat4` |  |  | `Y` | basketballTeamSortStat4 query parameter. |
| `basketballTeamSortStat5` | `basketball_team_sort_stat5` |  |  | `Y` | basketballTeamSortStat5 query parameter. |
| `basketballTeamSortStat6` | `basketball_team_sort_stat6` |  |  | `Y` | basketballTeamSortStat6 query parameter. |
| `basketballTeamSortStat7` | `basketball_team_sort_stat7` |  |  | `Y` | basketballTeamSortStat7 query parameter. |
| `basketballTeamSortStat8` | `basketball_team_sort_stat8` |  |  | `Y` | basketballTeamSortStat8 query parameter. |
| `basketballTeamSortStat9` | `basketball_team_sort_stat9` |  |  | `Y` | basketballTeamSortStat9 query parameter. |
| `basketballTeamStatIds0` | `basketball_team_stat_ids0` |  |  | `Y` | basketballTeamStatIds0 query parameter. |
| `basketballTeamStatIds1` | `basketball_team_stat_ids1` |  |  | `Y` | basketballTeamStatIds1 query parameter. |
| `basketballTeamStatIds2` | `basketball_team_stat_ids2` |  |  | `Y` | basketballTeamStatIds2 query parameter. |
| `basketballTeamStatIds3` | `basketball_team_stat_ids3` |  |  | `Y` | basketballTeamStatIds3 query parameter. |
| `basketballTeamStatIds4` | `basketball_team_stat_ids4` |  |  | `Y` | basketballTeamStatIds4 query parameter. |
| `basketballTeamStatIds5` | `basketball_team_stat_ids5` |  |  | `Y` | basketballTeamStatIds5 query parameter. |
| `basketballTeamStatIds6` | `basketball_team_stat_ids6` |  |  | `Y` | basketballTeamStatIds6 query parameter. |
| `basketballTeamStatIds7` | `basketball_team_stat_ids7` |  |  | `Y` | basketballTeamStatIds7 query parameter. |
| `basketballTeamStatIds8` | `basketball_team_stat_ids8` |  |  | `Y` | basketballTeamStatIds8 query parameter. |
| `basketballTeamStatIds9` | `basketball_team_stat_ids9` |  |  | `Y` | basketballTeamStatIds9 query parameter. |
| `footballLeaderSortStat0` | `football_leader_sort_stat0` |  |  | `Y` | footballLeaderSortStat0 query parameter. |
| `footballLeaderSortStat1` | `football_leader_sort_stat1` |  |  | `Y` | footballLeaderSortStat1 query parameter. |
| `footballLeaderSortStat2` | `football_leader_sort_stat2` |  |  | `Y` | footballLeaderSortStat2 query parameter. |
| `footballLeaderSortStat3` | `football_leader_sort_stat3` |  |  | `Y` | footballLeaderSortStat3 query parameter. |
| `footballLeaderStatIds0` | `football_leader_stat_ids0` |  |  | `Y` | footballLeaderStatIds0 query parameter. |
| `footballLeaderStatIds1` | `football_leader_stat_ids1` |  |  | `Y` | footballLeaderStatIds1 query parameter. |
| `footballLeaderStatIds2` | `football_leader_stat_ids2` |  |  | `Y` | footballLeaderStatIds2 query parameter. |
| `footballLeaderStatIds3` | `football_leader_stat_ids3` |  |  | `Y` | footballLeaderStatIds3 query parameter. |
| `footballPlayerStatIds0` | `football_player_stat_ids0` |  |  | `Y` | footballPlayerStatIds0 query parameter. |
| `footballPlayerStatIds1` | `football_player_stat_ids1` |  |  | `Y` | footballPlayerStatIds1 query parameter. |
| `footballPlayerStatIds2` | `football_player_stat_ids2` |  |  | `Y` | footballPlayerStatIds2 query parameter. |
| `footballPlayerStatIds3` | `football_player_stat_ids3` |  |  | `Y` | footballPlayerStatIds3 query parameter. |
| `footballPlayerStatIds4` | `football_player_stat_ids4` |  |  | `Y` | footballPlayerStatIds4 query parameter. |
| `footballPlayerStatIds5` | `football_player_stat_ids5` |  |  | `Y` | footballPlayerStatIds5 query parameter. |
| `footballPlayerStatIds6` | `football_player_stat_ids6` |  |  | `Y` | footballPlayerStatIds6 query parameter. |
| `footballPlayerStatIds7` | `football_player_stat_ids7` |  |  | `Y` | footballPlayerStatIds7 query parameter. |
| `footballTeamSortStat0` | `football_team_sort_stat0` |  |  | `Y` | footballTeamSortStat0 query parameter. |
| `footballTeamSortStat1` | `football_team_sort_stat1` |  |  | `Y` | footballTeamSortStat1 query parameter. |
| `footballTeamSortStat2` | `football_team_sort_stat2` |  |  | `Y` | footballTeamSortStat2 query parameter. |
| `footballTeamSortStat3` | `football_team_sort_stat3` |  |  | `Y` | footballTeamSortStat3 query parameter. |
| `footballTeamSortStat4` | `football_team_sort_stat4` |  |  | `Y` | footballTeamSortStat4 query parameter. |
| `footballTeamSortStat5` | `football_team_sort_stat5` |  |  | `Y` | footballTeamSortStat5 query parameter. |
| `footballTeamSortStat6` | `football_team_sort_stat6` |  |  | `Y` | footballTeamSortStat6 query parameter. |
| `footballTeamSortStat7` | `football_team_sort_stat7` |  |  | `Y` | footballTeamSortStat7 query parameter. |
| `footballTeamSortStat8` | `football_team_sort_stat8` |  |  | `Y` | footballTeamSortStat8 query parameter. |
| `footballTeamSortStat9` | `football_team_sort_stat9` |  |  | `Y` | footballTeamSortStat9 query parameter. |
| `footballTeamSortStat10` | `football_team_sort_stat10` |  |  | `Y` | footballTeamSortStat10 query parameter. |
| `footballTeamSortStat11` | `football_team_sort_stat11` |  |  | `Y` | footballTeamSortStat11 query parameter. |
| `footballTeamStatIds0` | `football_team_stat_ids0` |  |  | `Y` | footballTeamStatIds0 query parameter. |
| `footballTeamStatIds1` | `football_team_stat_ids1` |  |  | `Y` | footballTeamStatIds1 query parameter. |
| `footballTeamStatIds2` | `football_team_stat_ids2` |  |  | `Y` | footballTeamStatIds2 query parameter. |
| `footballTeamStatIds3` | `football_team_stat_ids3` |  |  | `Y` | footballTeamStatIds3 query parameter. |
| `footballTeamStatIds4` | `football_team_stat_ids4` |  |  | `Y` | footballTeamStatIds4 query parameter. |
| `footballTeamStatIds5` | `football_team_stat_ids5` |  |  | `Y` | footballTeamStatIds5 query parameter. |
| `footballTeamStatIds6` | `football_team_stat_ids6` |  |  | `Y` | footballTeamStatIds6 query parameter. |
| `footballTeamStatIds7` | `football_team_stat_ids7` |  |  | `Y` | footballTeamStatIds7 query parameter. |
| `footballTeamStatIds8` | `football_team_stat_ids8` |  |  | `Y` | footballTeamStatIds8 query parameter. |
| `footballTeamStatIds9` | `football_team_stat_ids9` |  |  | `Y` | footballTeamStatIds9 query parameter. |
| `footballTeamStatIds10` | `football_team_stat_ids10` |  |  | `Y` | footballTeamStatIds10 query parameter. |
| `footballTeamStatIds11` | `football_team_stat_ids11` |  |  | `Y` | footballTeamStatIds11 query parameter. |
| `hockeyLeaderSortStat0` | `hockey_leader_sort_stat0` |  |  | `Y` | hockeyLeaderSortStat0 query parameter. |
| `hockeyLeaderSortStat1` | `hockey_leader_sort_stat1` |  |  | `Y` | hockeyLeaderSortStat1 query parameter. |
| `hockeyLeaderSortStat2` | `hockey_leader_sort_stat2` |  |  | `Y` | hockeyLeaderSortStat2 query parameter. |
| `hockeyLeaderSortStat3` | `hockey_leader_sort_stat3` |  |  | `Y` | hockeyLeaderSortStat3 query parameter. |
| `hockeyLeaderStatIds0` | `hockey_leader_stat_ids0` |  |  | `Y` | hockeyLeaderStatIds0 query parameter. |
| `hockeyLeaderStatIds1` | `hockey_leader_stat_ids1` |  |  | `Y` | hockeyLeaderStatIds1 query parameter. |
| `hockeyLeaderStatIds2` | `hockey_leader_stat_ids2` |  |  | `Y` | hockeyLeaderStatIds2 query parameter. |
| `hockeyLeaderStatIds3` | `hockey_leader_stat_ids3` |  |  | `Y` | hockeyLeaderStatIds3 query parameter. |
| `hockeyPlayerStatIds0` | `hockey_player_stat_ids0` |  |  | `Y` | hockeyPlayerStatIds0 query parameter. |
| `hockeyPlayerStatIds1` | `hockey_player_stat_ids1` |  |  | `Y` | hockeyPlayerStatIds1 query parameter. |
| `hockeyPlayerStatIds2` | `hockey_player_stat_ids2` |  |  | `Y` | hockeyPlayerStatIds2 query parameter. |
| `hockeyTeamSortStat0` | `hockey_team_sort_stat0` |  |  | `Y` | hockeyTeamSortStat0 query parameter. |
| `hockeyTeamSortStat1` | `hockey_team_sort_stat1` |  |  | `Y` | hockeyTeamSortStat1 query parameter. |
| `hockeyTeamSortStat2` | `hockey_team_sort_stat2` |  |  | `Y` | hockeyTeamSortStat2 query parameter. |
| `hockeyTeamSortStat3` | `hockey_team_sort_stat3` |  |  | `Y` | hockeyTeamSortStat3 query parameter. |
| `hockeyTeamSortStat4` | `hockey_team_sort_stat4` |  |  | `Y` | hockeyTeamSortStat4 query parameter. |
| `hockeyTeamSortStat5` | `hockey_team_sort_stat5` |  |  | `Y` | hockeyTeamSortStat5 query parameter. |
| `hockeyTeamSortStat6` | `hockey_team_sort_stat6` |  |  | `Y` | hockeyTeamSortStat6 query parameter. |
| `hockeyTeamStatIds0` | `hockey_team_stat_ids0` |  |  | `Y` | hockeyTeamStatIds0 query parameter. |
| `hockeyTeamStatIds1` | `hockey_team_stat_ids1` |  |  | `Y` | hockeyTeamStatIds1 query parameter. |
| `hockeyTeamStatIds2` | `hockey_team_stat_ids2` |  |  | `Y` | hockeyTeamStatIds2 query parameter. |
| `hockeyTeamStatIds3` | `hockey_team_stat_ids3` |  |  | `Y` | hockeyTeamStatIds3 query parameter. |
| `hockeyTeamStatIds4` | `hockey_team_stat_ids4` |  |  | `Y` | hockeyTeamStatIds4 query parameter. |
| `hockeyTeamStatIds5` | `hockey_team_stat_ids5` |  |  | `Y` | hockeyTeamStatIds5 query parameter. |
| `hockeyTeamStatIds6` | `hockey_team_stat_ids6` |  |  | `Y` | hockeyTeamStatIds6 query parameter. |
| `soccerPlayerStatIds0` | `soccer_player_stat_ids0` |  |  | `Y` | soccerPlayerStatIds0 query parameter. |
| `soccerPlayerStatIds1` | `soccer_player_stat_ids1` |  |  | `Y` | soccerPlayerStatIds1 query parameter. |
| `soccerPlayerStatIds2` | `soccer_player_stat_ids2` |  |  | `Y` | soccerPlayerStatIds2 query parameter. |
| `soccerPlayerStatIds3` | `soccer_player_stat_ids3` |  |  | `Y` | soccerPlayerStatIds3 query parameter. |
| `soccerPlayerStatIds4` | `soccer_player_stat_ids4` |  |  | `Y` | soccerPlayerStatIds4 query parameter. |
| `soccerTeamSortStat0` | `soccer_team_sort_stat0` |  |  | `Y` | soccerTeamSortStat0 query parameter. |
| `soccerTeamSortStat1` | `soccer_team_sort_stat1` |  |  | `Y` | soccerTeamSortStat1 query parameter. |
| `soccerTeamSortStat2` | `soccer_team_sort_stat2` |  |  | `Y` | soccerTeamSortStat2 query parameter. |
| `soccerTeamSortStat3` | `soccer_team_sort_stat3` |  |  | `Y` | soccerTeamSortStat3 query parameter. |
| `soccerTeamSortStat4` | `soccer_team_sort_stat4` |  |  | `Y` | soccerTeamSortStat4 query parameter. |
| `soccerTeamSortStat5` | `soccer_team_sort_stat5` |  |  | `Y` | soccerTeamSortStat5 query parameter. |
| `soccerTeamStatIds0` | `soccer_team_stat_ids0` |  |  | `Y` | soccerTeamStatIds0 query parameter. |
| `soccerTeamStatIds1` | `soccer_team_stat_ids1` |  |  | `Y` | soccerTeamStatIds1 query parameter. |
| `soccerTeamStatIds2` | `soccer_team_stat_ids2` |  |  | `Y` | soccerTeamStatIds2 query parameter. |
| `soccerTeamStatIds3` | `soccer_team_stat_ids3` |  |  | `Y` | soccerTeamStatIds3 query parameter. |
| `soccerTeamStatIds4` | `soccer_team_stat_ids4` |  |  | `Y` | soccerTeamStatIds4 query parameter. |
| `soccerTeamStatIds5` | `soccer_team_stat_ids5` |  |  | `Y` | soccerTeamStatIds5 query parameter. |

### Returns {#yahoo_game_stats_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `status` | character | Status label. |
| `league_full_name` | character | Full league name (e.g., "NCAA Football"). |
| `league_football_team_season_stats0` | character | JSON-encoded league-wide team season-stat leader board occupying slot 0 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `league_football_team_season_stats1` | character | JSON-encoded league-wide team season-stat leader board occupying slot 1 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `league_football_team_season_stats2` | character | JSON-encoded league-wide team season-stat leader board occupying slot 2 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `league_football_team_season_stats3` | character | JSON-encoded league-wide team season-stat leader board occupying slot 3 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `league_football_team_season_stats4` | character | JSON-encoded league-wide team season-stat leader board occupying slot 4 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `league_football_team_season_stats5` | character | JSON-encoded league-wide team season-stat leader board occupying slot 5 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `league_football_team_season_stats6` | character | JSON-encoded league-wide team season-stat leader board occupying slot 6 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `league_football_team_season_stats7` | character | JSON-encoded league-wide team season-stat leader board occupying slot 7 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `league_football_team_season_stats8` | character | JSON-encoded league-wide team season-stat leader board occupying slot 8 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `league_football_team_season_stats9` | character | JSON-encoded league-wide team season-stat leader board occupying slot 9 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `league_football_team_season_stats10` | character | JSON-encoded league-wide team season-stat leader board occupying slot 10 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `league_football_team_season_stats11` | character | JSON-encoded league-wide team season-stat leader board occupying slot 11 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `game_leader_stats0` | character | JSON-encoded in-game statistical leader board occupying slot 0 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `game_leader_stats1` | character | JSON-encoded in-game statistical leader board occupying slot 1 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `game_leader_stats2` | character | JSON-encoded in-game statistical leader board occupying slot 2 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `game_leader_stats3` | character | JSON-encoded in-game statistical leader board occupying slot 3 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `away_team_game_stats0_stats` | character | JSON-encoded away-team game-stat block occupying slot 0 of that team's game-stats list; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `away_team_game_stats1_stats` | character | JSON-encoded away-team game-stat block occupying slot 1 of that team's game-stats list; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `home_team_game_stats0_stats` | character | JSON-encoded home-team game-stat block occupying slot 0 of that team's game-stats list; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `home_team_game_stats1_stats` | character | JSON-encoded home-team game-stat block occupying slot 1 of that team's game-stats list; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `home_team_lineup` | character | JSON-encoded starting lineup fielded by the home team. |
| `away_team_lineup` | character | JSON-encoded starting lineup fielded by the away team. |
| `away_team_id` | character | Unique identifier for the away team. |
| `away_team_full_name` | character | Full away team name (e.g. 'Las Vegas Aces'). |
| `away_team_team_id` | character | Yahoo composite team id of the away team (e.g., "ncaaf.t.29"). |
| `away_team_primary_color` | character | Primary brand color of the away team, as a hex RGB string without the leading hash. |
| `away_team_secondary_color` | character | Secondary brand color of the away team, as a hex RGB string without the leading hash. |
| `away_team_display_name` | character | Away team full display name; `team_detail = TRUE` only. |
| `away_team_abbreviation` | character | Away team abbreviation; `team_detail = TRUE` only. |
| `away_team_team_logo_white` | character | JSON-encoded image node for the away team's white knockout logo, used on dark backgrounds. |
| `away_team_team_logo` | character | JSON-encoded image node for the away team's standard logo. |
| `away_team_league` | character | JSON-encoded league node identifying the league the away team plays in. |
| `away_team_season_leader_stats0` | character | JSON-encoded away-team season leader board occupying slot 0 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `away_team_season_leader_stats1` | character | JSON-encoded away-team season leader board occupying slot 1 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `away_team_season_leader_stats2` | character | JSON-encoded away-team season leader board occupying slot 2 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `away_team_season_leader_stats3` | character | JSON-encoded away-team season leader board occupying slot 3 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `away_team_player_season_stats0` | character | JSON-encoded away-team player season-stat block occupying slot 0 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `away_team_player_season_stats1` | character | JSON-encoded away-team player season-stat block occupying slot 1 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `away_team_player_season_stats2` | character | JSON-encoded away-team player season-stat block occupying slot 2 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `away_team_player_season_stats3` | character | JSON-encoded away-team player season-stat block occupying slot 3 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `away_team_player_season_stats4` | character | JSON-encoded away-team player season-stat block occupying slot 4 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `away_team_player_season_stats5` | character | JSON-encoded away-team player season-stat block occupying slot 5 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `away_team_player_season_stats6` | character | JSON-encoded away-team player season-stat block occupying slot 6 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `away_team_player_season_stats7` | character | JSON-encoded away-team player season-stat block occupying slot 7 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `home_team_id` | character | Unique identifier for the home team. |
| `home_team_full_name` | character | Full home team name (e.g. 'Las Vegas Aces'). |
| `home_team_team_id` | character | Yahoo composite team id of the home team (e.g., "ncaaf.t.29"). |
| `home_team_primary_color` | character | Primary brand color of the home team, as a hex RGB string without the leading hash. |
| `home_team_secondary_color` | character | Secondary brand color of the home team, as a hex RGB string without the leading hash. |
| `home_team_display_name` | character | Home team full display name; `team_detail = TRUE` only. |
| `home_team_abbreviation` | character | Home team abbreviation; `team_detail = TRUE` only. |
| `home_team_team_logo_white` | character | JSON-encoded image node for the home team's white knockout logo, used on dark backgrounds. |
| `home_team_team_logo` | character | JSON-encoded image node for the home team's standard logo. |
| `home_team_league` | character | JSON-encoded league node identifying the league the home team plays in. |
| `home_team_season_leader_stats0` | character | JSON-encoded home-team season leader board occupying slot 0 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `home_team_season_leader_stats1` | character | JSON-encoded home-team season leader board occupying slot 1 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `home_team_season_leader_stats2` | character | JSON-encoded home-team season leader board occupying slot 2 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `home_team_season_leader_stats3` | character | JSON-encoded home-team season leader board occupying slot 3 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `home_team_player_season_stats0` | character | JSON-encoded home-team player season-stat block occupying slot 0 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `home_team_player_season_stats1` | character | JSON-encoded home-team player season-stat block occupying slot 1 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `home_team_player_season_stats2` | character | JSON-encoded home-team player season-stat block occupying slot 2 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `home_team_player_season_stats3` | character | JSON-encoded home-team player season-stat block occupying slot 3 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `home_team_player_season_stats4` | character | JSON-encoded home-team player season-stat block occupying slot 4 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `home_team_player_season_stats5` | character | JSON-encoded home-team player season-stat block occupying slot 5 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `home_team_player_season_stats6` | character | JSON-encoded home-team player season-stat block occupying slot 6 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `home_team_player_season_stats7` | character | JSON-encoded home-team player season-stat block occupying slot 7 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_game_stats_leaders-example}

```python
yahoo_game_stats_leaders()
```

_Last validated n/a._
