---
title: "NBA — NBA Stats API (stats.nba.com) — Other"
sidebar_label: "Other"
sidebar_position: 17
description: "NBA — NBA Stats API (stats.nba.com) — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NBA — NBA Stats API (stats.nba.com) — Other

## nba_stats_alltimeleadersgrids

GET /stats/alltimeleadersgrids

**Endpoint URL:** `GET https://stats.nba.com/stats/alltimeleadersgrids`

**Valid URL:** [https://stats.nba.com/stats/alltimeleadersgrids?LeagueID=00](https://stats.nba.com/stats/alltimeleadersgrids?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PerMode` | `per_mode_simple` |  |  | `Y` |  |
| `SeasonType` | `season_type` |  |  | `Y` | Season phase: 1=preseason, 2=regular season, 3=postseason. |
| `TopX` | `topx` |  |  | `Y` |  |

### Returns {#nba_stats_alltimeleadersgrids-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `tov` | numeric | Turnovers. |
| `tov_rank` | integer | All-time league rank of the player's career turnover total on the leaders grid. |
| `is_active_flag` | character | Flag indicating whether the player is currently active in the league. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_alltimeleadersgrids-example}

```python
nba_stats_alltimeleadersgrids(league_id='00')
```

_Last validated n/a._

## nba_stats_assistleaders

GET /stats/assistleaders

**Endpoint URL:** `GET https://stats.nba.com/stats/assistleaders`

**Valid URL:** [https://stats.nba.com/stats/assistleaders?LeagueID=00](https://stats.nba.com/stats/assistleaders?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PerMode` | `per_mode_simple` |  |  | `Y` |  |
| `PlayerOrTeam` | `player_or_team` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` |  |

### Returns {#nba_stats_assistleaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `rank` | integer | Rank. |
| `player_id` | integer | Unique player identifier. |
| `player` | character | Player name. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `jersey_num` | character | Jersey number worn by the player. |
| `player_position` | character | Position of the player accordinng to NGS |
| `ast` | numeric | Assists. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_assistleaders-example}

```python
nba_stats_assistleaders(league_id='00')
```

_Last validated n/a._

## nba_stats_assisttracker

GET /stats/assisttracker

**Endpoint URL:** `GET https://stats.nba.com/stats/assisttracker`

**Valid URL:** [https://stats.nba.com/stats/assisttracker?LeagueID=00](https://stats.nba.com/stats/assisttracker?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `College` | `college_nullable` |  |  | `Y` |  |
| `Conference` | `conference_nullable` |  |  | `Y` |  |
| `Country` | `country_nullable` |  |  | `Y` |  |
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `Division` | `division_simple_nullable` |  |  | `Y` |  |
| `DraftPick` | `draft_pick_nullable` |  |  | `Y` |  |
| `DraftYear` | `draft_year_nullable` |  |  | `Y` |  |
| `GameScope` | `game_scope_simple_nullable` |  |  | `Y` |  |
| `Height` | `height_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `Month` | `month_nullable` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id_nullable` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PerMode` | `per_mode_simple_nullable` |  |  | `Y` |  |
| `PlayerExperience` | `player_experience_nullable` |  |  | `Y` |  |
| `PlayerPosition` | `player_position_abbreviation_nullable` |  |  | `Y` |  |
| `Season` | `season_nullable` |  |  | `Y` |  |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star_nullable` |  |  | `Y` |  |
| `StarterBench` | `starter_bench_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |
| `Weight` | `weight_nullable` |  |  | `Y` |  |

### Returns {#nba_stats_assisttracker-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `assists` | numeric | Total assists. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_assisttracker-example}

```python
nba_stats_assisttracker(league_id='00')
```

_Last validated n/a._

## nba_stats_drafthistory

GET /stats/drafthistory

**Endpoint URL:** `GET https://stats.nba.com/stats/drafthistory`

**Valid URL:** [https://stats.nba.com/stats/drafthistory?LeagueID=00&Season=2024](https://stats.nba.com/stats/drafthistory?LeagueID=00&Season=2024)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `College` | `college_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `OverallPick` | `overall_pick_nullable` |  |  | `Y` |  |
| `RoundNum` | `round_num_nullable` |  |  | `Y` |  |
| `RoundPick` | `round_pick_nullable` |  |  | `Y` |  |
| `Season` | `season_year_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `TopX` | `topx_nullable` |  |  | `Y` |  |

### Returns {#nba_stats_drafthistory-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `person_id` | integer | Unique player identifier (V3 endpoints). |
| `player_name` | character | Player name. |
| `season` | character | Season year. |
| `round_number` | integer | Numeric round. |
| `round_pick` | integer | Round pick. |
| `overall_pick` | integer | Overall pick. |
| `draft_type` | character | NBA or WNBA Stats value for draft type in the drafthistory result set. |
| `team_id` | integer | Unique team identifier. |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `organization` | character | Organization. |
| `organization_type` | character | Organization type. |
| `player_profile_flag` | integer | Player profile flag. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_drafthistory-example}

```python
nba_stats_drafthistory(league_id='00', season_year_nullable='2024')
```

_Last validated n/a._

## nba_stats_fantasywidget

GET /stats/fantasywidget

**Endpoint URL:** `GET https://stats.nba.com/stats/fantasywidget`

**Valid URL:** [https://stats.nba.com/stats/fantasywidget?LeagueID=00](https://stats.nba.com/stats/fantasywidget?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `ActivePlayers` | `active_players` |  |  | `Y` |  |
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `Month` | `month_nullable` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PlayerID` | `player_id_nullable` |  |  | `Y` |  |
| `Position` | `position_nullable` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `TodaysOpponent` | `todays_opponent` |  |  | `Y` |  |
| `TodaysPlayers` | `todays_players` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns {#nba_stats_fantasywidget-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `player_position` | character | Position of the player accordinng to NGS |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `gp` | integer | Games played. |
| `min` | numeric | Minutes played. |
| `fan_duel_pts` | numeric | Fantasy points under FanDuel's scoring formula. |
| `nba_fantasy_pts` | numeric | Fantasy points under the NBA's fantasy scoring formula. |
| `pts` | numeric | Points scored. |
| `reb` | numeric | Rebounds per game. |
| `ast` | numeric | Assists. |
| `blk` | numeric | Blocks. |
| `stl` | numeric | Steals. |
| `tov` | numeric | Turnovers. |
| `fg3m` | numeric | Three-point field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_fantasywidget-example}

```python
nba_stats_fantasywidget(league_id='00')
```

_Last validated n/a._

## nba_stats_gamerotation

GET /stats/gamerotation

**Endpoint URL:** `GET https://stats.nba.com/stats/gamerotation`

**Valid URL:** [https://stats.nba.com/stats/gamerotation?LeagueID=00](https://stats.nba.com/stats/gamerotation?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |

### Returns {#nba_stats_gamerotation-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `team_id` | integer | Unique team identifier. |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `person_id` | integer | Unique player identifier (V3 endpoints). |
| `player_first` | character | NBA or WNBA Stats value for player first in the gamerotation result set. |
| `player_last` | character | NBA or WNBA Stats value for player last in the gamerotation result set. |
| `in_time_real` | numeric | Real-time clock value when the player entered the game rotation stint. |
| `out_time_real` | numeric | Real-time clock value when the player exited the game rotation stint. |
| `player_pts` | integer | Scoring or score-margin metric for player points in the requested NBA or WNBA Stats split. |
| `pt_diff` | numeric | NBA or WNBA Stats value for pt diff in the gamerotation result set. |
| `usg_pct` | numeric | Percentage or rate for usage percentage in the requested NBA or WNBA Stats split. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_gamerotation-example}

```python
nba_stats_gamerotation(league_id='00')
```

_Last validated n/a._

## nba_stats_homepageleaders

GET /stats/homepageleaders

**Endpoint URL:** `GET https://stats.nba.com/stats/homepageleaders`

**Valid URL:** [https://stats.nba.com/stats/homepageleaders?LeagueID=00](https://stats.nba.com/stats/homepageleaders?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameScope` | `game_scope_detailed` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PlayerOrTeam` | `player_or_team` |  |  | `Y` |  |
| `PlayerScope` | `player_scope` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` |  |
| `StatCategory` | `stat_category` |  |  | `Y` |  |

### Returns {#nba_stats_homepageleaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `rank` | character | Rank. |
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `pts` | character | Points scored. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `efg_pct` | numeric | Effective field goal percentage, as a decimal. |
| `ts_pct` | numeric | True shooting percentage (0-1). |
| `pts_per48` | character | Points scored per 48 minutes played. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_homepageleaders-example}

```python
nba_stats_homepageleaders(league_id='00')
```

_Last validated n/a._

## nba_stats_homepagev2

GET /stats/homepagev2

**Endpoint URL:** `GET https://stats.nba.com/stats/homepagev2`

**Valid URL:** [https://stats.nba.com/stats/homepagev2?LeagueID=00](https://stats.nba.com/stats/homepagev2?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameScope` | `game_scope_detailed` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PlayerOrTeam` | `player_or_team` |  |  | `Y` |  |
| `PlayerScope` | `player_scope` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` |  |
| `StatType` | `stat_type` |  |  | `Y` |  |

### Returns {#nba_stats_homepagev2-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `rank` | integer | Rank. |
| `player_id` | integer | Unique player identifier. |
| `player` | character | Player name. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `jersey_num` | character | Jersey number worn by the player. |
| `player_position` | character | Position of the player accordinng to NGS |
| `blk` | numeric | Blocks. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_homepagev2-example}

```python
nba_stats_homepagev2(league_id='00')
```

_Last validated n/a._

## nba_stats_hustlestatsboxscore

GET /stats/hustlestatsboxscore

**Endpoint URL:** `GET https://stats.nba.com/stats/hustlestatsboxscore`

**Valid URL:** [https://stats.nba.com/stats/hustlestatsboxscore](https://stats.nba.com/stats/hustlestatsboxscore)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |

### Returns {#nba_stats_hustlestatsboxscore-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `team_id` | character | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `player_id` | character | Unique player identifier. |
| `player_name` | character | Player name. |
| `start_position` | character | Position the player started the game at (F, C, or G); empty for reserves. |
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `minutes` | character | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `pts` | integer | Points scored. |
| `contested_shots` | numeric | Defensively contested shots. |
| `contested_shots_2pt` | numeric | Opponent two-point attempts contested. |
| `contested_shots_3pt` | numeric | Opponent three-point attempts contested. |
| `deflections` | numeric | Defensive deflections. |
| `charges_drawn` | numeric | Charges drawn. |
| `screen_assists` | numeric | Screen assists (resulting in a basket). |
| `screen_ast_pts` | numeric | Points teammates scored directly off the row's screen assists. |
| `off_loose_balls_recovered` | numeric | Loose balls recovered while on offense. |
| `def_loose_balls_recovered` | numeric | Loose balls recovered while on defense. |
| `loose_balls_recovered` | numeric | Total loose balls recovered. |
| `off_boxouts` | numeric | Box-outs recorded on the offensive glass. |
| `def_boxouts` | numeric | Box-outs recorded on the defensive glass. |
| `box_out_player_team_rebs` | numeric | Team rebounds secured following the row's box-outs. |
| `box_out_player_rebs` | numeric | Rebounds the player secured directly off their own box-outs. |
| `box_outs` | numeric | Box-outs executed. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_hustlestatsboxscore-example}

```python
nba_stats_hustlestatsboxscore()
```

_Last validated n/a._

## nba_stats_infographicfanduelplayer

GET /stats/infographicfanduelplayer

**Endpoint URL:** `GET https://stats.nba.com/stats/infographicfanduelplayer`

**Valid URL:** [https://stats.nba.com/stats/infographicfanduelplayer](https://stats.nba.com/stats/infographicfanduelplayer)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |

### Returns {#nba_stats_infographicfanduelplayer-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `jersey_num` | character | Jersey number worn by the player. |
| `player_position` | character | Position of the player accordinng to NGS |
| `location` | character | Location. |
| `fan_duel_pts` | numeric | Scoring or score-margin metric for fan duel points in the requested NBA or WNBA Stats split. |
| `nba_fantasy_pts` | numeric | Nba fantasy points for the requested NBA or WNBA Stats split. |
| `usg_pct` | numeric | Percentage or rate for usage percentage in the requested NBA or WNBA Stats split. |
| `min` | numeric | Minutes played. |
| `fgm` | integer | Field goals made. |
| `fga` | integer | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | integer | Three-point field goals made. |
| `fg3a` | integer | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | integer | Free throws made. |
| `fta` | integer | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | integer | Offensive rebounds. |
| `dreb` | integer | Defensive rebounds. |
| `reb` | integer | Rebounds per game. |
| `ast` | integer | Assists. |
| `tov` | integer | Turnovers. |
| `stl` | integer | Steals. |
| `blk` | integer | Blocks. |
| `blka` | integer | Blocked field-goal attempts against for the requested NBA or WNBA Stats split. |
| `pf` | integer | Personal fouls. |
| `pfd` | integer | Personal fouls drawn for the requested NBA or WNBA Stats split. |
| `pts` | integer | Points scored. |
| `plus_minus` | integer | Plus/minus point differential while on court. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_infographicfanduelplayer-example}

```python
nba_stats_infographicfanduelplayer()
```

_Last validated n/a._

## nba_stats_leaderstiles

GET /stats/leaderstiles

**Endpoint URL:** `GET https://stats.nba.com/stats/leaderstiles`

**Valid URL:** [https://stats.nba.com/stats/leaderstiles?LeagueID=00](https://stats.nba.com/stats/leaderstiles?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameScope` | `game_scope_detailed` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PlayerOrTeam` | `player_or_team` |  |  | `Y` |  |
| `PlayerScope` | `player_scope` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` |  |
| `Stat` | `stat` |  |  | `Y` |  |

### Returns {#nba_stats_leaderstiles-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `rank` | integer | Rank. |
| `player_id` | integer | Unique player identifier. |
| `player` | character | Player name. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `pts` | numeric | Points scored. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_leaderstiles-example}

```python
nba_stats_leaderstiles(league_id='00')
```

_Last validated n/a._

## nba_stats_matchupsrollup

GET /stats/matchupsrollup

**Endpoint URL:** `GET https://stats.nba.com/stats/matchupsrollup`

**Valid URL:** [https://stats.nba.com/stats/matchupsrollup?LeagueID=00](https://stats.nba.com/stats/matchupsrollup?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DefPlayerID` | `def_player_id_nullable` |  |  | `Y` |  |
| `DefTeamID` | `def_team_id_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `OffPlayerID` | `off_player_id_nullable` |  |  | `Y` |  |
| `OffTeamID` | `off_team_id_nullable` |  |  | `Y` |  |
| `PerMode` | `per_mode_simple` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` |  |

### Returns {#nba_stats_matchupsrollup-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `season_id` | character | Unique season identifier. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `percent_of_time` | numeric | Time value for percent of time in the NBA or WNBA Stats result set. |
| `def_player_id` | integer | Stats API identifier for defensive player identifier associated with this NBA or WNBA Stats row. |
| `def_player_name` | character | Display name for defensive player name associated with this NBA or WNBA Stats row. |
| `gp` | integer | Games played. |
| `matchup_min` | numeric | NBA or WNBA Stats value for matchup minutes in the matchupsrollup result set. |
| `partial_poss` | numeric | Estimated partial possessions credited to the stint or rotation interval. |
| `player_pts` | numeric | Scoring or score-margin metric for player points in the requested NBA or WNBA Stats split. |
| `team_pts` | numeric | Scoring or score-margin metric for team points in the requested NBA or WNBA Stats split. |
| `matchup_ast` | numeric | NBA or WNBA Stats value for matchup assists in the matchupsrollup result set. |
| `matchup_tov` | numeric | Turnover or loose-ball metric for matchup turnovers in the requested NBA or WNBA Stats split. |
| `matchup_blk` | numeric | NBA or WNBA Stats value for matchup blocks in the matchupsrollup result set. |
| `matchup_fgm` | numeric | Shooting metric for matchup fgm in the requested NBA or WNBA Stats split. |
| `matchup_fga` | numeric | Shooting metric for matchup fga in the requested NBA or WNBA Stats split. |
| `matchup_fg_pct` | numeric | Percentage or rate for matchup field goals percentage in the requested NBA or WNBA Stats split. |
| `matchup_fg3m` | numeric | Shooting metric for matchup fg3m in the requested NBA or WNBA Stats split. |
| `matchup_fg3a` | numeric | Shooting metric for matchup fg3a in the requested NBA or WNBA Stats split. |
| `matchup_fg3_pct` | numeric | Percentage or rate for matchup three-point field goals percentage in the requested NBA or WNBA Stats split. |
| `matchup_ftm` | numeric | NBA or WNBA Stats value for matchup ftm in the matchupsrollup result set. |
| `matchup_fta` | numeric | NBA or WNBA Stats value for matchup fta in the matchupsrollup result set. |
| `sfl` | numeric | NBA or WNBA Stats value for sfl in the matchupsrollup result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_matchupsrollup-example}

```python
nba_stats_matchupsrollup(league_id='00')
```

_Last validated n/a._

## nba_stats_playbyplayv3

GET /stats/playbyplayv3

**Endpoint URL:** `GET https://stats.nba.com/stats/playbyplayv3`

**Valid URL:** [https://stats.nba.com/stats/playbyplayv3](https://stats.nba.com/stats/playbyplayv3)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |

### Returns {#nba_stats_playbyplayv3-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `actionid` | integer | NBA or WNBA Stats action identifier for the play event. |
| `actionnumber` | integer | Sequential action number for the play within the game feed. |
| `actiontype` | character | Normalized play action type reported by NBA or WNBA Stats. |
| `clock` | character | Game clock value. |
| `description` | character | Long-form description text. |
| `gameid` | character | Unique NBA or WNBA Stats game identifier for the play event. |
| `isfieldgoal` | integer | Flag indicating whether the play event is a field-goal attempt. |
| `location` | character | Location. |
| `period` | integer | Period of the game (1-4 quarters; 5+ for OT). |
| `personid` | integer | NBA or WNBA Stats player identifier associated with the play, when present. |
| `playername` | character | Full display name for the player associated with the play event. |
| `playernamei` | character | Abbreviated player display name used by the play feed. |
| `pointstotal` | integer | Running points total credited to the player after the play, when reported. |
| `scoreaway` | character | Away team's score after the play, when reported by the feed. |
| `scorehome` | character | Home team's score after the play, when reported by the feed. |
| `shotdistance` | integer | Shot distance in feet for shot attempts, when available. |
| `shotresult` | character | Result of the shot attempt, such as made or missed. |
| `shotvalue` | integer | Point value of the shot attempt, usually two or three points. |
| `subtype` | character | Secondary play subtype reported by NBA or WNBA Stats. |
| `teamid` | integer | Teamid. |
| `teamtricode` | character | Three-letter code for the team associated with the play event. |
| `videoavailable` | integer | Flag indicating whether video is available for the play or game row. |
| `xlegacy` | integer | Legacy NBA Stats x-coordinate for shot-location play events. |
| `ylegacy` | integer | Legacy NBA Stats y-coordinate for shot-location play events. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_playbyplayv3-example}

```python
nba_stats_playbyplayv3()
```

_Last validated n/a._

## nba_stats_playoffpicture

GET /stats/playoffpicture

**Endpoint URL:** `GET https://stats.nba.com/stats/playoffpicture`

**Valid URL:** [https://stats.nba.com/stats/playoffpicture?LeagueID=00](https://stats.nba.com/stats/playoffpicture?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `SeasonID` | `season_id` |  |  | `Y` |  |

### Returns {#nba_stats_playoffpicture-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `conference` | character | Conference name. |
| `rank` | integer | Rank. |
| `team` | character | Team-side label or team identifier. |
| `team_slug` | character | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `team_id` | integer | Unique team identifier. |
| `wins` | integer | Total wins. |
| `losses` | integer | Total losses. |
| `pct` | numeric | Win percentage. |
| `div` | character | Abbreviation of the team's division. |
| `conf` | character | character. |
| `home` | character | Home. |
| `away` | character | Away record. |
| `gb` | numeric | Games behind the conference leader. |
| `gr_over_500` | integer | Remaining games against teams with winning (over .500) records. |
| `gr_over_500_home` | integer | Remaining home games against teams with winning records. |
| `gr_over_500_away` | integer | Remaining road games against teams with winning records. |
| `gr_under_500` | integer | Remaining games against teams with losing (under .500) records. |
| `gr_under_500_home` | integer | Remaining home games against teams with losing records. |
| `gr_under_500_away` | integer | Remaining road games against teams with losing records. |
| `ranking_criteria` | integer | Code for the ranking or tiebreak criteria applied to the team in the playoff picture. |
| `clinched_playoffs` | integer | Flag (1/0) for whether the team has clinched a playoff berth. |
| `clinched_conference` | integer | Flag (1/0) for whether the team has clinched the conference title. |
| `clinched_division` | integer | Flag (1/0) for whether the team has clinched its division. |
| `eliminated_playoffs` | integer | Flag (1/0) for whether the team has been eliminated from playoff contention. |
| `sosa_remaining` | character | Strength of schedule of the team's remaining opponents (combined opponent winning percentage). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_playoffpicture-example}

```python
nba_stats_playoffpicture(league_id='00')
```

_Last validated n/a._

## nba_stats_shotchartdetail

GET /stats/shotchartdetail

**Endpoint URL:** `GET https://stats.nba.com/stats/shotchartdetail`

**Valid URL:** [https://stats.nba.com/stats/shotchartdetail?LeagueID=00](https://stats.nba.com/stats/shotchartdetail?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `AheadBehind` | `ahead_behind_nullable` |  |  | `Y` |  |
| `ClutchTime` | `clutch_time_nullable` |  |  | `Y` |  |
| `ContextFilter` | `context_filter_nullable` |  |  | `Y` |  |
| `ContextMeasure` | `context_measure_simple` |  |  | `Y` |  |
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `EndPeriod` | `end_period_nullable` |  |  | `Y` |  |
| `EndRange` | `end_range_nullable` |  |  | `Y` |  |
| `GameID` | `game_id_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |
| `PlayerPosition` | `player_position_nullable` |  |  | `Y` |  |
| `PointDiff` | `point_diff_nullable` |  |  | `Y` |  |
| `Position` | `position_nullable` |  |  | `Y` |  |
| `RangeType` | `range_type_nullable` |  |  | `Y` |  |
| `RookieYear` | `rookie_year_nullable` |  |  | `Y` |  |
| `Season` | `season_nullable` |  |  | `Y` |  |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `StartPeriod` | `start_period_nullable` |  |  | `Y` |  |
| `StartRange` | `start_range_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns {#nba_stats_shotchartdetail-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `grid_type` | character | Shot chart grid type label returned by the stats API (e.g. "Shot Chart Detail"). |
| `game_id` | character | Unique game identifier. |
| `game_event_id` | character | Unique identifier for game event. |
| `player_id` | character | Unique player identifier. |
| `player_name` | character | Player name. |
| `team_id` | character | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `period` | character | Period of the game (1-4 quarters; 5+ for OT). |
| `minutes_remaining` | character | Minutes remaining. |
| `seconds_remaining` | character | Seconds remaining in the period. |
| `event_type` | character | Event / play type code (V2 PBP). |
| `action_type` | character | Action type label (e.g. 'Made Shot', 'Substitution'). |
| `shot_type` | character | Shot type label (e.g. 'Jump Shot', 'Layup'). |
| `shot_zone_basic` | character | Shot zone (e.g. 'Restricted Area', 'Mid-Range', 'Above the Break 3'). |
| `shot_zone_area` | character | Shot zone area ('Left Side', 'Right Side', 'Center'). |
| `shot_zone_range` | character | Shot zone range ('Less Than 8 ft.', '8-16 ft.', '16-24 ft.', etc.). |
| `shot_distance` | character | Shot distance from the basket, in feet. |
| `loc_x` | character | X coordinate on the court (units of inches; 0 = basket center). |
| `loc_y` | character | Y coordinate on the court (units of inches; baseline at 0). |
| `shot_attempted_flag` | character | 1 if a shot was attempted on this event. |
| `shot_made_flag` | character | 1 if the shot was made; 0 if missed. |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `htm` | character | Home team abbreviation for the game. |
| `vtm` | character | Visiting team abbreviation for the game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_shotchartdetail-example}

```python
nba_stats_shotchartdetail(league_id='00')
```

_Last validated n/a._

## nba_stats_shotchartleaguewide

GET /stats/shotchartleaguewide

**Endpoint URL:** `GET https://stats.nba.com/stats/shotchartleaguewide`

**Valid URL:** [https://stats.nba.com/stats/shotchartleaguewide?LeagueID=00](https://stats.nba.com/stats/shotchartleaguewide?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#nba_stats_shotchartleaguewide-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `grid_type` | character | NBA or WNBA Stats value for grid type in the shotchartleaguewide result set. |
| `shot_zone_basic` | character | Shot zone (e.g. 'Restricted Area', 'Mid-Range', 'Above the Break 3'). |
| `shot_zone_area` | character | Shot zone area ('Left Side', 'Right Side', 'Center'). |
| `shot_zone_range` | character | Shot zone range ('Less Than 8 ft.', '8-16 ft.', '16-24 ft.', etc.). |
| `fga` | integer | Field goal attempts. |
| `fgm` | integer | Field goals made. |
| `fg_pct` | numeric | Field goal percentage (0-1). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_shotchartleaguewide-example}

```python
nba_stats_shotchartleaguewide(league_id='00')
```

_Last validated n/a._

## nba_stats_shotchartlineupdetail

GET /stats/shotchartlineupdetail

**Endpoint URL:** `GET https://stats.nba.com/stats/shotchartlineupdetail`

**Valid URL:** [https://stats.nba.com/stats/shotchartlineupdetail?LeagueID=00](https://stats.nba.com/stats/shotchartlineupdetail?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `ContextFilter` | `context_filter_nullable` |  |  | `Y` |  |
| `ContextMeasure` | `context_measure_detailed` |  |  | `Y` |  |
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `GROUP_ID` | `group_id` |  |  | `Y` |  |
| `GameID` | `game_id_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `Month` | `month_nullable` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id_nullable` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns {#nba_stats_shotchartlineupdetail-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `grid_type` | character | Shot chart grid type label returned by the stats API (e.g. "Shot Chart Detail"). |
| `game_id` | character | Unique game identifier. |
| `game_event_id` | character | Unique identifier for game event. |
| `group_id` | character | ESPN group id. |
| `group_name` | character | Group name (conference / division). |
| `player_id` | character | Unique player identifier. |
| `player_name` | character | Player name. |
| `team_id` | character | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `period` | character | Period of the game (1-4 quarters; 5+ for OT). |
| `minutes_remaining` | character | Minutes remaining. |
| `seconds_remaining` | character | Seconds remaining in the period. |
| `event_type` | character | Event / play type code (V2 PBP). |
| `action_type` | character | Action type label (e.g. 'Made Shot', 'Substitution'). |
| `shot_type` | character | Shot type label (e.g. 'Jump Shot', 'Layup'). |
| `shot_zone_basic` | character | Shot zone (e.g. 'Restricted Area', 'Mid-Range', 'Above the Break 3'). |
| `shot_zone_area` | character | Shot zone area ('Left Side', 'Right Side', 'Center'). |
| `shot_zone_range` | character | Shot zone range ('Less Than 8 ft.', '8-16 ft.', '16-24 ft.', etc.). |
| `shot_distance` | character | Shot distance from the basket, in feet. |
| `loc_x` | character | X coordinate on the court (units of inches; 0 = basket center). |
| `loc_y` | character | Y coordinate on the court (units of inches; baseline at 0). |
| `shot_attempted_flag` | character | 1 if a shot was attempted on this event. |
| `shot_made_flag` | character | 1 if the shot was made; 0 if missed. |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `htm` | character | Home team abbreviation for the game. |
| `vtm` | character | Visiting team abbreviation for the game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_shotchartlineupdetail-example}

```python
nba_stats_shotchartlineupdetail(league_id='00')
```

_Last validated n/a._

## nba_stats_synergyplaytypes

GET /stats/synergyplaytypes

**Endpoint URL:** `GET https://stats.nba.com/stats/synergyplaytypes`

**Valid URL:** [https://stats.nba.com/stats/synergyplaytypes?LeagueID=00](https://stats.nba.com/stats/synergyplaytypes?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PerMode` | `per_mode_simple` |  |  | `Y` |  |
| `PlayType` | `play_type_nullable` |  |  | `Y` |  |
| `PlayerOrTeam` | `player_or_team_abbreviation` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `SeasonYear` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `TypeGrouping` | `type_grouping_nullable` |  |  | `Y` |  |

### Returns {#nba_stats_synergyplaytypes-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `season_id` | character | Unique season identifier. |
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `play_type` | character | Play type description. |
| `type_grouping` | character | Whether the play-type row measures the offensive or defensive side. |
| `percentile` | numeric | League percentile of the row's points per possession for the play type, as a decimal. |
| `gp` | integer | Games played. |
| `poss_pct` | numeric | Poss percentage (0-1 decimal). |
| `ppp` | numeric | Points scored per possession on the play type (Synergy play-type tracking). |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `ft_poss_pct` | numeric | Share of play-type possessions ending in free throws, as a decimal. |
| `tov_poss_pct` | numeric | Share of play-type possessions ending in a turnover, as a decimal. |
| `sf_poss_pct` | numeric | Share of play-type possessions on which a shooting foul was drawn, as a decimal. |
| `plusone_poss_pct` | numeric | Share of play-type possessions producing an and-one, as a decimal. |
| `score_poss_pct` | numeric | Share of play-type possessions on which points were scored, as a decimal. |
| `efg_pct` | numeric | Effective field goal percentage on the play type, as a decimal. |
| `poss` | numeric | Poss. |
| `pts` | numeric | Points scored. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fgmx` | numeric | Field goals missed on the play type. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_synergyplaytypes-example}

```python
nba_stats_synergyplaytypes(league_id='00')
```

_Last validated n/a._

## nba_stats_videodetailsasset

GET /stats/videodetailsasset

**Endpoint URL:** `GET https://stats.nba.com/stats/videodetailsasset`

**Valid URL:** [https://stats.nba.com/stats/videodetailsasset?LeagueID=00](https://stats.nba.com/stats/videodetailsasset?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `ContextMeasure` | `context_measure_detailed` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `StartRange` | `start_range_nullable` |  |  | `Y` |  |
| `StartPeriod` | `start_period_nullable` |  |  | `Y` |  |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `RookieYear` | `rookie_year_nullable` |  |  | `Y` |  |
| `RangeType` | `range_type_nullable` |  |  | `Y` |  |
| `Position` | `position_nullable` |  |  | `Y` |  |
| `PointDiff` | `point_diff_nullable` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `GameID` | `game_id_nullable` |  |  | `Y` |  |
| `EndRange` | `end_range_nullable` |  |  | `Y` |  |
| `EndPeriod` | `end_period_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `ContextFilter` | `context_filter_nullable` |  |  | `Y` |  |
| `ClutchTime` | `clutch_time_nullable` |  |  | `Y` |  |
| `AheadBehind` | `ahead_behind_nullable` |  |  | `Y` |  |

### Returns {#nba_stats_videodetailsasset-returns}

**`return_parsed=True`** (default) — A dict of two DataFrames keyed `videoUrls` (clip URLs, durations, thumbnails) and `playlist` (per-event game metadata) (representative columns below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
| col_name | type | description |
|---|---|---|
| `uuid` | character | Uuid. |
| `sdur` | integer | Sdur. |
| `surl` | character | Surl. |
| `sth` | character | Sth. |
| `mdur` | integer | Mdur. |
| `murl` | character | Murl. |
| `mth` | character | Mth. |
| `ldur` | integer | Ldur. |
| `lurl` | character | Lurl. |
| `lth` | character | Lth. |
| `vtt` | character | Vtt. |
| `scc` | character | Scc. |
| `srt` | character | Srt. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_videodetailsasset-example}

```python
nba_stats_videodetailsasset(league_id='00')
```

_Last validated n/a._

## nba_stats_videoevents

GET /stats/videoevents

**Endpoint URL:** `GET https://stats.nba.com/stats/videoevents`

**Valid URL:** [https://stats.nba.com/stats/videoevents](https://stats.nba.com/stats/videoevents)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameEventID` | `game_event_id` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |

### Returns {#nba_stats_videoevents-returns}

**`return_parsed=True`** (default) — A dict of two DataFrames keyed `videoUrls` (clip URLs, durations, thumbnails) and `playlist` (per-event game metadata) (representative columns below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
| col_name | type | description |
|---|---|---|
| `uuid` | character | Uuid. |
| `dur` | character | Dur. |
| `stt` | character | Stt. |
| `stp` | character | Stp. |
| `sth` | character | Sth. |
| `stw` | character | Stw. |
| `mtt` | character | Mtt. |
| `mtp` | character | Mtp. |
| `mth` | character | Mth. |
| `mtw` | character | Mtw. |
| `ltt` | character | Ltt. |
| `ltp` | character | Ltp. |
| `lth` | character | Lth. |
| `ltw` | character | Ltw. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_videoevents-example}

```python
nba_stats_videoevents()
```

_Last validated n/a._

## nba_stats_videoeventsasset

GET /stats/videoeventsasset

**Endpoint URL:** `GET https://stats.nba.com/stats/videoeventsasset`

**Valid URL:** [https://stats.nba.com/stats/videoeventsasset](https://stats.nba.com/stats/videoeventsasset)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameEventID` | `game_event_id` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |

### Returns {#nba_stats_videoeventsasset-returns}

**`return_parsed=True`** (default) — A dict of two DataFrames keyed `videoUrls` (clip URLs, durations, thumbnails) and `playlist` (per-event game metadata) (representative columns below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
| col_name | type | description |
|---|---|---|
| `uuid` | character | Uuid. |
| `sdur` | integer | Sdur. |
| `surl` | character | Surl. |
| `sth` | character | Sth. |
| `mdur` | integer | Mdur. |
| `murl` | character | Murl. |
| `mth` | character | Mth. |
| `ldur` | integer | Ldur. |
| `lurl` | character | Lurl. |
| `lth` | character | Lth. |
| `vtt` | character | Vtt. |
| `scc` | character | Scc. |
| `srt` | character | Srt. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_videoeventsasset-example}

```python
nba_stats_videoeventsasset()
```

_Last validated n/a._

## nba_stats_videostatus

GET /stats/videostatus

**Endpoint URL:** `GET https://stats.nba.com/stats/videostatus`

**Valid URL:** [https://stats.nba.com/stats/videostatus?LeagueID=00](https://stats.nba.com/stats/videostatus?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameDate` | `game_date` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |

### Returns {#nba_stats_videostatus-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_id` | integer | Unique game identifier. |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `visitor_team_id` | integer | Unique identifier for visitor team. |
| `visitor_team_city` | character | City name of the visiting team. |
| `visitor_team_name` | character | Nickname of the visiting team. |
| `visitor_team_abbreviation` | character | Abbreviation of the visiting team. |
| `home_team_id` | integer | Unique identifier for the home team. |
| `home_team_city` | character | Home team city / location. |
| `home_team_name` | character | Home team name. |
| `home_team_abbreviation` | character | Home team abbreviation; `team_detail = TRUE` only. |
| `game_status` | character | Game status label. |
| `game_status_text` | character | Game status display text (e.g. 'Final', '4:32 - 4th'). |
| `is_available` | character | Flag indicating whether game video is available in the league's stats video system. |
| `pt_xyz_available` | character | Pt xyz available. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_videostatus-example}

```python
nba_stats_videostatus(league_id='00')
```

_Last validated n/a._
