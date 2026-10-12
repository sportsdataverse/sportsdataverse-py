---
title: "LIIGA — Liiga API v2 (liiga.fi/api/v2, Finland) — Player"
sidebar_label: "Player"
sidebar_position: 2
description: "LIIGA — Liiga API v2 (liiga.fi/api/v2, Finland) — Player — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# LIIGA — Liiga API v2 (liiga.fi/api/v2, Finland) — Player

## liiga_players_info

Roster and bio of every player in a season range.

**Endpoint URL:** `GET https://liiga.fi/api/v2/players/info`

**Valid URL:** [https://liiga.fi/api/v2/players/info?fromSeason=2026&toSeason=2026&tournament=runkosarja](https://liiga.fi/api/v2/players/info?fromSeason=2026&toSeason=2026&tournament=runkosarja)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `fromSeason` | `season_from` |  |  | `Y` | First season (END year) of the range. |
| `toSeason` | `season_to` |  |  | `Y` | Last season (END year) of the range. |
| `tournament` | `tournament` |  |  | `Y` | Tournament: runkosarja (regular season), playoffs, playout, qualifications, valmistavat_ottelut (preseason) or chl (Champions Hockey League). Required by the API; defaults to runkosarja. |

### Returns {#liiga_players_info-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player id; the same number in lineups, rosters, stat tables and the profile (fihaId). |
| `team_id` | integer | Team id, the number of Liiga's `<num>:<slug>` team key (sent as that string, a bare integer or a digit string depending on the route). |
| `team_slug` | character | Team slug, the part of Liiga's `<num>:<slug>` team key after the colon (e.g. hpk). |
| `team_name` | character | Team display name. |
| `country_of_birth` | character | Country of birth, 3-letter code. |
| `place_of_birth` | character | Place of birth. |
| `date_of_birth` | character | Date of birth, YYYY-MM-DD. |
| `nationality` | character | Nationality, 3-letter code. |
| `first_name` | character | First name. |
| `last_name` | character | Last name. |
| `role` | character | Position: CENTER, LEFT_WING, RIGHT_WING, LEFT_DEFENSEMAN, RIGHT_DEFENSEMAN, SEVENTH_DEFENSEMAN, GOALIE. |
| `role_code` | character | Position code, Finnish: KH = centre, VL/OL = left/right wing, VP/OP = left/right defence, 7. P = seventh defenceman, MV = goalkeeper. |
| `handedness` | character | Shoots LEFT or RIGHT. |
| `height` | integer | Height, cm. |
| `weight` | integer | Weight, kg. |
| `rookie` | logical | Rookie. |
| `jersey` | integer | Jersey number. |
| `injured` | logical | Injured. |
| `suspended` | logical | Suspended. |
| `removed` | logical | Removed from the roster. |
| `captain` | logical | Captain. |
| `alternate_captain` | logical | Alternate captain. |
| `picture_url` | character | Photo URL. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#liiga_players_info-example}

```python
liiga_players_info(season_from=2026, season_to=2026)
```

_Last validated n/a._

## liiga_player

One player's bio, teams by season and active seasons.

**Endpoint URL:** `GET https://liiga.fi/api/v2/players/info/{player_id}`

**Valid URL:** [https://liiga.fi/api/v2/players/info/21870028](https://liiga.fi/api/v2/players/info/21870028)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Player id (player_id in liiga_players_info or a lineup; equals fihaId), e.g. 21870028. |

### Returns {#liiga_player-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `active_seasons` | character | Seasons (END year) the player appears in. |
| `team_seasons` | character | Map team id -> seasons (END year) with that team. |
| `team_list` | character | Map team name -> team as `<num>:<slug>`. |
| `birth_locality_country_code` | character | Birth place: country: code. |
| `birth_locality_country_name` | character | Birth place: country: rink name. |
| `birth_locality_name` | character | Birth place: rink name. |
| `date_of_birth` | character | Date of birth, YYYY-MM-DD. |
| `player_id` | integer | Player id; the same number in lineups, rosters, stat tables and the profile (fihaId). |
| `first_name` | character | First name. |
| `last_name` | character | Last name. |
| `handedness` | character | Shoots LEFT or RIGHT. |
| `height` | integer | Height, cm. |
| `is_suspended` | logical | Suspended. |
| `is_removed` | logical | Removed from the roster. |
| `nationality_code` | character | Nationality: code. |
| `nationality_name` | character | Nationality: rink name. |
| `weight` | integer | Weight, kg. |
| `teams` | character | Team directory: a map keyed by team as `<num>:<slug>` (e.g. `1368624751:k-espoo`), one row per current or past team. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#liiga_player-example}

```python
liiga_player(player_id=21870028)
```

_Last validated n/a._

## liiga_player_career

One player's career: season totals by tournament.

**Endpoint URL:** `GET https://liiga.fi/api/v2/players/info/{player_id}`

**Valid URL:** [https://liiga.fi/api/v2/players/info/21870028](https://liiga.fi/api/v2/players/info/21870028)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Player id (player_id in liiga_players_info or a lineup; equals fihaId), e.g. 21870028. |

### Returns {#liiga_player_career-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player id; the same number in lineups, rosters, stat tables and the profile (fihaId). |
| `season` | integer | Season END year (2026 = 2025-26). |
| `games` | integer | Games (player, all-time). |
| `played_games` | integer | Played games (player, that season and tournament). |
| `goals` | integer | Goals. |
| `assists` | integer | Assists (player, all-time). |
| `points` | integer | Points (player, all-time). |
| `penalty_minutes` | integer | Penalty minutes. |
| `plus` | integer | On ice for goals for (the + of plus/minus). |
| `minus` | integer | On ice for goals against (the - of plus/minus). |
| `plus_minus` | integer | plus minus minus. |
| `power_play_goals` | integer | Power play goals (team, this period). |
| `short_handed_goals` | integer | Short handed goals. |
| `winning_goals` | integer | Winning goals (player, that season and tournament). |
| `shots` | integer | Shots (player, all-time). |
| `shots_into_goal` | integer | Shots into goal (player, that season and tournament). |
| `shot_percentage` | numeric | Shot percentage (player, all-time). Percent, 0-100. |
| `shot_into_goal_percentage` | numeric | Shot into goal percentage (player, that season and tournament). Percent, 0-100, sent as a STRING. |
| `tournament` | character | Tournament. |
| `league_name` | character | League name. |
| `team_name` | character | Team display name. |
| `end_time` | character | Period end, seconds from game start. |
| `team_short_name` | character | Team abbreviation. |
| `time_on_ice` | integer | Time on ice (player, that season and tournament). Seconds. |
| `save_percentage` | numeric | Save percentage (team, over the season range). Percent, 0-100. |
| `goals_against_average` | numeric | Goals against average. |
| `win_percentage` | numeric | Share of games won, a 0-1 FRACTION sent as a STRING (e.g. "0.583"), season to date. |
| `shut_out` | character | Shut out. |
| `faceoffs_won` | integer | Faceoffs won. |
| `faceoffs_percentage` | numeric | Faceoffs percentage (player, that season and tournament). Percent, 0-100, sent as a STRING. |
| `wins` | integer | Wins (team, season to date in that table's tournament). |
| `losses` | integer | Losses (team, season to date in that table's tournament). |
| `ties` | integer | Ties (team, season to date in that table's tournament). |
| `gk_wins` | integer | Goalkeeper wins (player, that season and tournament). |
| `gk_losses` | integer | Goalkeeper losses (player, that season and tournament). |
| `gk_ties` | integer | Goalkeeper ties (player, that season and tournament). |
| `saves` | integer | Saves (goalkeeper, this period). |
| `goals_against` | integer | Goals against (team, season to date in that table's tournament). |
| `faceoff_total` | integer | Faceoff total (team, over the season range). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#liiga_player_career-example}

```python
liiga_player_career(player_id=21870028)
```

_Last validated n/a._

## liiga_player_game_log

One player's game log for a season, every tournament.

**Endpoint URL:** `GET https://liiga.fi/api/v2/players/info/{player_id}/games/{season}`

**Valid URL:** [https://liiga.fi/api/v2/players/info/21870028/games/2026](https://liiga.fi/api/v2/players/info/21870028/games/2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Player id (player_id in liiga_players_info or a lineup; equals fihaId), e.g. 21870028. |
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26). |

### Returns {#liiga_player_game_log-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player id; the same number in lineups, rosters, stat tables and the profile (fihaId). |
| `season` | integer | Season END year (2026 = 2025-26). |
| `tournament` | character | Tournament. |
| `game_str` | character | Matchup label, e.g. `HPK - LUK`. |
| `game_id` | integer | Game id, unique only within a season: key a game as (season, game_id). |
| `date` | character | Game start, ISO 8601 UTC. |
| `assists` | integer | Assists (player, all-time). |
| `blocked_or_saved_shots` | integer | Blocked or saved shots (player, this game). |
| `faceoffs_total` | integer | Faceoffs total. |
| `faceoffs_won` | integer | Faceoffs won. |
| `faceoffs_percentage` | numeric | Faceoffs percentage (player, that season and tournament). Percent, 0-100, sent as a STRING. |
| `goals` | integer | Goals. |
| `minus` | integer | On ice for goals against (the - of plus/minus). |
| `total_points` | integer | Total points (player, this game). |
| `home_team_name` | character | Home team name. |
| `away_team_name` | character | Away team name. |
| `penalty_minutes` | integer | Penalty minutes. |
| `penaltykill_assists` | integer | Penaltykill assists (goalkeeper, this period). |
| `penaltykill_goals` | integer | Penaltykill goals (player, this game). |
| `penaltykill_points` | integer | Penaltykill points (player, this game). |
| `plus` | integer | On ice for goals for (the + of plus/minus). |
| `plus_minus` | integer | plus minus minus. |
| `powerplay_assists` | integer | Powerplay assists (goalkeeper, this period). |
| `powerplay_goals` | integer | Powerplay goals. |
| `powerplay_seconds` | integer | Powerplay seconds (player, this game). Seconds. |
| `time_on_ice` | integer | Time on ice (player, that season and tournament). Seconds. |
| `saves` | integer | Saves (goalkeeper, this period). |
| `save_percentage` | numeric | Save percentage (team, over the season range). Percent, 0-100. |
| `block_or_saved_shots_percentage` | numeric | Block or saved shots percentage (player, this game). Percent, 0-100, sent as a STRING. |
| `shots` | integer | Shots (player, all-time). |
| `shots_into_goal` | integer | Shots into goal (player, that season and tournament). |
| `shut_out` | integer | Shut out. |
| `shots_saved` | integer | Shots saved (team, over the season range). |
| `shots_saved_by_skater` | integer | Shots saved by skater (player, this game). |
| `shot_percentage` | numeric | Shot percentage (player, all-time). Percent, 0-100. |
| `shot_into_goal_percentage` | numeric | Shot into goal percentage (player, that season and tournament). Percent, 0-100, sent as a STRING. |
| `goals_against` | integer | Goals against (team, season to date in that table's tournament). |
| `goalkeeper` | logical | Goalkeeper. |
| `winning_goals` | integer | Winning goals (player, that season and tournament). |
| `won` | integer | Won (player, this game). |
| `tied` | integer | Tied (player, this game). |
| `lost` | integer | Lost (player, this game). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#liiga_player_game_log-example}

```python
liiga_player_game_log(player_id=21870028, season=2026)
```

_Last validated n/a._

## liiga_player_stats

Player stat table summed over a season range, by data_type (the whole league, unfiltered).

**Endpoint URL:** `GET https://liiga.fi/api/v2/players/stats/summed/{season_from}/{season_to}/{tournament}/true`

**Valid URL:** [https://liiga.fi/api/v2/players/stats/summed/2026/2026/runkosarja/true?dataType=basicStats](https://liiga.fi/api/v2/players/stats/summed/2026/2026/runkosarja/true?dataType=basicStats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season_from` | `season_from` |  | `Y` |  | First season (END year) of the range, e.g. 2026. |
| `season_to` | `season_to` |  | `Y` |  | Last season (END year) of the range, e.g. 2026. |
| `tournament` | `tournament` |  | `Y` |  | Tournament: runkosarja (regular season), playoffs, playout, qualifications, valmistavat_ottelut (preseason) or chl (Champions Hockey League). |
| `dataType` | `data_type` |  |  | `Y` | Stat table: basicStats, basicStatsGk, advancedStats, goalStats, shotStats, skatingStats, faceOffStats, penaltyStats, powerplayPenaltykillStats, passes, winningShotComp, winningShotCompGk, gameTime. |

### Returns {#liiga_player_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player id; the same number in lineups, rosters, stat tables and the profile (fihaId). |
| `team_id` | integer | Team id, the number of Liiga's `<num>:<slug>` team key (sent as that string, a bare integer or a digit string depending on the route). |
| `team_name` | character | Team display name. |
| `team_short_name` | character | Team abbreviation. |
| `role` | character | Position: CENTER, LEFT_WING, RIGHT_WING, LEFT_DEFENSEMAN, RIGHT_DEFENSEMAN, SEVENTH_DEFENSEMAN, GOALIE. |
| `first_name` | character | First name. |
| `last_name` | character | Last name. |
| `nationality` | character | Nationality, 3-letter code. |
| `tournament` | character | Tournament. |
| `picture_url` | character | Photo URL. |
| `previous_teams_for_tournament` | character | Earlier teams of a player traded during the tournament. |
| `injured` | logical | Injured. |
| `jersey` | integer | Jersey number. |
| `last_season` | integer | Last season (player, summed over the season range). |
| `goalkeeper` | logical | Goalkeeper. |
| `games` | integer | Games (player, all-time). |
| `played_games` | integer | Played games (player, that season and tournament). |
| `rookie` | logical | Rookie. |
| `suspended` | logical | Suspended. |
| `removed` | logical | Removed from the roster. |
| `time_on_ice` | integer | Time on ice (player, that season and tournament). Seconds. |
| `current` | logical | Player is on the team now. |
| `is_u20` | logical | Is u 2 #0. |
| `golden_helmets` | integer | Golden helmets (player, summed over the season range). |
| `alumni_points` | integer | Alumni points (player, summed over the season range). |
| `goals` | integer | Goals. |
| `assists` | integer | Assists (player, all-time). |
| `points` | integer | Points (player, all-time). |
| `plus` | integer | On ice for goals for (the + of plus/minus). |
| `minus` | integer | On ice for goals against (the - of plus/minus). |
| `plus_minus` | integer | plus minus minus. |
| `penalty_minutes` | integer | Penalty minutes. |
| `powerplay_goals` | integer | Powerplay goals. |
| `penaltykill_goals` | integer | Penaltykill goals (player, this game). |
| `winning_goals` | integer | Winning goals (player, that season and tournament). |
| `shots` | integer | Shots (player, all-time). |
| `shots_into_goal` | integer | Shots into goal (player, that season and tournament). |
| `expected_goals` | numeric | Expected goals (xG), computed by the provider with an undocumented model: the team's total for the game in /games and /games/{season}/{id}; the player's sum over the season range in /players/stats/summed. /shotmap has no per-shot xG. |
| `contest_won` | integer | Contest won (player, summed over the season range). |
| `contest_lost` | integer | Contest lost (player, summed over the season range). |
| `goals_to_empty_goal` | integer | Goals to empty goal (goalkeeper, this period). |
| `time_on_ice_avg` | numeric | Time on ice (player, summed over the season range). Seconds per game. |
| `contest_won_percentage` | numeric | Contest won percentage (player, summed over the season range). Percent, 0-100. |
| `xge` | numeric | Goals minus expectedGoals over the season range (4 goals on 5.0956 xG = -1.0956). |
| `shot_percentage` | numeric | Shot percentage (player, all-time). Percent, 0-100. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#liiga_player_stats-example}

```python
liiga_player_stats(season_from=2026, season_to=2026, tournament='runkosarja')
```

_Last validated n/a._
