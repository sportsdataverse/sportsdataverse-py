# Liiga — Liiga API v2 (liiga.fi/api/v2, Finland) — Player

> Liiga — Liiga API v2 (liiga.fi/api/v2, Finland) — Player — function reference in sdv-py, the SportsDataverse Python package.

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
| `player_id` | Int64 | Player id; the same number in lineups, rosters, stat tables and the profile (fihaId). |
| `team_id` | Int64 | Team id, the number of Liiga's `<num>:<slug>` team key (sent as that string, a bare integer or a digit string depending on the route). |
| `team_slug` | String | Team slug, the part of Liiga's `<num>:<slug>` team key after the colon (e.g. hpk). |
| `team_name` | String | Team's display name, e.g. HPK or Tappara. |
| `country_of_birth` | String | Country of birth, 3-letter code. |
| `place_of_birth` | String | Player's place of birth (town or city). |
| `date_of_birth` | String | Date of birth, YYYY-MM-DD. |
| `nationality` | String | Nationality, 3-letter code. |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `role` | String | Position: CENTER, LEFT_WING, RIGHT_WING, LEFT_DEFENSEMAN, RIGHT_DEFENSEMAN, SEVENTH_DEFENSEMAN, GOALIE. |
| `role_code` | String | Position code, Finnish: KH = centre, VL/OL = left/right wing, VP/OP = left/right defence, 7. P = seventh defenceman, MV = goalkeeper. |
| `handedness` | String | Shoots LEFT or RIGHT. |
| `height` | Int64 | Player's height in centimetres. |
| `weight` | Int64 | Player's weight in kilograms. |
| `rookie` | Boolean | True when Liiga flags the player as a rookie. |
| `jersey` | Int64 | Jersey (sweater) number the player wore. |
| `injured` | Boolean | True when the player is listed as injured. |
| `suspended` | Boolean | True when the player is listed as suspended. |
| `removed` | Boolean | Removed from the roster. |
| `captain` | Boolean | True when the player is the team captain (C). |
| `alternate_captain` | Boolean | True when the player is an alternate captain (A). |
| `picture_url` | String | URL of the player's photo. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#liiga_players_info-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Liiga has nothing for the request (an unknown game, a tournament not played that season, a shot map before 2016). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#liiga_players_info-example}

```python
liiga_players_info(season_from=2026, season_to=2026)
```

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
| `active_seasons` | String | Seasons (END year) the player appears in. |
| `team_seasons` | String | Map team id -> seasons (END year) with that team. |
| `team_list` | String | Map team name -> team as `<num>:<slug>`. |
| `birth_locality_country_code` | String | Player's birth country, 3-letter code (e.g. FIN). |
| `birth_locality_country_name` | String | Player's birth country name, in Finnish (e.g. Suomi). |
| `birth_locality_name` | String | Player's birth place (town or city). |
| `date_of_birth` | String | Date of birth, YYYY-MM-DD. |
| `player_id` | Int64 | Player id; the same number in lineups, rosters, stat tables and the profile (fihaId). |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `handedness` | String | Shoots LEFT or RIGHT. |
| `height` | Int64 | Player's height in centimetres. |
| `is_suspended` | Boolean | True when the player is listed as suspended. |
| `is_removed` | Boolean | Removed from the roster. |
| `nationality_code` | String | Player's nationality, 3-letter country code (e.g. FIN). |
| `nationality_name` | String | Player's nationality as a country name, in Finnish (e.g. Suomi). |
| `weight` | Int64 | Player's weight in kilograms. |
| `teams` | String | Map keyed by an id -> one of the player's team seasons (season, jersey, teamId, slug). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#liiga_player-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Liiga has nothing for the request (an unknown game, a tournament not played that season, a shot map before 2016). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#liiga_player-example}

```python
liiga_player(player_id=21870028)
```

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
| `player_id` | Int64 | Player id; the same number in lineups, rosters, stat tables and the profile (fihaId). |
| `season` | Int64 | Season END year (2026 = 2025-26). |
| `games` | Int64 | Games (player, that season and tournament). |
| `played_games` | Int64 | Played games (player, that season and tournament). |
| `goals` | Int64 | Goals (player, that season and tournament). |
| `assists` | Int64 | Assists (player, that season and tournament). |
| `points` | Int64 | Points (player, that season and tournament). |
| `penalty_minutes` | Int64 | Penalty minutes (player, that season and tournament). |
| `plus` | Int64 | On ice for goals for (the + of plus/minus). |
| `minus` | Int64 | On ice for goals against (the - of plus/minus). |
| `plus_minus` | Int64 | Plus/minus: on-ice goals for minus on-ice goals against. |
| `power_play_goals` | Int64 | Power play goals (player, that season and tournament). |
| `short_handed_goals` | Int64 | Short handed goals (player, that season and tournament). |
| `winning_goals` | Int64 | Winning goals (player, that season and tournament). |
| `shots` | Int64 | Shots (player, that season and tournament). |
| `shots_into_goal` | Int64 | Shots into goal (player, that season and tournament). |
| `shot_percentage` | Float64 | Shot percentage (player, that season and tournament). Percent, 0-100, sent as a STRING. |
| `shot_into_goal_percentage` | Float64 | Shot into goal percentage (player, that season and tournament). Percent, 0-100, sent as a STRING. |
| `tournament` | String | Tournament of the career row, uppercase: RUNKOSARJA (regular season), PLAYOFFS, PRACTICE (preseason), ... |
| `league_name` | String | Name of the league of that career row, e.g. Liiga. |
| `team_name` | String | Team's display name, e.g. HPK or Tappara. |
| `end_time` | String | ISO 8601 UTC timestamp, not game seconds: the end of that season-tournament for finished seasons (2026-03-17T23:59:59Z for the 2025-26 regular season) and the request time for the season in progress. Looks like the end of the stats window; meaning unverified. |
| `team_short_name` | String | Team abbreviation. |
| `time_on_ice` | Int64 | Time on ice (player, that season and tournament). Seconds. |
| `save_percentage` | Float64 | Save percentage (player, that season and tournament). Percent, 0-100, sent as a STRING. |
| `goals_against_average` | Float64 | Goals against per game (goalkeeper), that season and tournament; sent as a string. |
| `win_percentage` | Float64 | (wins + 0.5 x ties) / games x 10,000, NOT 0-100 (sent as "4,583.33", comma = thousands separator): divide by 10,000 for the standings 0-1 scale. Practice rows can show 0.00 despite wins; unreliable there. |
| `shut_out` | String | Shutouts (goalkeeper), that season and tournament; sent as a string (e.g. "0"). |
| `faceoffs_won` | Int64 | Faceoffs won (player, that season and tournament). |
| `faceoffs_percentage` | Float64 | Faceoffs percentage (player, that season and tournament). Percent, 0-100, sent as a STRING. |
| `wins` | Int64 | Wins (player, that season and tournament). |
| `losses` | Int64 | Losses (player, that season and tournament). |
| `ties` | Int64 | Ties (player, that season and tournament). |
| `gk_wins` | Int64 | Goalkeeper wins (player, that season and tournament). |
| `gk_losses` | Int64 | Goalkeeper losses (player, that season and tournament). |
| `gk_ties` | Int64 | Goalkeeper ties (player, that season and tournament). |
| `saves` | Int64 | Saves (player, that season and tournament). |
| `goals_against` | Int64 | Goals against (player, that season and tournament). |
| `faceoff_total` | Int64 | Faceoff total (player, that season and tournament). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#liiga_player_career-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Liiga has nothing for the request (an unknown game, a tournament not played that season, a shot map before 2016). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#liiga_player_career-example}

```python
liiga_player_career(player_id=21870028)
```

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
| `player_id` | Int64 | Player id; the same number in lineups, rosters, stat tables and the profile (fihaId). |
| `season` | Int64 | Season END year (2026 = 2025-26). |
| `tournament` | String | Tournament of the game (the body's list key): regular, playoffs, practice, qualifications, playout, chl or pitsiturnaus. |
| `game_str` | String | Matchup label, e.g. `HPK - LUK`. |
| `game_id` | Int64 | Game id, unique only within a season: key a game as (season, game_id). |
| `date` | String | Game start, ISO 8601 UTC. |
| `assists` | Int64 | Assists (player, this game). |
| `blocked_or_saved_shots` | Int64 | Blocked or saved shots (player, this game). |
| `faceoffs_total` | Int64 | Faceoffs total (player, this game). |
| `faceoffs_won` | Int64 | Faceoffs won (player, this game). |
| `faceoffs_percentage` | Float64 | Faceoffs percentage (player, this game). Percent, 0-100, sent as a STRING. |
| `goals` | Int64 | Goals (player, this game). |
| `minus` | Int64 | On ice for goals against (the - of plus/minus). |
| `total_points` | Int64 | Total points (player, this game). |
| `home_team_name` | String | Display name of the home team of the game, e.g. HPK. |
| `away_team_name` | String | Display name of the away team of the game, e.g. Lukko. |
| `penalty_minutes` | Int64 | Penalty minutes (player, this game). |
| `penaltykill_assists` | Int64 | Penaltykill assists (player, this game). |
| `penaltykill_goals` | Int64 | Penaltykill goals (player, this game). |
| `penaltykill_points` | Int64 | Penaltykill points (player, this game). |
| `plus` | Int64 | On ice for goals for (the + of plus/minus). |
| `plus_minus` | Int64 | Plus/minus: on-ice goals for minus on-ice goals against. |
| `powerplay_assists` | Int64 | Powerplay assists (player, this game). |
| `powerplay_goals` | Int64 | Powerplay goals (player, this game). |
| `powerplay_seconds` | Int64 | Powerplay seconds (player, this game). Seconds. |
| `time_on_ice` | Int64 | Time on ice (player, this game). Seconds. |
| `saves` | Int64 | Saves (player, this game). |
| `save_percentage` | Float64 | Save percentage (player, this game). Percent, 0-100, sent as a STRING. |
| `block_or_saved_shots_percentage` | Float64 | Block or saved shots percentage (player, this game). Percent, 0-100, sent as a STRING. |
| `shots` | Int64 | Shots (player, this game). |
| `shots_into_goal` | Int64 | Shots into goal (player, this game). |
| `shut_out` | Int64 | 1 for a goalkeeper's shutout in this game, else 0. |
| `shots_saved` | Int64 | Shots saved (player, this game). |
| `shots_saved_by_skater` | Int64 | Shots saved by skater (player, this game). |
| `shot_percentage` | Float64 | Shot percentage (player, this game). Percent, 0-100, sent as a STRING. |
| `shot_into_goal_percentage` | Float64 | Shot into goal percentage (player, this game). Percent, 0-100, sent as a STRING. |
| `goals_against` | Int64 | Goals against (player, this game). |
| `goalkeeper` | Boolean | True when the player is a goalkeeper. |
| `winning_goals` | Int64 | Winning goals (player, this game). |
| `won` | Int64 | 1 when the player's team won the game, else 0. |
| `tied` | Int64 | 1 when Liiga counts the game as a tie for the player's team, else 0 (overtime handling unverified). |
| `lost` | Int64 | 1 when the player's team lost the game, else 0. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#liiga_player_game_log-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Liiga has nothing for the request (an unknown game, a tournament not played that season, a shot map before 2016). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#liiga_player_game_log-example}

```python
liiga_player_game_log(player_id=21870028, season=2026)
```

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
| `player_id` | Int64 | Player id; the same number in lineups, rosters, stat tables and the profile (fihaId). |
| `team_id` | Int64 | Team id, the number of Liiga's `<num>:<slug>` team key (sent as that string, a bare integer or a digit string depending on the route). |
| `team_name` | String | Team's display name, e.g. HPK or Tappara. |
| `team_short_name` | String | Team abbreviation. |
| `role` | String | Position: CENTER, LEFT_WING, RIGHT_WING, LEFT_DEFENSEMAN, RIGHT_DEFENSEMAN, SEVENTH_DEFENSEMAN, GOALIE. |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `nationality` | String | Nationality, 3-letter code. |
| `tournament` | String | Tournament of the summed row as Liiga labels it, e.g. regular. |
| `picture_url` | String | URL of the player's photo. |
| `previous_teams_for_tournament` | String | Earlier teams of a player traded during the tournament. |
| `injured` | Boolean | True when the player is listed as injured. |
| `jersey` | Int64 | Jersey (sweater) number the player wore. |
| `last_season` | Int64 | Last season (END year) of the range in which the player appears. |
| `goalkeeper` | Boolean | True when the player is a goalkeeper. |
| `games` | Int64 | Games (player, summed over the season range). |
| `played_games` | Int64 | Played games (player, summed over the season range). |
| `rookie` | Boolean | True when Liiga flags the player as a rookie. |
| `suspended` | Boolean | True when the player is listed as suspended. |
| `removed` | Boolean | Removed from the roster. |
| `time_on_ice` | Int64 | Time on ice (player, summed over the season range). Seconds. |
| `current` | Boolean | Player is on the team now. |
| `is_u20` | Boolean | True when Liiga flags the player as under 20 (U20). |
| `golden_helmets` | Int64 | Golden helmets (Liiga's team points-leader helmet; reading unverified), summed over the season range. |
| `alumni_points` | Int64 | Alumni points (meaning undocumented by Liiga), summed over the season range. |
| `goals` | Int64 | Goals (player, summed over the season range). |
| `assists` | Int64 | Assists (player, summed over the season range). |
| `points` | Int64 | Points (player, summed over the season range). |
| `plus` | Int64 | On ice for goals for (the + of plus/minus). |
| `minus` | Int64 | On ice for goals against (the - of plus/minus). |
| `plus_minus` | Int64 | Plus/minus: on-ice goals for minus on-ice goals against. |
| `penalty_minutes` | Int64 | Penalty minutes (player, summed over the season range). |
| `powerplay_goals` | Int64 | Powerplay goals (player, summed over the season range). |
| `penaltykill_goals` | Int64 | Penaltykill goals (player, summed over the season range). |
| `winning_goals` | Int64 | Winning goals (player, summed over the season range). |
| `shots` | Int64 | Shots (player, summed over the season range). |
| `shots_into_goal` | Int64 | Shots into goal (player, summed over the season range). |
| `expected_goals` | Float64 | Expected goals (xG) of the player's shots summed over the season range, from Liiga's undocumented model. |
| `contest_won` | Int64 | Contests (puck battles; definition unverified) the player won, summed over the season range. |
| `contest_lost` | Int64 | Contests (puck battles; definition unverified) the player lost, summed over the season range. |
| `goals_to_empty_goal` | Int64 | Goals to empty goal (player, summed over the season range). |
| `time_on_ice_avg` | Float64 | Time on ice (player, summed over the season range). Seconds per game. |
| `contest_won_percentage` | Float64 | Share of contests won, 0-100, over the season range (definition unverified). |
| `xge` | Float64 | Goals minus expectedGoals over the season range (4 goals on 5.0956 xG = -1.0956). |
| `shot_percentage` | Float64 | Shot percentage (player, summed over the season range). Percent, 0-100. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#liiga_player_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Liiga has nothing for the request (an unknown game, a tournament not played that season, a shot map before 2016). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#liiga_player_stats-example}

```python
liiga_player_stats(season_from=2026, season_to=2026, tournament='runkosarja')
```
