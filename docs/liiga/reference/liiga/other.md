# LIIGA — Liiga API v2 (liiga.fi/api/v2, Finland) — Other

> LIIGA — Liiga API v2 (liiga.fi/api/v2, Finland) — Other — function reference in sdv-py, the SportsDataverse Python package.

## liiga_tournament

Current state of the league: game week of the season in progress and its last game week.

**Endpoint URL:** `GET https://liiga.fi/api/v2/tournament`

**Valid URL:** [https://liiga.fi/api/v2/tournament](https://liiga.fi/api/v2/tournament)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#liiga_tournament-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `cachebust` | logical | Cache-busting flag for the site. |
| `game_week` | integer | Game week of the season. |
| `max_game_week` | integer | Last game week of the season. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#liiga_tournament-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Liiga has nothing for the request (an unknown game, a tournament not played that season, a shot map before 2016). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#liiga_tournament-example}

```python
liiga_tournament()
```

_Last validated n/a._

## liiga_games

Every game of a season and tournament: teams, score, xG, power plays, periods and rink.

**Endpoint URL:** `GET https://liiga.fi/api/v2/games`

**Valid URL:** [https://liiga.fi/api/v2/games?season=2026&tournament=runkosarja](https://liiga.fi/api/v2/games?season=2026&tournament=runkosarja)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season END year (2026 = 2025-26). Omitted = the season in progress. |
| `tournament` | `tournament` |  |  | `Y` | Tournament: runkosarja (regular season), playoffs, playout, qualifications, valmistavat_ottelut (preseason) or chl (Champions Hockey League). Omitted = every tournament, preseason included. |

### Returns {#liiga_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | Game id, unique only within a season: key a game as (season, game_id). |
| `season` | integer | Season END year (2026 = 2025-26). |
| `start` | character | Scheduled start, ISO 8601 UTC. |
| `end` | character | Time the game ended, ISO 8601 UTC (absent from the refs captures; seen in the full 2026 playoff body). |
| `periods` | character | JSON list of the game's periods: index, home and away goals, category, start and end second. |
| `finished_type` | character | How the game ended: ENDED_DURING_REGULAR_GAME_TIME, ENDED_DURING_EXTENDED_GAME_TIME (overtime), ENDED_DURING_WINNING_SHOT_COMPETITION (shootout). |
| `started` | logical | Game has started. |
| `ended` | logical | Game has ended. |
| `game_time` | integer | Seconds from the start of the game. Regular season: 3900 (65:00) = the end of the 5-minute overtime; playoff overtime runs past it. |
| `spectators` | integer | Attendance (spectators) reported for the game. |
| `play_off_pair` | integer | Playoff pairing number, else null. |
| `play_off_phase` | integer | Playoff round, else null. |
| `play_off_req_wins` | integer | Wins needed to take the playoff series, else null. |
| `buy_tickets_url` | character | Ticket link for the game, else null. |
| `ice_rink_id` | integer | Liiga's id of the rink (ice arena). |
| `ice_rink_name` | character | Name of the rink (ice arena). |
| `ice_rink_latitude` | numeric | Rink latitude, decimal degrees. |
| `ice_rink_longitude` | numeric | Rink longitude, decimal degrees. |
| `ice_rink_street_address` | character | Street address of the rink. |
| `ice_rink_zip` | character | Postal code of the rink. |
| `ice_rink_city` | character | City where the rink is located. |
| `current_period` | integer | Last period played (3 = regulation; 4 and up = overtime periods or the shootout; 2026 values 3, 4, 5, 7). |
| `stale` | logical | Live data is stale. |
| `serie` | character | Tournament code, uppercase (RUNKOSARJA, PLAYOFFS, PRACTICE, ...); in /schedule the integer tournament id instead (see serieId and Divergence notes). |
| `game_week` | integer | Game week of the season. |
| `home_team_id` | integer | Home team id (the number of the `<num>:<slug>` key). |
| `home_team_slug` | character | Home team slug (e.g. hpk). |
| `home_placeholder` | character | Home team: placeholder label for an undecided playoff team, else null. |
| `home_name` | character | Home team: display name of the team, e.g. HPK. |
| `home_goals` | integer | Home team: goals scored by the team in the game. |
| `home_time_out` | integer | Home team: second of the game (from game start) at which the team called its timeout. |
| `home_powerplay_instances` | integer | Home team: power plays the team had in the game. |
| `home_powerplay_goals` | integer | Home team: power-play goals the team scored in the game. |
| `home_short_handed_instances` | integer | Home team: times the team played short-handed in the game. |
| `home_short_handed_goals` | integer | Home team: short-handed goals the team scored in the game. |
| `home_expected_goals` | numeric | Home team: expected goals (xG) for the team in the game, from Liiga's undocumented model. |
| `home_ranking` | integer | Home team: table position. |
| `home_game_start_date_time` | character | Home team: scheduled start, ISO 8601 UTC. |
| `home_logos_dark_bg` | character | Home team: logo image URL for dark backgrounds. |
| `home_logos_light_bg` | character | Home team: logo image URL for light backgrounds. |
| `home_logos_dark_bg_original` | character | Home team: original-file logo URL for dark backgrounds; null in every capture. |
| `home_logos_light_bg_original` | character | Home team: original-file logo URL for light backgrounds; null in every capture. |
| `away_team_id` | integer | Away team id (the number of the `<num>:<slug>` key). |
| `away_team_slug` | character | Away team slug (e.g. tps). |
| `away_placeholder` | character | Away team: placeholder label for an undecided playoff team, else null. |
| `away_name` | character | Away team: display name of the team, e.g. HPK. |
| `away_goals` | integer | Away team: goals scored by the team in the game. |
| `away_time_out` | integer | Away team: second of the game (from game start) at which the team called its timeout. |
| `away_powerplay_instances` | integer | Away team: power plays the team had in the game. |
| `away_powerplay_goals` | integer | Away team: power-play goals the team scored in the game. |
| `away_short_handed_instances` | integer | Away team: times the team played short-handed in the game. |
| `away_short_handed_goals` | integer | Away team: short-handed goals the team scored in the game. |
| `away_expected_goals` | numeric | Away team: expected goals (xG) for the team in the game, from Liiga's undocumented model. |
| `away_ranking` | integer | Away team: table position. |
| `away_game_start_date_time` | character | Away team: scheduled start, ISO 8601 UTC. |
| `away_logos_dark_bg` | character | Away team: logo image URL for dark backgrounds. |
| `away_logos_light_bg` | character | Away team: logo image URL for light backgrounds. |
| `away_logos_dark_bg_original` | character | Away team: original-file logo URL for dark backgrounds; null in every capture. |
| `away_logos_light_bg_original` | character | Away team: original-file logo URL for light backgrounds; null in every capture. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#liiga_games-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Liiga has nothing for the request (an unknown game, a tournament not played that season, a shot map before 2016). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#liiga_games-example}

```python
liiga_games(season=2026, tournament='runkosarja')
```

_Last validated n/a._

## liiga_goals

Every goal of a season and tournament: scorer, assists, strength codes and skaters on ice.

**Endpoint URL:** `GET https://liiga.fi/api/v2/games`

**Valid URL:** [https://liiga.fi/api/v2/games?season=2026&tournament=runkosarja](https://liiga.fi/api/v2/games?season=2026&tournament=runkosarja)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season END year (2026 = 2025-26). Omitted = the season in progress. |
| `tournament` | `tournament` |  |  | `Y` | Tournament: runkosarja (regular season), playoffs, playout, qualifications, valmistavat_ottelut (preseason) or chl (Champions Hockey League). Omitted = every tournament, preseason included. |

### Returns {#liiga_goals-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2026 = 2025-26). |
| `game_id` | integer | Game id, unique only within a season: key a game as (season, game_id). |
| `side` | character | Which team of the game the row belongs to: home or away. |
| `team_id` | integer | Team id, the number of Liiga's `<num>:<slug>` team key (sent as that string, a bare integer or a digit string depending on the route). |
| `team_slug` | character | Team slug, the part of Liiga's `<num>:<slug>` team key after the colon (e.g. hpk). |
| `log_time` | character | Wall-clock time the event was logged, ISO 8601 UTC. |
| `winning_goal` | logical | Game-winning goal. |
| `game_time` | integer | Seconds from the start of the game. Regular season: 3900 (65:00) = the end of the 5-minute overtime; playoff overtime runs past it. |
| `period` | integer | Period number: 1-3 are regulation, 4 and up overtime (or the shootout). |
| `event_id` | integer | Event id within the game. |
| `home_team_score` | integer | Home score after the event. |
| `away_team_score` | integer | Away score after the event. |
| `goals_so_far_in_season` | integer | Scorer's season goal count after this goal. |
| `video_clip_url` | character | URL of the goal's video clip; null in every capture. |
| `video_clip_playlist_url` | character | URL of the goal's video clip playlist; null in every capture. |
| `video_thumbnail_url` | character | URL of the goal video's thumbnail image; null in every capture. |
| `scorer_player_id` | integer | Scorer's player id. |
| `scorer_first_name` | character | Scorer's first name. |
| `scorer_last_name` | character | Scorer's last name. |
| `assist1_player_id` | integer | First assist's player id. |
| `assist1_first_name` | character | First assist's first name. |
| `assist1_last_name` | character | First assist's last name. |
| `assist1_season_assists` | integer | First assister's season assist count after this goal. |
| `assist2_player_id` | integer | Second assist's player id. |
| `assist2_first_name` | character | Second assist's first name. |
| `assist2_last_name` | character | Second assist's last name. |
| `assist2_season_assists` | integer | Second assister's season assist count after this goal. |
| `goal_types` | character | Goal type codes, comma-separated Finnish abbreviations: YV power play, AV short-handed, TM empty net, RL penalty shot (others undocumented). |
| `plus_jerseys` | character | Jersey NUMBERS of this team's skaters on ice for the goal, space-separated. |
| `minus_jerseys` | character | Jersey NUMBERS of the other team's skaters on ice for the goal, space-separated. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#liiga_goals-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Liiga has nothing for the request (an unknown game, a tournament not played that season, a shot map before 2016). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#liiga_goals-example}

```python
liiga_goals(season=2026, tournament='runkosarja')
```

_Last validated n/a._

## liiga_schedule

Schedule of a season and tournament: game ids, teams, goals, xG, start and rink.

**Endpoint URL:** `GET https://liiga.fi/api/v2/schedule`

**Valid URL:** [https://liiga.fi/api/v2/schedule?season=2026&tournament=runkosarja](https://liiga.fi/api/v2/schedule?season=2026&tournament=runkosarja)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season END year (2026 = 2025-26). Omitted = the season in progress. |
| `tournament` | `tournament` |  |  | `Y` | Tournament: runkosarja (regular season), playoffs, playout, qualifications, valmistavat_ottelut (preseason) or chl (Champions Hockey League). Omitted = runkosarja. |

### Returns {#liiga_schedule-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | Game id, unique only within a season: key a game as (season, game_id). |
| `serie_id` | integer | Tournament id, the same every season: 1188 regular season, 1189 playoffs, 1194 preseason, 1890 CHL. |
| `season` | integer | Season END year (2026 = 2025-26). |
| `home_team_name` | character | Display name of the home team, e.g. HPK. |
| `away_team_name` | character | Display name of the away team, e.g. TPS. |
| `home_team_id` | integer | Home team id (the number of the `<num>:<slug>` key). |
| `home_team_slug` | character | Home team slug (e.g. hpk). |
| `away_team_id` | integer | Away team id (the number of the `<num>:<slug>` key). |
| `away_team_slug` | character | Away team slug (e.g. tps). |
| `home_team_placeholder` | character | Placeholder label for an undecided playoff home team, else null. |
| `away_team_placeholder` | character | Placeholder label for an undecided playoff away team, else null. |
| `home_team_goals` | integer | Goals scored by the home team in the game. |
| `away_team_goals` | integer | Goals scored by the away team in the game. |
| `expected_home_team_goals` | numeric | Home team's expected goals (xG) for the game, provider-computed (same value as /games `homeTeam.expectedGoals`). |
| `expected_away_team_goals` | numeric | Away team's expected goals (xG) for the game, provider-computed. |
| `game_time` | integer | Seconds from the start of the game. Regular season: 3900 (65:00) = the end of the 5-minute overtime; playoff overtime runs past it. |
| `started` | logical | Game has started. |
| `ended` | logical | Game has ended. |
| `home_team_winner` | logical | True when the home team won the game. |
| `spectators` | integer | Attendance (spectators) reported for the game. |
| `home_team_ranking` | integer | Home team's table position; null in every capture. |
| `away_team_ranking` | integer | Away team's table position; null in every capture. |
| `play_off_pair` | integer | Playoff pairing number, else null. |
| `play_off_phase` | integer | Playoff round, else null. |
| `play_off_req_wins` | integer | Wins needed to take the series, else null. |
| `start` | character | Scheduled start, ISO 8601 UTC. |
| `buy_tickets_url` | character | Ticket link, else null. |
| `finished_type` | character | How the game ended: ENDED_DURING_REGULAR_GAME_TIME, ENDED_DURING_EXTENDED_GAME_TIME (overtime), ENDED_DURING_WINNING_SHOT_COMPETITION (shootout). |
| `ice_rink_id` | integer | Liiga's id of the rink (ice arena). |
| `ice_rink_name` | character | Name of the rink (ice arena). |
| `ice_rink_latitude` | numeric | Rink latitude, decimal degrees. |
| `ice_rink_longitude` | numeric | Rink longitude, decimal degrees. |
| `ice_rink_street_address` | character | Street address of the rink. |
| `ice_rink_zip` | character | Postal code of the rink. |
| `ice_rink_city` | character | City where the rink is located. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#liiga_schedule-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Liiga has nothing for the request (an unknown game, a tournament not played that season, a shot map before 2016). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#liiga_schedule-example}

```python
liiga_schedule(season=2026, tournament='runkosarja')
```

_Last validated n/a._

## liiga_gameweeks

Number of game weeks of a season and tournament.

**Endpoint URL:** `GET https://liiga.fi/api/v2/gameweeks`

**Valid URL:** [https://liiga.fi/api/v2/gameweeks?season=2026&tournament=runkosarja](https://liiga.fi/api/v2/gameweeks?season=2026&tournament=runkosarja)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season END year (2026 = 2025-26). Omitted = the season in progress. |
| `tournament` | `tournament` |  |  | `Y` | Tournament: runkosarja (regular season), playoffs, playout, qualifications, valmistavat_ottelut (preseason) or chl (Champions Hockey League). Required by the API; defaults to runkosarja. |

### Returns {#liiga_gameweeks-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2026 = 2025-26). |
| `serie` | character | Tournament code, uppercase (RUNKOSARJA, PLAYOFFS, PRACTICE, ...); in /schedule the integer tournament id instead (see serieId and Divergence notes). |
| `nb_weeks` | integer | Number of game weeks. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#liiga_gameweeks-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Liiga has nothing for the request (an unknown game, a tournament not played that season, a shot map before 2016). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#liiga_gameweeks-example}

```python
liiga_gameweeks(season=2026)
```

_Last validated n/a._

## liiga_game

One game's header: teams, score, xG, power plays, periods, rink, referees and attendance.

**Endpoint URL:** `GET https://liiga.fi/api/v2/games/{season}/{game_id}`

**Valid URL:** [https://liiga.fi/api/v2/games/2026/1](https://liiga.fi/api/v2/games/2026/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26). Game ids repeat across seasons, so a game is (season, game_id). |
| `game_id` | `game_id` |  | `Y` |  | Game id (game_id in liiga_schedule or liiga_games), e.g. 1; unique only within a season. |

### Returns {#liiga_game-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | Game id, unique only within a season: key a game as (season, game_id). |
| `season` | integer | Season END year (2026 = 2025-26). |
| `start` | character | Scheduled start, ISO 8601 UTC. |
| `periods` | character | JSON list of the game's periods: index, home and away goals, category, start and end second. |
| `finished_type` | character | How the game ended: ENDED_DURING_REGULAR_GAME_TIME, ENDED_DURING_EXTENDED_GAME_TIME (overtime), ENDED_DURING_WINNING_SHOT_COMPETITION (shootout). |
| `started` | logical | Game has started. |
| `ended` | logical | Game has ended. |
| `game_time` | integer | Seconds from the start of the game. Regular season: 3900 (65:00) = the end of the 5-minute overtime; playoff overtime runs past it. |
| `spectators` | integer | Attendance (spectators) reported for the game. |
| `ice_rink_id` | integer | Liiga's id of the rink (ice arena). |
| `ice_rink_name` | character | Name of the rink (ice arena). |
| `ice_rink_latitude` | numeric | Rink latitude, decimal degrees. |
| `ice_rink_longitude` | numeric | Rink longitude, decimal degrees. |
| `ice_rink_street_address` | character | Street address of the rink. |
| `ice_rink_zip` | character | Postal code of the rink. |
| `ice_rink_city` | character | City where the rink is located. |
| `current_period` | integer | Last period played (3 = regulation; 4 and up = overtime periods or the shootout; 2026 values 3, 4, 5, 7). |
| `referees` | character | Game officials. |
| `stale` | logical | Live data is stale. |
| `serie` | character | Tournament code, uppercase (RUNKOSARJA, PLAYOFFS, PRACTICE, ...); in /schedule the integer tournament id instead (see serieId and Divergence notes). |
| `game_week` | integer | Game week of the season. |
| `home_team_id` | integer | Home team id (the number of the `<num>:<slug>` key). |
| `home_team_slug` | character | Home team slug (e.g. hpk). |
| `home_placeholder` | character | Home team: placeholder label for an undecided playoff team, else null. |
| `home_name` | character | Home team: display name of the team, e.g. HPK. |
| `home_goals` | integer | Home team: goals scored by the team in the game. |
| `home_time_out` | integer | Home team: second of the game (from game start) at which the team called its timeout. |
| `home_powerplay_instances` | integer | Home team: power plays the team had in the game. |
| `home_powerplay_goals` | integer | Home team: power-play goals the team scored in the game. |
| `home_short_handed_instances` | integer | Home team: times the team played short-handed in the game. |
| `home_short_handed_goals` | integer | Home team: short-handed goals the team scored in the game. |
| `home_expected_goals` | numeric | Home team: expected goals (xG) for the team in the game, from Liiga's undocumented model. |
| `home_ranking` | integer | Home team: table position. |
| `home_game_start_date_time` | character | Home team: scheduled start, ISO 8601 UTC. |
| `home_logos_dark_bg` | character | Home team: logo image URL for dark backgrounds. |
| `home_logos_light_bg` | character | Home team: logo image URL for light backgrounds. |
| `home_logos_dark_bg_original` | character | Home team: original-file logo URL for dark backgrounds; null in every capture. |
| `home_logos_light_bg_original` | character | Home team: original-file logo URL for light backgrounds; null in every capture. |
| `away_team_id` | integer | Away team id (the number of the `<num>:<slug>` key). |
| `away_team_slug` | character | Away team slug (e.g. tps). |
| `away_placeholder` | character | Away team: placeholder label for an undecided playoff team, else null. |
| `away_name` | character | Away team: display name of the team, e.g. HPK. |
| `away_goals` | integer | Away team: goals scored by the team in the game. |
| `away_time_out` | integer | Away team: second of the game (from game start) at which the team called its timeout. |
| `away_powerplay_instances` | integer | Away team: power plays the team had in the game. |
| `away_powerplay_goals` | integer | Away team: power-play goals the team scored in the game. |
| `away_short_handed_instances` | integer | Away team: times the team played short-handed in the game. |
| `away_short_handed_goals` | integer | Away team: short-handed goals the team scored in the game. |
| `away_expected_goals` | numeric | Away team: expected goals (xG) for the team in the game, from Liiga's undocumented model. |
| `away_ranking` | integer | Away team: table position. |
| `away_game_start_date_time` | character | Away team: scheduled start, ISO 8601 UTC. |
| `away_logos_dark_bg` | character | Away team: logo image URL for dark backgrounds. |
| `away_logos_light_bg` | character | Away team: logo image URL for light backgrounds. |
| `away_logos_dark_bg_original` | character | Away team: original-file logo URL for dark backgrounds; null in every capture. |
| `away_logos_light_bg_original` | character | Away team: original-file logo URL for light backgrounds; null in every capture. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#liiga_game-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Liiga has nothing for the request (an unknown game, a tournament not played that season, a shot map before 2016). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#liiga_game-example}

```python
liiga_game(game_id=1, season=2026)
```

_Last validated n/a._

## liiga_shotmap

One game's shots with rink-image coordinates (pixels), shooter, blocker and skaters on ice; from 2016.

**Endpoint URL:** `GET https://liiga.fi/api/v2/shotmap/{season}/{game_id}`

**Valid URL:** [https://liiga.fi/api/v2/shotmap/2026/1](https://liiga.fi/api/v2/shotmap/2026/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26). Game ids repeat across seasons, so a game is (season, game_id). |
| `game_id` | `game_id` |  | `Y` |  | Game id (game_id in liiga_schedule or liiga_games), e.g. 1; unique only within a season. |

### Returns {#liiga_shotmap-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2026 = 2025-26). |
| `game_id` | integer | Game id, unique only within a season: key a game as (season, game_id). |
| `type` | character | Strength of the shot, e.g. EvenStrengthShot, PowerplayShot. |
| `shooting_team_id` | integer | Team id of the shooter's team (the number of the `<num>:<slug>` key). |
| `left_team_id` | integer | Team whose shots are drawn at the right end of the rink image (x grows toward it). |
| `right_team_id` | integer | Team whose shots are drawn at the left end of the rink image. |
| `game_time` | integer | Seconds from the start of the game. Regular season: 3900 (65:00) = the end of the 5-minute overtime; playoff overtime runs past it. |
| `period` | integer | Period number: 1-3 are regulation, 4 and up overtime (or the shootout). |
| `shot_x` | integer | Shot x on the site's rink drawing. Game 1 of 2026: x 90-932; the leftTeam's shots lie at x 188-932 and the rightTeam's at 90-379, so x grows toward the right end. Origin, unit and image width are not stated (unverified). |
| `shot_y` | integer | Shot y on the site's rink drawing. Game 1 of 2026: y 9-514. Origin, direction and image height are not stated (unverified). |
| `blocker_id` | integer | Player id of the goalkeeper or skater who stopped the shot, else null. |
| `shooter_id` | integer | Shooter's player id. |
| `event_type` | character | Shot outcome: GOAL, GOALIE_BLOCKED (saved), MISSED, PLAYER_BLOCKED. |
| `own_team_players_on_ice` | integer | Shooting team's skaters on ice. |
| `other_team_players_on_ice` | integer | Defending team's skaters on ice. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#liiga_shotmap-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Liiga has nothing for the request (an unknown game, a tournament not played that season, a shot map before 2016). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#liiga_shotmap-example}

```python
liiga_shotmap(game_id=1, season=2026)
```

_Last validated n/a._

## liiga_standings

Standings of a season: regular season, playoffs, preseason, playout and qualifications tables.

**Endpoint URL:** `GET https://liiga.fi/api/v2/standings/`

**Valid URL:** [https://liiga.fi/api/v2/standings/?season=2026](https://liiga.fi/api/v2/standings/?season=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season END year (2026 = 2025-26). Omitted = the season in progress. |

### Returns {#liiga_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2026 = 2025-26). |
| `tournament` | character | Table the row belongs to, in the request vocabulary: runkosarja, playoffs, valmistavat_ottelut, playout or qualifications. |
| `internal_id` | integer | Team id (same number as team_id). |
| `team_id` | integer | Team id, the number of Liiga's `<num>:<slug>` team key (sent as that string, a bare integer or a digit string depending on the route). |
| `team_slug` | character | Team slug, the part of Liiga's `<num>:<slug>` team key after the colon (e.g. hpk). |
| `team_name` | character | Team's display name, e.g. HPK or Tappara. |
| `team_logos_dark_bg` | character | Logo image URL for dark backgrounds. |
| `team_logos_light_bg` | character | Logo image URL for light backgrounds. |
| `team_logos_dark_bg_original` | character | Original-file logo URL for dark backgrounds; null in every capture. |
| `team_logos_light_bg_original` | character | Original-file logo URL for light backgrounds; null in every capture. |
| `ranking` | integer | Table position. |
| `live_ranking` | integer | Table position counting games in progress. |
| `games` | integer | Games (team, season to date in that table's tournament). |
| `live_games` | integer | Games counting games in progress (team, season to date in that table's tournament). |
| `wins` | integer | Wins (team, season to date in that table's tournament). |
| `live_wins` | integer | Wins counting games in progress (team, season to date in that table's tournament). |
| `overtime_wins` | integer | Overtime wins (team, season to date in that table's tournament). |
| `losses` | integer | Losses (team, season to date in that table's tournament). |
| `live_losses` | integer | Losses counting games in progress (team, season to date in that table's tournament). |
| `ties` | integer | Ties (team, season to date in that table's tournament). |
| `live_ties` | integer | Ties counting games in progress (team, season to date in that table's tournament). |
| `overtime_losses` | integer | Overtime losses (team, season to date in that table's tournament). |
| `points` | integer | Points (team, season to date in that table's tournament). |
| `live_points` | integer | Points counting games in progress (team, season to date in that table's tournament). |
| `goals` | integer | Goals (team, season to date in that table's tournament). |
| `goals_against` | integer | Goals against (team, season to date in that table's tournament). |
| `live_goals` | integer | Goals counting games in progress (team, season to date in that table's tournament). |
| `live_goals_against` | integer | Goals against counting games in progress (team, season to date in that table's tournament). |
| `power_play_instances` | integer | Power play instances (team, season to date in that table's tournament). |
| `power_play_time` | integer | Seconds on the power play, season to date. |
| `power_play_goals` | integer | Power play goals (team, season to date in that table's tournament). |
| `short_handed_instances` | integer | Short handed instances (team, season to date in that table's tournament). |
| `short_handed_time` | integer | Seconds short-handed, season to date. |
| `short_handed_goals_against` | integer | Short handed goals against (team, season to date in that table's tournament). |
| `penalty_minutes` | integer | Penalty minutes (team, season to date in that table's tournament). |
| `two_minute_penalties` | integer | Two minute penalties (team, season to date in that table's tournament). |
| `five_minute_penalties` | integer | Five minute penalties (team, season to date in that table's tournament). |
| `ten_minute_penalties` | integer | Ten minute penalties (team, season to date in that table's tournament). |
| `twenty_minute_penalties` | integer | Twenty minute penalties (team, season to date in that table's tournament). |
| `twenty_five_minute_penalties` | integer | Twenty five minute penalties (team, season to date in that table's tournament). |
| `total_penalties` | integer | Total penalties (team, season to date in that table's tournament). |
| `distance` | numeric | Distance (team, season to date in that table's tournament). Distance; unit not stated (unverified). |
| `serie` | character | Tournament code, uppercase (RUNKOSARJA, PLAYOFFS, PRACTICE, ...); in /schedule the integer tournament id instead (see serieId and Divergence notes). |
| `live_points_per_game` | numeric | Points per game counting games in progress (team, season to date in that table's tournament). |
| `power_play_percentage` | numeric | Power-play conversion, a 0-1 FRACTION sent as a STRING (e.g. "0.277"), season to date. |
| `short_handed_percentage` | numeric | Penalty-kill success, a 0-1 FRACTION sent as a STRING (e.g. "0.815"), season to date. |
| `distance_per_game` | numeric | Distance per game (team, season to date in that table's tournament). Distance; unit not stated (unverified). |
| `points_per_game` | numeric | Points per game (team, season to date in that table's tournament). |
| `win_percentage` | numeric | Share of games won, a 0-1 FRACTION sent as a STRING (e.g. "0.583"), season to date. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#liiga_standings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Liiga has nothing for the request (an unknown game, a tournament not played that season, a shot map before 2016). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#liiga_standings-example}

```python
liiga_standings(season=2026)
```

_Last validated n/a._
