# HockeyAllsvenskan — HockeyAllsvenskan site API (hockeyallsvenskan.se, Sweden)

> HockeyAllsvenskan — HockeyAllsvenskan site API (hockeyallsvenskan.se, Sweden) — endpoint reference in sdv-py, the SportsDataverse Python package.

`sportsdataverse.hockey.ha` — 11 endpoints.

## ha_schedule

The current season's games (CMS records, decoded from the schedule page): slug, StatNet game number and team ids, start time, score.

**Endpoint URL:** `GET https://hockeyallsvenskan.se/pages/matcher`

**Valid URL:** [https://hockeyallsvenskan.se/pages/matcher](https://hockeyallsvenskan.se/pages/matcher)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#ha_schedule-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season END year (2026 = 2025/26). |
| `game_cms_id` | Int64 | CMS (Strapi) record id of the game. |
| `game_document_id` | String | CMS (Strapi) document id of the game. |
| `cms_season` | String | CMS season key as sent (season_2026 = 2026/27: a START year). |
| `statnet_game_number` | Int64 | StatNet game number (23401 = 2026/27 game 1); the play-by-play request key. |
| `slug` | String | Game slug yyyymmdd-home-away (lower-case CMS short names); the /api/game?slug= key. |
| `round` | Int64 | Round number of the game in the season schedule. |
| `scheduled_date_time` | String | Scheduled face-off time (ISO 8601 UTC). |
| `venue` | String | Arena name on the CMS game record. |
| `attendance` | Int64 | Attendance; null for an unplayed game (0 for a played one means unknown). |
| `current_period` | String | Live period while the game runs; empty string '' before and after. |
| `current_period_start` | String | Start time of the live period; null outside a live game. |
| `home_score` | Int64 | Home goals, final. |
| `away_score` | Int64 | Away goals, final. |
| `ended_date_time` | String | End time (ISO 8601 UTC). |
| `decided_in` | String | How the game was decided (FT, OT, SO); empty for a game not yet played. |
| `home_score_p1` | Int64 | Home goals in period 1. |
| `away_score_p1` | Int64 | Away goals in period 1. |
| `home_score_p2` | Int64 | Home goals in period 2. |
| `away_score_p2` | Int64 | Away goals in period 2. |
| `home_score_p3` | Int64 | Home goals in period 3. |
| `away_score_p3` | Int64 | Away goals in period 3. |
| `home_ot_score` | Int64 | Home overtime goals; null when the game had no overtime. |
| `away_ot_score` | Int64 | Away overtime goals; null when the game had no overtime. |
| `home_so_score` | Int64 | Home shootout goals; null when the game had no shootout. |
| `away_so_score` | Int64 | Away shootout goals; null when the game had no shootout. |
| `game_type` | String | Game type (phase): HA regular season, HAPlayoff playoffs, HAkval playout. |
| `play_off_game` | String | Game of the playoff series (1, 2, ...); null outside the playoffs. |
| `play_off_game_level` | String | Playoff round label. |
| `played_date_time` | String | Actual start time (ISO 8601 UTC). |
| `is_completed` | Boolean | true once the game is final. |
| `home_statnet_id` | String | StatNet team id of the home team (AIK, MODO, MIK). |
| `away_statnet_id` | String | StatNet team id of the away team. |
| `created_at` | String | CMS creation time of the record (ISO 8601 UTC). |
| `updated_at` | String | CMS update time of the record (ISO 8601 UTC). |
| `published_at` | String | CMS publish time of the record (ISO 8601 UTC). |
| `home_team_cms_id` | Int64 | CMS (Strapi) record id of the home team (81 = AIK). |
| `home_team_document_id` | String | Home team: CMS (Strapi) document id (string; stable across drafts and locales). |
| `home_team_name` | String | Home team: short club name (the teams list's name). |
| `home_team_code` | String | Display code of the home team (AIK, MoDo, MORA, NVIF, ÖIK), not the StatNet id. |
| `home_team_main_color_hex` | String | Home team: primary colour (hex). |
| `home_team_alternate_color_hex` | String | Home team: secondary colour (hex). |
| `home_team_team_arena` | String | Home team: home arena. |
| `home_team_arena_address` | String | Home team: postal address of the arena (multi-line, newline-separated). |
| `home_team_arena_latitude` | Float64 | Home team: arena latitude, decimal degrees. |
| `home_team_arena_longitude` | Float64 | Home team: arena longitude, decimal degrees. |
| `home_team_arena_telephone` | String | Home team: arena telephone contact. |
| `home_team_tickets_provider_url` | String | Home team: ticket shop URL. |
| `home_team_club_mate_lottery_url` | String | Home team: club lottery URL. |
| `home_team_created_at` | String | Home team: CMS creation time of the record (ISO 8601 UTC). |
| `home_team_updated_at` | String | Home team: CMS update time of the record (ISO 8601 UTC). |
| `home_team_published_at` | String | Home team: CMS publish time of the record (ISO 8601 UTC). |
| `home_team_logo_url` | String | Logo URL of the home team. |
| `away_team_cms_id` | Int64 | CMS (Strapi) record id of the away team. |
| `away_team_document_id` | String | Away team: CMS (Strapi) document id (string; stable across drafts and locales). |
| `away_team_name` | String | Away team: short club name (the teams list's name). |
| `away_team_code` | String | Display code of the away team, not the StatNet id. |
| `away_team_main_color_hex` | String | Away team: primary colour (hex). |
| `away_team_alternate_color_hex` | String | Away team: secondary colour (hex). |
| `away_team_team_arena` | String | Away team: home arena. |
| `away_team_arena_address` | String | Away team: postal address of the arena (multi-line, newline-separated). |
| `away_team_arena_latitude` | Float64 | Away team: arena latitude, decimal degrees. |
| `away_team_arena_longitude` | Float64 | Away team: arena longitude, decimal degrees. |
| `away_team_arena_telephone` | String | Away team: arena telephone contact. |
| `away_team_tickets_provider_url` | String | Away team: ticket shop URL. |
| `away_team_club_mate_lottery_url` | String | Away team: club lottery URL. |
| `away_team_created_at` | String | Away team: CMS creation time of the record (ISO 8601 UTC). |
| `away_team_updated_at` | String | Away team: CMS update time of the record (ISO 8601 UTC). |
| `away_team_published_at` | String | Away team: CMS publish time of the record (ISO 8601 UTC). |
| `away_team_logo_url` | String | Logo URL of the away team. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#ha_schedule-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (a season before 2005/06, an unknown or pre-2025/26 game, a phase not played). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A value outside its enumerated set, or a field the site requires is missing. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#ha_schedule-example}

```python
ha_schedule()
```

## ha_game

One game's CMS record by slug (2025/26 onward): StatNet game number and team ids, scores by period, venue, attendance.

**Endpoint URL:** `GET https://hockeyallsvenskan.se/api/game`

**Valid URL:** [https://hockeyallsvenskan.se/api/game?slug=20260918-aik-modo](https://hockeyallsvenskan.se/api/game?slug=20260918-aik-modo)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `slug` | `slug` |  | `Y` |  | Game slug yyyymmdd-home-away (slug in ha_schedule), e.g. 20260918-aik-modo. |

### Returns {#ha_game-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season END year (2026 = 2025/26). |
| `game_cms_id` | Int64 | CMS (Strapi) record id of the game. |
| `game_document_id` | String | CMS (Strapi) document id of the game. |
| `cms_season` | String | CMS season key as sent (season_2026 = 2026/27: a START year). |
| `statnet_game_number` | Int64 | StatNet game number (23401 = 2026/27 game 1); the play-by-play request key. |
| `slug` | String | Game slug yyyymmdd-home-away (lower-case CMS short names); the /api/game?slug= key. |
| `round` | Int64 | Round number of the game in the season schedule. |
| `scheduled_date_time` | String | Scheduled face-off time (ISO 8601 UTC). |
| `venue` | String | Arena name on the CMS game record. |
| `attendance` | Int64 | Attendance; null for an unplayed game (0 for a played one means unknown). |
| `current_period` | String | Live period while the game runs; empty string '' before and after. |
| `current_period_start` | String | Start time of the live period; null outside a live game. |
| `home_score` | Int64 | Home goals, final. |
| `away_score` | Int64 | Away goals, final. |
| `ended_date_time` | String | End time (ISO 8601 UTC). |
| `decided_in` | String | How the game was decided (FT, OT, SO); empty for a game not yet played. |
| `home_score_p1` | Int64 | Home goals in period 1. |
| `away_score_p1` | Int64 | Away goals in period 1. |
| `home_score_p2` | Int64 | Home goals in period 2. |
| `away_score_p2` | Int64 | Away goals in period 2. |
| `home_score_p3` | Int64 | Home goals in period 3. |
| `away_score_p3` | Int64 | Away goals in period 3. |
| `home_ot_score` | Int64 | Home overtime goals; null when the game had no overtime. |
| `away_ot_score` | Int64 | Away overtime goals; null when the game had no overtime. |
| `home_so_score` | Int64 | Home shootout goals; null when the game had no shootout. |
| `away_so_score` | Int64 | Away shootout goals; null when the game had no shootout. |
| `game_type` | String | Game type (phase): HA regular season, HAPlayoff playoffs, HAkval playout. |
| `play_off_game` | String | Game of the playoff series (1, 2, ...); null outside the playoffs. |
| `play_off_game_level` | String | Playoff round label. |
| `played_date_time` | String | Actual start time (ISO 8601 UTC). |
| `is_completed` | Boolean | true once the game is final. |
| `home_statnet_id` | String | StatNet team id of the home team (AIK, MODO, MIK). |
| `away_statnet_id` | String | StatNet team id of the away team. |
| `created_at` | String | CMS creation time of the record (ISO 8601 UTC). |
| `updated_at` | String | CMS update time of the record (ISO 8601 UTC). |
| `published_at` | String | CMS publish time of the record (ISO 8601 UTC). |
| `home_team_cms_id` | Int64 | CMS (Strapi) record id of the home team (81 = AIK). |
| `home_team_document_id` | String | Home team: CMS (Strapi) document id (string; stable across drafts and locales). |
| `home_team_name` | String | Home team: short club name (the teams list's name). |
| `home_team_code` | String | Display code of the home team (AIK, MoDo, MORA, NVIF, ÖIK), not the StatNet id. |
| `home_team_main_color_hex` | String | Home team: primary colour (hex). |
| `home_team_alternate_color_hex` | String | Home team: secondary colour (hex). |
| `home_team_team_arena` | String | Home team: home arena. |
| `home_team_arena_address` | String | Home team: postal address of the arena (multi-line, newline-separated). |
| `home_team_arena_latitude` | Float64 | Home team: arena latitude, decimal degrees. |
| `home_team_arena_longitude` | Float64 | Home team: arena longitude, decimal degrees. |
| `home_team_arena_telephone` | String | Home team: arena telephone contact. |
| `home_team_tickets_provider_url` | String | Home team: ticket shop URL. |
| `home_team_club_mate_lottery_url` | String | Home team: club lottery URL. |
| `home_team_created_at` | String | Home team: CMS creation time of the record (ISO 8601 UTC). |
| `home_team_updated_at` | String | Home team: CMS update time of the record (ISO 8601 UTC). |
| `home_team_published_at` | String | Home team: CMS publish time of the record (ISO 8601 UTC). |
| `home_team_logo_url` | String | Logo URL of the home team. |
| `away_team_cms_id` | Int64 | CMS (Strapi) record id of the away team. |
| `away_team_document_id` | String | Away team: CMS (Strapi) document id (string; stable across drafts and locales). |
| `away_team_name` | String | Away team: short club name (the teams list's name). |
| `away_team_code` | String | Display code of the away team, not the StatNet id. |
| `away_team_main_color_hex` | String | Away team: primary colour (hex). |
| `away_team_alternate_color_hex` | String | Away team: secondary colour (hex). |
| `away_team_team_arena` | String | Away team: home arena. |
| `away_team_arena_address` | String | Away team: postal address of the arena (multi-line, newline-separated). |
| `away_team_arena_latitude` | Float64 | Away team: arena latitude, decimal degrees. |
| `away_team_arena_longitude` | Float64 | Away team: arena longitude, decimal degrees. |
| `away_team_arena_telephone` | String | Away team: arena telephone contact. |
| `away_team_tickets_provider_url` | String | Away team: ticket shop URL. |
| `away_team_club_mate_lottery_url` | String | Away team: club lottery URL. |
| `away_team_created_at` | String | Away team: CMS creation time of the record (ISO 8601 UTC). |
| `away_team_updated_at` | String | Away team: CMS update time of the record (ISO 8601 UTC). |
| `away_team_published_at` | String | Away team: CMS publish time of the record (ISO 8601 UTC). |
| `away_team_logo_url` | String | Logo URL of the away team. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#ha_game-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (a season before 2005/06, an unknown or pre-2025/26 game, a phase not played). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A value outside its enumerated set, or a field the site requires is missing. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#ha_game-example}

```python
ha_game(slug='20260918-aik-modo')
```

## ha_pbp

One game's events (2025/26 onward): shots with outcome, goals with assists and strength, penalties, goalkeeper changes, timeouts. No coordinates, no on-ice players.

**Endpoint URL:** `POST https://hockeyallsvenskan.se/api/play-by-play`

**Example request:** `curl -X POST 'https://hockeyallsvenskan.se/api/play-by-play' -H 'Content-Type: application/json' -d '{"statNetGameNumber": 23401, "homeStatNetId": "AIK", "awayStatNetId": "MODO", "scheduledDateTime": "2026-09-18T17:00:00.000Z"}'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `statNetGameNumber` | `statnet_game_number` | body |  | `Y` |  | StatNet game number (statnet_game_number in ha_schedule / ha_game), e.g. 23401. |
| `homeStatNetId` | `home_statnet_id` | body |  | `Y` |  | StatNet team id of the home team, e.g. AIK. |
| `awayStatNetId` | `away_statnet_id` | body |  | `Y` |  | StatNet team id of the away team, e.g. MODO. |
| `scheduledDateTime` | `scheduled_date_time` | body |  | `Y` |  | Scheduled start (scheduled_date_time in ha_schedule), ISO 8601 UTC. Required by the site. |
| `playedDateTime` | `played_date_time` | body |  |  | `Y` | Actual start (played_date_time), ISO 8601 UTC. Optional. |

### Returns {#ha_pbp-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `statnet_game_number` | Int64 | StatNet game number (23401 = 2026/27 game 1); the play-by-play request key. |
| `home_statnet_id` | String | StatNet team id of the home team (AIK, MODO, MIK). |
| `away_statnet_id` | String | StatNet team id of the away team. |
| `period` | Int64 | Period number the event belongs to. |
| `period_label` | String | Period label ('Period 1'). |
| `event_type` | String | Event type: Shot, Goal, Penalty, GoalkeeperEvent or Timeout. |
| `time` | Int64 | Seconds into the period. |
| `is_entering` | Boolean | Goalkeeper event: true when the goalkeeper enters, false when he leaves. |
| `team_name` | String | Team label as shown on the card (the display team code). |
| `team_code` | String | Display code of the team (AIK, MoDo, MORA, NVIF, ÖIK), not the StatNet id. |
| `team_statnet_id` | String | StatNet team id (AIK, MODO, MIK, NYB, OSIK), not the display code. |
| `player_statnet_id` | Int64 | StatNet player id (the same id space as SHL's isa id, verified on one player). |
| `player_first_name` | String | Player: first name of the player. |
| `player_family_name` | String | Player: family name of the player. |
| `player_jersey_number` | Int64 | Player: jersey number of the player. |
| `event_description` | String | Shot outcome, goal strength (EQ), penalty text or timeout note. |
| `running_score` | String | Score after the goal, home-away. |
| `scorer_season_goals` | Int64 | Scorer's season goals after this goal. |
| `is_penalty_shot` | Boolean | Goal scored on a penalty shot. |
| `assist1_name` | String | First assist, by name (no id). |
| `assist1_jersey_today` | Int64 | First assist's jersey number in this game. |
| `assist1_season_assists` | Int64 | First assist's season assists after this goal. |
| `assist2_name` | String | Second assist, by name (no id). |
| `assist2_jersey_today` | Int64 | Second assist's jersey number in this game. |
| `assist2_season_assists` | Int64 | Second assist's season assists after this goal. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#ha_pbp-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (a season before 2005/06, an unknown or pre-2025/26 game, a phase not played). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A value outside its enumerated set, or a field the site requires is missing. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#ha_pbp-example}

```python
ha_pbp(away_statnet_id='MODO', home_statnet_id='AIK', scheduled_date_time='2026-09-18T17:00:00.000Z', statnet_game_number=23401)
```

## ha_games

Every game of a season, played or scheduled (is_played): date, display team codes, score, periods, arena, attendance, shots and power play. No game id.

**Endpoint URL:** `POST https://hockeyallsvenskan.se/api/all-time-games-stats`

**Example request:** `curl -X POST 'https://hockeyallsvenskan.se/api/all-time-games-stats' -H 'Content-Type: application/json' -d '{"season": "2025", "phase": "HA", "team_side": "all", "team": "all", "against_team": "all", "all_results": "all", "game_status": "all", "decided_in": "all"}'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `season` | `season` | body |  |  | `Y` | Season END year (2026 = 2025/26); the site's START-year key is sent as season - 1. Omitted = the season in progress. Stats and standings start in 2005/06 (2006). |
| `phase` | `phase` | body |  |  | `Y` | Game type: HA regular season, HAPlayoff playoffs, HAkval playout (relegation series). |
| `team_side` | `team_side` | body |  |  | `Y` | Home / away split. |
| `team` | `team` | body |  |  | `Y` | Team filter: all, or one team of the site's filter list (values not probed). |
| `against_team` | `against_team` | body |  |  | `Y` | Opponent filter: all, or one team (values not probed). |
| `all_results` | `all_results` | body |  |  | `Y` | Result filter: W win, T tie, L loss. |
| `game_status` | `game_status` | body |  |  | `Y` | Game status filter: played drops the season's unplayed games. |
| `decided_in` | `decided_in` | body |  |  | `Y` | How the game was decided: ft full time, ot overtime, so shootout, wo walkover. |

### Returns {#ha_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season END year (2026 = 2025/26). |
| `date` | String | Game date (YYYY-MM-DD). |
| `home_team_code` | String | Display code of the home team (AIK, MoDo, MORA, NVIF, ÖIK), not the StatNet id. |
| `away_team_code` | String | Display code of the away team, not the StatNet id. |
| `home_score` | Int64 | Home goals, final. |
| `away_score` | Int64 | Away goals, final. |
| `decided_in` | String | How the game was decided (FT, OT, SO); empty for a game not yet played. |
| `is_played` | Boolean | True when the game has been played (decided_in is not empty). |
| `arena` | String | Arena the game is played in, as the games table names it. |
| `attendance` | Int64 | Attendance; null for an unplayed game (0 for a played one means unknown). |
| `home_shots_total` | Int64 | Home team: shots on goal. |
| `home_shots_goal` | Int64 | Home team: goals. |
| `home_shots_efficiency` | Float64 | Home team: shooting percentage (goals per 100 shots); sent as text, 0 or null. |
| `home_powerplay_goals` | Int64 | Home team: power-play goals. |
| `home_powerplay_num_pp` | Int64 | Home team: power plays. |
| `home_powerplay_efficiency` | Float64 | Home team: power-play percentage; sent as text, 0 or null. |
| `home_expulsions_in_minutes` | Int64 | Home team: penalty minutes. |
| `away_shots_total` | Int64 | Away team: shots on goal. |
| `away_shots_goal` | Int64 | Away team: goals. |
| `away_shots_efficiency` | Float64 | Away team: shooting percentage (goals per 100 shots); sent as text, 0 or null. |
| `away_powerplay_goals` | Int64 | Away team: power-play goals. |
| `away_powerplay_num_pp` | Int64 | Away team: power plays. |
| `away_powerplay_efficiency` | Float64 | Away team: power-play percentage; sent as text, 0 or null. |
| `away_expulsions_in_minutes` | Int64 | Away team: penalty minutes. |
| `period_scores` | String | Goals per period, JSON list of {Home, Away}; null for an unplayed game. |
| `most_points` | String | Top point scorers of the game, JSON list of {player, team (StatNet id), points}. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#ha_games-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (a season before 2005/06, an unknown or pre-2025/26 game, a phase not played). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A value outside its enumerated set, or a field the site requires is missing. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#ha_games-example}

```python
ha_games(season=2026)
```

## ha_standings

Standings of a season. Wins and losses are REGULATION only; overtime counts include shootouts.

**Endpoint URL:** `POST https://hockeyallsvenskan.se/api/league-standings-all-time`

**Example request:** `curl -X POST 'https://hockeyallsvenskan.se/api/league-standings-all-time' -H 'Content-Type: application/json' -d '{"season": "2025", "phase": "HA", "location": "all"}'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `season` | `season` | body |  |  | `Y` | Season END year (2026 = 2025/26); the site's START-year key is sent as season - 1. Omitted = the season in progress. Stats and standings start in 2005/06 (2006). |
| `phase` | `phase` | body |  |  | `Y` | Game type: HA regular season, HAPlayoff playoffs, HAkval playout (relegation series). |
| `location` | `location` | body |  |  | `Y` | Home / away standings split. |

### Returns {#ha_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season END year (2026 = 2025/26). |
| `rank` | Int64 | Rank in the table. |
| `team_code_mixed` | String | Upstream standings teamCode: the StatNet id for some clubs (MODO), the display code for others (MORA, NVIF, ÖIK). Join on team_code instead. |
| `team_code` | String | Display code of the team (AIK, MoDo, MORA, NVIF, ÖIK), not the StatNet id. |
| `games_played` | Int64 | Games played in the season and phase. |
| `regulation_wins` | Int64 | Regulation wins only (the team table's wins_total also counts OT and shootout wins). |
| `regulation_losses` | Int64 | Regulation losses only. |
| `ties` | Int64 | Ties (0 in the overtime era). |
| `overtime_and_shootout_wins` | Int64 | Overtime wins, shootout wins included. |
| `overtime_and_shootout_losses` | Int64 | Overtime losses, shootout losses included. |
| `shootouts_wins` | Int64 | Shootout wins as sent (0 in every sampled row: they are counted in overtime_and_shootout_wins). |
| `shootouts_losses` | Int64 | Shootout losses as sent (0 in every sampled row). |
| `goals` | Int64 | Goals scored by the team (goals for). |
| `goals_against` | Int64 | Goals conceded by the team. |
| `goal_difference` | Int64 | Goal difference. |
| `total_points` | Int64 | Standings points of the team. |
| `power_play_perc` | Float64 | Power-play percentage; empty-string placeholder '' in every sample. |
| `penalty_kill_perc` | Float64 | Penalty-kill percentage; empty-string placeholder '' in every sample. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#ha_standings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (a season before 2005/06, an unknown or pre-2025/26 game, a phase not played). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A value outside its enumerated set, or a field the site requires is missing. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#ha_standings-example}

```python
ha_standings(season=2026)
```

## ha_skater_stats

Skater stats of a season, one category (column set) per call. Names and StatNet team ids, no player ids.

**Endpoint URL:** `POST https://hockeyallsvenskan.se/api/all-time-skaters-stats`

**Example request:** `curl -X POST 'https://hockeyallsvenskan.se/api/all-time-skaters-stats' -H 'Content-Type: application/json' -d '{"season": "2025", "phase": "HA", "categories": "summary", "position": "all", "ages": "all", "team_side": "all", "team": "all", "against_team": "all", "all_results": "all", "game_status": "all", "decided_in": "all"}'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `season` | `season` | body |  |  | `Y` | Season END year (2026 = 2025/26); the site's START-year key is sent as season - 1. Omitted = the season in progress. Stats and standings start in 2005/06 (2006). |
| `phase` | `phase` | body |  |  | `Y` | Game type: HA regular season, HAPlayoff playoffs, HAkval playout (relegation series). |
| `categories` | `category` | body |  |  | `Y` | Stats category (column set), one of summary, points, goals, time_on_ice, power_play, penalty_kill, equal_strength, penalties, plus_minus, faceoffs, offensive, defensive, shootouts, corsi_fenwick_pdo. Anything else answers zero rows. |
| `position` | `position` | body |  |  | `Y` | Position filter (back = defence). |
| `ages` | `ages` | body |  |  | `Y` | Age-group filter. |
| `team_side` | `team_side` | body |  |  | `Y` | Home / away split. |
| `team` | `team` | body |  |  | `Y` | Team filter: all, or one team of the site's filter list (values not probed). |
| `against_team` | `against_team` | body |  |  | `Y` | Opponent filter: all, or one team (values not probed). |
| `all_results` | `all_results` | body |  |  | `Y` | Result filter: W win, T tie, L loss. |
| `game_status` | `game_status` | body |  |  | `Y` | Game status filter: played drops the season's unplayed games. |
| `decided_in` | `decided_in` | body |  |  | `Y` | How the game was decided: ft full time, ot overtime, so shootout, wo walkover. |

### Returns {#ha_skater_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season END year (2026 = 2025/26). |
| `rank` | Int64 | Rank in the table. |
| `player_name` | String | Player name (the table carries no player id). |
| `team_statnet_id` | String | StatNet team id (AIK, MODO, MIK, NYB, OSIK), not the display code. |
| `gp` | Int64 | Games played in the season and phase. |
| `tp` | Int64 | Points (goals + assists). |
| `g` | Int64 | Goals scored by the skater. |
| `a` | Int64 | Assists credited to the skater. |
| `pim` | Int64 | Penalty minutes. |
| `gwg` | Int64 | Game-winning goals. |
| `ppg` | Int64 | Power-play goals. |
| `sog` | Int64 | Shots on goal by the skater. |
| `hits` | Int64 | Hits (0 in seasons before tracking). |
| `bks` | Int64 | Blocked shots (0 in seasons before tracking). |
| `plus` | Int64 | Even-strength / short-handed goals for while on ice (plus count). |
| `minus` | Int64 | Goals against while on ice (minus count). |
| `plus_minus` | Int64 | Plus/minus: the Plus count less the Minus count. |
| `toi_gp` | String | Average time on ice per game, mm:ss string ('0:00' = not tracked, e.g. 2010). |
| `tppg` | Float64 | Points per game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#ha_skater_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (a season before 2005/06, an unknown or pre-2025/26 game, a phase not played). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A value outside its enumerated set, or a field the site requires is missing. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#ha_skater_stats-example}

```python
ha_skater_stats(category='summary', season=2026)
```

## ha_goalie_stats

Goalkeeper stats of a season, one category per call. Names and StatNet team ids, no player ids.

**Endpoint URL:** `POST https://hockeyallsvenskan.se/api/all-time-goalkeepers-stats`

**Example request:** `curl -X POST 'https://hockeyallsvenskan.se/api/all-time-goalkeepers-stats' -H 'Content-Type: application/json' -d '{"season": "2025", "phase": "HA", "categories": "summary", "team_side": "all", "team": "all", "against_team": "all", "all_results": "all", "game_status": "all", "decided_in": "all"}'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `season` | `season` | body |  |  | `Y` | Season END year (2026 = 2025/26); the site's START-year key is sent as season - 1. Omitted = the season in progress. Stats and standings start in 2005/06 (2006). |
| `phase` | `phase` | body |  |  | `Y` | Game type: HA regular season, HAPlayoff playoffs, HAkval playout (relegation series). |
| `categories` | `category` | body |  |  | `Y` | Stats category (column set), one of summary, results, game_strength, defensive, shootouts. Anything else answers zero rows. |
| `team_side` | `team_side` | body |  |  | `Y` | Home / away split. |
| `team` | `team` | body |  |  | `Y` | Team filter: all, or one team of the site's filter list (values not probed). |
| `against_team` | `against_team` | body |  |  | `Y` | Opponent filter: all, or one team (values not probed). |
| `all_results` | `all_results` | body |  |  | `Y` | Result filter: W win, T tie, L loss. |
| `game_status` | `game_status` | body |  |  | `Y` | Game status filter: played drops the season's unplayed games. |
| `decided_in` | `decided_in` | body |  |  | `Y` | How the game was decided: ft full time, ot overtime, so shootout, wo walkover. |

### Returns {#ha_goalie_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season END year (2026 = 2025/26). |
| `rank` | Int64 | Rank in the table. |
| `player_name` | String | Player name (the table carries no player id). |
| `team_statnet_id` | String | StatNet team id (AIK, MODO, MIK, NYB, OSIK), not the display code. |
| `svs` | Int64 | Saves made by the goalkeeper. |
| `svs_perc` | Float64 | Save percentage ('92.61'). |
| `gp` | Int64 | Games played in the season and phase. |
| `gpi` | Int64 | Games played in (appeared). |
| `ga` | Int64 | Goals against: goals allowed in the season and phase. |
| `gaa` | Float64 | Goals-against average. |
| `w` | Int64 | Wins credited to the goalkeeper. |
| `t` | Int64 | Ties credited to the goalkeeper (0 in the overtime era). |
| `l` | Int64 | Losses credited to the goalkeeper. |
| `so` | Int64 | Shutouts recorded by the goalkeeper. |
| `mip` | String | Minutes played, total mm:ss string ('1726:51'). |
| `valid_ranking` | Int64 | 1 when the goalkeeper qualifies for the ranked list (minimum games), else 0. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#ha_goalie_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (a season before 2005/06, an unknown or pre-2025/26 game, a phase not played). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A value outside its enumerated set, or a field the site requires is missing. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#ha_goalie_stats-example}

```python
ha_goalie_stats(category='summary', season=2026)
```

## ha_team_stats

Team stats of a season, one category per call. W / L are totals including overtime and shootout (wins_total / losses_total).

**Endpoint URL:** `POST https://hockeyallsvenskan.se/api/all-time-teams-stats`

**Example request:** `curl -X POST 'https://hockeyallsvenskan.se/api/all-time-teams-stats' -H 'Content-Type: application/json' -d '{"season": "2025", "phase": "HA", "categories": "results", "team_side": "all", "team": "all", "against_team": "all", "all_results": "all", "game_status": "all", "decided_in": "all"}'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `season` | `season` | body |  |  | `Y` | Season END year (2026 = 2025/26); the site's START-year key is sent as season - 1. Omitted = the season in progress. Stats and standings start in 2005/06 (2006). |
| `phase` | `phase` | body |  |  | `Y` | Game type: HA regular season, HAPlayoff playoffs, HAkval playout (relegation series). |
| `categories` | `category` | body |  |  | `Y` | Stats category (column set), one of results, goals, power_play, penalty_kill, penalties, offensive, defensive, faceoffs, shootouts, corsi_fenwick_pdo, stoppage_reasons, attendance. Anything else answers zero rows. |
| `team_side` | `team_side` | body |  |  | `Y` | Home / away split. |
| `team` | `team` | body |  |  | `Y` | Team filter: all, or one team of the site's filter list (values not probed). |
| `against_team` | `against_team` | body |  |  | `Y` | Opponent filter: all, or one team (values not probed). |
| `all_results` | `all_results` | body |  |  | `Y` | Result filter: W win, T tie, L loss. |
| `game_status` | `game_status` | body |  |  | `Y` | Game status filter: played drops the season's unplayed games. |
| `decided_in` | `decided_in` | body |  |  | `Y` | How the game was decided: ft full time, ot overtime, so shootout, wo walkover. |

### Returns {#ha_team_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season END year (2026 = 2025/26). |
| `rank` | Int64 | Rank in the table. |
| `team_code` | String | Display code of the team (AIK, MoDo, MORA, NVIF, ÖIK), not the StatNet id. |
| `gp` | Int64 | Games played in the season and phase. |
| `wins_total` | Int64 | Wins in total, overtime and shootout wins included (otw and sow are subsets). |
| `losses_total` | Int64 | Losses in total, overtime and shootout losses included (otl and sol are subsets). |
| `leading` | Int64 | Games or situations leading (StatNet 'Leading'; meaning unverified). |
| `trailing` | Int64 | Games or situations trailing (StatNet 'Trailing'; meaning unverified). |
| `g` | Int64 | Goals scored by the team. |
| `ga` | Int64 | Goals against: goals allowed in the season and phase. |
| `gfg` | Int64 | Games in which the team scored the first goal (inferred from the label; unverified). |
| `otw` | Int64 | Overtime wins (a subset of wins_total). |
| `otl` | Int64 | Overtime losses. |
| `sow` | Int64 | Shootout wins (a subset of wins_total). |
| `sol` | Int64 | Shootout losses. |
| `p_gp` | Float64 | Points per game. |
| `gpg` | Float64 | Goals for per game (number). |
| `gapg` | Float64 | Goals against per game (number). |
| `points` | Int64 | Standings points: 3 per regulation win, 2 per overtime/shootout win, 1 per overtime/shootout loss (2025 IFB: 102 + 14 + 3 = 119). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#ha_team_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (a season before 2005/06, an unknown or pre-2025/26 game, a phase not played). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A value outside its enumerated set, or a field the site requires is missing. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#ha_team_stats-example}

```python
ha_team_stats(category='results', season=2026)
```

## ha_teams

Teams of a season with both team keys: StatNet team id <-> display code (from the stats tables' teams list).

**Endpoint URL:** `POST https://hockeyallsvenskan.se/api/all-time-goalkeepers-stats`

**Example request:** `curl -X POST 'https://hockeyallsvenskan.se/api/all-time-goalkeepers-stats' -H 'Content-Type: application/json' -d '{"season": "2025", "phase": "HA"}'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `season` | `season` | body |  |  | `Y` | Season END year (2026 = 2025/26); the site's START-year key is sent as season - 1. Omitted = the season in progress. Stats and standings start in 2005/06 (2006). |
| `phase` | `phase` | body |  |  | `Y` | Game type: HA regular season, HAPlayoff playoffs, HAkval playout (relegation series). |

### Returns {#ha_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season END year (2026 = 2025/26). |
| `team_statnet_id` | String | StatNet team id (AIK, MODO, MIK, NYB, OSIK), not the display code. |
| `team_code` | String | Display code of the team (AIK, MoDo, MORA, NVIF, ÖIK), not the StatNet id. |
| `name` | String | Short club name (the teams list's name). |
| `full_name` | String | Full club name (the teams list's fullName). |
| `logos_icon` | String | Logo: icon-size logo URL. |
| `logos_large` | String | Logo: large logo URL. |
| `logos_small` | String | Logo: small logo rendition (500 px). |
| `logos_medium` | String | Logo: medium logo URL. |
| `logos_xlarge` | String | Logo: extra-large logo URL. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#ha_teams-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (a season before 2005/06, an unknown or pre-2025/26 game, a phase not played). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A value outside its enumerated set, or a field the site requires is missing. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#ha_teams-example}

```python
ha_teams(season=2026)
```

## ha_leaders

Current-season top 25 of one metric, with the StatNet player id (the only stats route that has one).

**Endpoint URL:** `POST https://hockeyallsvenskan.se/api/player-leaderboard`

**Example request:** `curl -X POST 'https://hockeyallsvenskan.se/api/player-leaderboard' -H 'Content-Type: application/json' -d '{"metric": "TP", "phase": "HA"}'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `metric` | `metric` | body |  | `Y` |  | Leaderboard metric. Seen on the site: TP, G, A, PlusMinus, Hits, FOPerc, TOI/GP (skaters). |
| `phase` | `phase` | body |  |  | `Y` | Game type: HA regular season, HAPlayoff playoffs, HAkval playout (relegation series). |
| `team` | `team` | body |  |  | `Y` | Team filter; omitted = the whole league (values not probed). |
| `isGoalkeeper` | `is_goalkeeper` | body |  |  | `Y` | True for a goalkeeper metric. |

### Returns {#ha_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `rank` | Int64 | Rank in the table. |
| `player_name` | String | Player name (the table carries no player id). |
| `team_code` | String | Display code of the team (AIK, MoDo, MORA, NVIF, ÖIK), not the StatNet id. |
| `team_statnet_id` | String | StatNet team id (AIK, MODO, MIK, NYB, OSIK), not the display code. |
| `jersey_number` | Int64 | Jersey number of the player. |
| `position` | String | Playing position: F forward, D defence (G goalkeeper on goalie metrics). |
| `metric` | String | Metric code (echo of the request metric). |
| `metric_value` | String | Value of the metric (a count, a percentage or a clock, as text). |
| `player_statnet_id` | Int64 | StatNet player id (the same id space as SHL's isa id, verified on one player). |
| `first_name` | String | First name of the player. |
| `family_name` | String | Family name of the player. |
| `slug` | String | Player slug; joins ha_players. |
| `headshot` | String | Headshot URL (medium rendition). |
| `headshot_small` | String | Small headshot URL. |
| `headshot_medium` | String | Medium headshot URL. |
| `team_logo` | String | Logo URL of the player's team. |
| `team_name` | String | Team label as shown on the card (the display team code). |
| `team_main_color` | String | Team primary colour (hex). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#ha_leaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (a season before 2005/06, an unknown or pre-2025/26 game, a phase not played). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A value outside its enumerated set, or a field the site requires is missing. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#ha_leaders-example}

```python
ha_leaders(metric='TP')
```

## ha_players

One team's players from the CMS directory: CMS ids, position, jersey, birth date, size, headshots. One team per call (league-wide paging is unreliable).

**Endpoint URL:** `POST https://hockeyallsvenskan.se/api/all-players`

**Example request:** `curl -X POST 'https://hockeyallsvenskan.se/api/all-players' -H 'Content-Type: application/json' -d '{"teamShortName": "AIK", "orderBy": "familyName"}'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `teamShortName` | `team` | body |  | `Y` |  | Team display code (CMS short name: AIK, MORA, ÖIK, ...). Required: the directory pages unreliably league-wide. |
| `orderBy` | `order_by` | body |  |  | `Y` | Sort: familyName or firstName. |
| `searchQuery` | `search_query` | body |  |  | `Y` | Free-text name search; omitted = everyone. |

### Returns {#ha_players-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `family_name` | String | Family name of the player. |
| `first_name` | String | First name of the player. |
| `player_cms_id` | Int64 | CMS (Strapi) record id of the player (not the StatNet id). |
| `player_document_id` | String | CMS (Strapi) document id of the player. |
| `headshots_large` | String | Headshot URL (large rendition). |
| `headshots_small` | String | Headshot URL (small rendition). |
| `headshots_thumb` | String | Headshot URL (thumb rendition). |
| `headshots_medium` | String | Headshot URL (medium rendition). |
| `position_code` | String | Playing position code (CE centre, LW left wing, ...); positionName spells it out in English. |
| `jersey_number` | Int64 | Jersey number of the player. |
| `birth_date` | String | Birth date, YYYY-MM-DD. |
| `country` | String | Nationality, IOC/ISO alpha-3 code (SWE, NOR). |
| `height` | Int64 | Player height in centimetres. |
| `weight` | Int64 | Player weight in kilograms. |
| `shoots` | String | Shoots / catches: L or R. |
| `youth_team` | String | Youth (development) club. |
| `is_rookie` | Boolean | True when the CMS directory flags the player as a rookie. |
| `team_player` | String | Card label 'TEAM \\| POS JERSEY \\| Family name'. |
| `slug` | String | Player slug; joins ha_leaders. |
| `position_name` | String | Playing position, English label (Center, Left Wing, ...). |
| `team_cms_id` | Int64 | CMS (Strapi) record id of the team (81 = AIK). |
| `team_document_id` | String | CMS (Strapi) document id of the team. |
| `team_code` | String | Display code of the team (AIK, MoDo, MORA, NVIF, ÖIK), not the StatNet id. |
| `team_main_color_hex` | String | Primary colour of the team (hex). |
| `team_name` | String | Team label as shown on the card (the display team code). |
| `team_logo_url` | String | Logo URL of the team. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#ha_players-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (a season before 2005/06, an unknown or pre-2025/26 game, a phase not played). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A value outside its enumerated set, or a field the site requires is missing. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#ha_players-example}

```python
ha_players(team='AIK')
```
