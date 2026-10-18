---
title: HA — HockeyAllsvenskan site API (hockeyallsvenskan.se, Sweden)
sidebar_label: HockeyAllsvenskan site API (hockeyallsvenskan.se, Sweden)
description: "HA — HockeyAllsvenskan site API (hockeyallsvenskan.se, Sweden) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
toc_max_heading_level: 2
---
# HA — HockeyAllsvenskan site API (hockeyallsvenskan.se, Sweden)

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
| `season` | integer | Season END year (2026 = 2025/26). |
| `game_cms_id` | integer | CMS (Strapi) record id of the game. |
| `game_document_id` | character | CMS (Strapi) document id of the game. |
| `cms_season` | character | CMS season key as sent (season_2026 = 2026/27: a START year). |
| `statnet_game_number` | integer | StatNet game number (23401 = 2026/27 game 1); the play-by-play request key. |
| `slug` | character | Game slug yyyymmdd-home-away (lower-case CMS short names); the /api/game?slug= key. |
| `round` | integer | Round number of the game in the season schedule. |
| `scheduled_date_time` | character | Scheduled face-off time (ISO 8601 UTC). |
| `venue` | character | Arena name on the CMS game record. |
| `attendance` | integer | Attendance; null for an unplayed game (0 for a played one means unknown). |
| `current_period` | character | Live period while the game runs; empty string '' before and after. |
| `current_period_start` | character | Start time of the live period; null outside a live game. |
| `home_score` | integer | Home goals, final. |
| `away_score` | integer | Away goals, final. |
| `ended_date_time` | character | End time (ISO 8601 UTC). |
| `decided_in` | character | How the game was decided (FT, OT, SO); empty for a game not yet played. |
| `home_score_p1` | integer | Home goals in period 1. |
| `away_score_p1` | integer | Away goals in period 1. |
| `home_score_p2` | integer | Home goals in period 2. |
| `away_score_p2` | integer | Away goals in period 2. |
| `home_score_p3` | integer | Home goals in period 3. |
| `away_score_p3` | integer | Away goals in period 3. |
| `home_ot_score` | integer | Home overtime goals; null when the game had no overtime. |
| `away_ot_score` | integer | Away overtime goals; null when the game had no overtime. |
| `home_so_score` | integer | Home shootout goals; null when the game had no shootout. |
| `away_so_score` | integer | Away shootout goals; null when the game had no shootout. |
| `game_type` | character | Game type (phase): HA regular season, HAPlayoff playoffs, HAkval playout. |
| `play_off_game` | character | Game of the playoff series (1, 2, ...); null outside the playoffs. |
| `play_off_game_level` | character | Playoff round label. |
| `played_date_time` | character | Actual start time (ISO 8601 UTC). |
| `is_completed` | logical | true once the game is final. |
| `home_statnet_id` | character | StatNet team id of the home team (AIK, MODO, MIK). |
| `away_statnet_id` | character | StatNet team id of the away team. |
| `created_at` | character | CMS creation time of the record (ISO 8601 UTC). |
| `updated_at` | character | CMS update time of the record (ISO 8601 UTC). |
| `published_at` | character | CMS publish time of the record (ISO 8601 UTC). |
| `home_team_cms_id` | integer | CMS (Strapi) record id of the home team (81 = AIK). |
| `home_team_document_id` | character | Home team: CMS (Strapi) document id (string; stable across drafts and locales). |
| `home_team_name` | character | Home team: short club name (the teams list's name). |
| `home_team_code` | character | Display code of the home team (AIK, MoDo, MORA, NVIF, ÖIK), not the StatNet id. |
| `home_team_main_color_hex` | character | Home team: primary colour (hex). |
| `home_team_alternate_color_hex` | character | Home team: secondary colour (hex). |
| `home_team_team_arena` | character | Home team: home arena. |
| `home_team_arena_address` | character | Home team: postal address of the arena (multi-line, newline-separated). |
| `home_team_arena_latitude` | numeric | Home team: arena latitude, decimal degrees. |
| `home_team_arena_longitude` | numeric | Home team: arena longitude, decimal degrees. |
| `home_team_arena_telephone` | character | Home team: arena telephone contact. |
| `home_team_tickets_provider_url` | character | Home team: ticket shop URL. |
| `home_team_club_mate_lottery_url` | character | Home team: club lottery URL. |
| `home_team_created_at` | character | Home team: CMS creation time of the record (ISO 8601 UTC). |
| `home_team_updated_at` | character | Home team: CMS update time of the record (ISO 8601 UTC). |
| `home_team_published_at` | character | Home team: CMS publish time of the record (ISO 8601 UTC). |
| `home_team_logo_url` | character | Logo URL of the home team. |
| `away_team_cms_id` | integer | CMS (Strapi) record id of the away team. |
| `away_team_document_id` | character | Away team: CMS (Strapi) document id (string; stable across drafts and locales). |
| `away_team_name` | character | Away team: short club name (the teams list's name). |
| `away_team_code` | character | Display code of the away team, not the StatNet id. |
| `away_team_main_color_hex` | character | Away team: primary colour (hex). |
| `away_team_alternate_color_hex` | character | Away team: secondary colour (hex). |
| `away_team_team_arena` | character | Away team: home arena. |
| `away_team_arena_address` | character | Away team: postal address of the arena (multi-line, newline-separated). |
| `away_team_arena_latitude` | numeric | Away team: arena latitude, decimal degrees. |
| `away_team_arena_longitude` | numeric | Away team: arena longitude, decimal degrees. |
| `away_team_arena_telephone` | character | Away team: arena telephone contact. |
| `away_team_tickets_provider_url` | character | Away team: ticket shop URL. |
| `away_team_club_mate_lottery_url` | character | Away team: club lottery URL. |
| `away_team_created_at` | character | Away team: CMS creation time of the record (ISO 8601 UTC). |
| `away_team_updated_at` | character | Away team: CMS update time of the record (ISO 8601 UTC). |
| `away_team_published_at` | character | Away team: CMS publish time of the record (ISO 8601 UTC). |
| `away_team_logo_url` | character | Logo URL of the away team. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#ha_schedule-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the site has nothing for the request (a season before 2005/06, an unknown or pre-2025/26 game, a phase not played). |
| `ValueError` | a value outside its enumerated set, or a field the site requires is missing. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#ha_schedule-example}

```python
ha_schedule()
```

_Last validated n/a._

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
| `season` | integer | Season END year (2026 = 2025/26). |
| `game_cms_id` | integer | CMS (Strapi) record id of the game. |
| `game_document_id` | character | CMS (Strapi) document id of the game. |
| `cms_season` | character | CMS season key as sent (season_2026 = 2026/27: a START year). |
| `statnet_game_number` | integer | StatNet game number (23401 = 2026/27 game 1); the play-by-play request key. |
| `slug` | character | Game slug yyyymmdd-home-away (lower-case CMS short names); the /api/game?slug= key. |
| `round` | integer | Round number of the game in the season schedule. |
| `scheduled_date_time` | character | Scheduled face-off time (ISO 8601 UTC). |
| `venue` | character | Arena name on the CMS game record. |
| `attendance` | integer | Attendance; null for an unplayed game (0 for a played one means unknown). |
| `current_period` | character | Live period while the game runs; empty string '' before and after. |
| `current_period_start` | character | Start time of the live period; null outside a live game. |
| `home_score` | integer | Home goals, final. |
| `away_score` | integer | Away goals, final. |
| `ended_date_time` | character | End time (ISO 8601 UTC). |
| `decided_in` | character | How the game was decided (FT, OT, SO); empty for a game not yet played. |
| `home_score_p1` | integer | Home goals in period 1. |
| `away_score_p1` | integer | Away goals in period 1. |
| `home_score_p2` | integer | Home goals in period 2. |
| `away_score_p2` | integer | Away goals in period 2. |
| `home_score_p3` | integer | Home goals in period 3. |
| `away_score_p3` | integer | Away goals in period 3. |
| `home_ot_score` | integer | Home overtime goals; null when the game had no overtime. |
| `away_ot_score` | integer | Away overtime goals; null when the game had no overtime. |
| `home_so_score` | integer | Home shootout goals; null when the game had no shootout. |
| `away_so_score` | integer | Away shootout goals; null when the game had no shootout. |
| `game_type` | character | Game type (phase): HA regular season, HAPlayoff playoffs, HAkval playout. |
| `play_off_game` | character | Game of the playoff series (1, 2, ...); null outside the playoffs. |
| `play_off_game_level` | character | Playoff round label. |
| `played_date_time` | character | Actual start time (ISO 8601 UTC). |
| `is_completed` | logical | true once the game is final. |
| `home_statnet_id` | character | StatNet team id of the home team (AIK, MODO, MIK). |
| `away_statnet_id` | character | StatNet team id of the away team. |
| `created_at` | character | CMS creation time of the record (ISO 8601 UTC). |
| `updated_at` | character | CMS update time of the record (ISO 8601 UTC). |
| `published_at` | character | CMS publish time of the record (ISO 8601 UTC). |
| `home_team_cms_id` | integer | CMS (Strapi) record id of the home team (81 = AIK). |
| `home_team_document_id` | character | Home team: CMS (Strapi) document id (string; stable across drafts and locales). |
| `home_team_name` | character | Home team: short club name (the teams list's name). |
| `home_team_code` | character | Display code of the home team (AIK, MoDo, MORA, NVIF, ÖIK), not the StatNet id. |
| `home_team_main_color_hex` | character | Home team: primary colour (hex). |
| `home_team_alternate_color_hex` | character | Home team: secondary colour (hex). |
| `home_team_team_arena` | character | Home team: home arena. |
| `home_team_arena_address` | character | Home team: postal address of the arena (multi-line, newline-separated). |
| `home_team_arena_latitude` | numeric | Home team: arena latitude, decimal degrees. |
| `home_team_arena_longitude` | numeric | Home team: arena longitude, decimal degrees. |
| `home_team_arena_telephone` | character | Home team: arena telephone contact. |
| `home_team_tickets_provider_url` | character | Home team: ticket shop URL. |
| `home_team_club_mate_lottery_url` | character | Home team: club lottery URL. |
| `home_team_created_at` | character | Home team: CMS creation time of the record (ISO 8601 UTC). |
| `home_team_updated_at` | character | Home team: CMS update time of the record (ISO 8601 UTC). |
| `home_team_published_at` | character | Home team: CMS publish time of the record (ISO 8601 UTC). |
| `home_team_logo_url` | character | Logo URL of the home team. |
| `away_team_cms_id` | integer | CMS (Strapi) record id of the away team. |
| `away_team_document_id` | character | Away team: CMS (Strapi) document id (string; stable across drafts and locales). |
| `away_team_name` | character | Away team: short club name (the teams list's name). |
| `away_team_code` | character | Display code of the away team, not the StatNet id. |
| `away_team_main_color_hex` | character | Away team: primary colour (hex). |
| `away_team_alternate_color_hex` | character | Away team: secondary colour (hex). |
| `away_team_team_arena` | character | Away team: home arena. |
| `away_team_arena_address` | character | Away team: postal address of the arena (multi-line, newline-separated). |
| `away_team_arena_latitude` | numeric | Away team: arena latitude, decimal degrees. |
| `away_team_arena_longitude` | numeric | Away team: arena longitude, decimal degrees. |
| `away_team_arena_telephone` | character | Away team: arena telephone contact. |
| `away_team_tickets_provider_url` | character | Away team: ticket shop URL. |
| `away_team_club_mate_lottery_url` | character | Away team: club lottery URL. |
| `away_team_created_at` | character | Away team: CMS creation time of the record (ISO 8601 UTC). |
| `away_team_updated_at` | character | Away team: CMS update time of the record (ISO 8601 UTC). |
| `away_team_published_at` | character | Away team: CMS publish time of the record (ISO 8601 UTC). |
| `away_team_logo_url` | character | Logo URL of the away team. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#ha_game-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the site has nothing for the request (a season before 2005/06, an unknown or pre-2025/26 game, a phase not played). |
| `ValueError` | a value outside its enumerated set, or a field the site requires is missing. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#ha_game-example}

```python
ha_game(slug='20260918-aik-modo')
```

_Last validated n/a._

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
| `statnet_game_number` | integer | StatNet game number (23401 = 2026/27 game 1); the play-by-play request key. |
| `home_statnet_id` | character | StatNet team id of the home team (AIK, MODO, MIK). |
| `away_statnet_id` | character | StatNet team id of the away team. |
| `period` | integer | Period number the event belongs to. |
| `period_label` | character | Period label ('Period 1'). |
| `event_type` | character | Event type: Shot, Goal, Penalty, GoalkeeperEvent or Timeout. |
| `time` | integer | Seconds into the period. |
| `is_entering` | logical | Goalkeeper event: true when the goalkeeper enters, false when he leaves. |
| `team_name` | character | Team label as shown on the card (the display team code). |
| `team_code` | character | Display code of the team (AIK, MoDo, MORA, NVIF, ÖIK), not the StatNet id. |
| `team_statnet_id` | character | StatNet team id (AIK, MODO, MIK, NYB, OSIK), not the display code. |
| `player_statnet_id` | integer | StatNet player id (the same id space as SHL's isa id, verified on one player). |
| `player_first_name` | character | Player: first name of the player. |
| `player_family_name` | character | Player: family name of the player. |
| `player_jersey_number` | integer | Player: jersey number of the player. |
| `event_description` | character | Shot outcome, goal strength (EQ), penalty text or timeout note. |
| `running_score` | character | Score after the goal, home-away. |
| `scorer_season_goals` | integer | Scorer's season goals after this goal. |
| `is_penalty_shot` | logical | Goal scored on a penalty shot. |
| `assist1_name` | character | First assist, by name (no id). |
| `assist1_jersey_today` | integer | First assist's jersey number in this game. |
| `assist1_season_assists` | integer | First assist's season assists after this goal. |
| `assist2_name` | character | Second assist, by name (no id). |
| `assist2_jersey_today` | integer | Second assist's jersey number in this game. |
| `assist2_season_assists` | integer | Second assist's season assists after this goal. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#ha_pbp-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the site has nothing for the request (a season before 2005/06, an unknown or pre-2025/26 game, a phase not played). |
| `ValueError` | a value outside its enumerated set, or a field the site requires is missing. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#ha_pbp-example}

```python
ha_pbp(away_statnet_id='MODO', home_statnet_id='AIK', scheduled_date_time='2026-09-18T17:00:00.000Z', statnet_game_number=23401)
```

_Last validated n/a._

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
| `season` | integer | Season END year (2026 = 2025/26). |
| `date` | character | Game date (YYYY-MM-DD). |
| `home_team_code` | character | Display code of the home team (AIK, MoDo, MORA, NVIF, ÖIK), not the StatNet id. |
| `away_team_code` | character | Display code of the away team, not the StatNet id. |
| `home_score` | integer | Home goals, final. |
| `away_score` | integer | Away goals, final. |
| `decided_in` | character | How the game was decided (FT, OT, SO); empty for a game not yet played. |
| `is_played` | logical | True when the game has been played (decided_in is not empty). |
| `arena` | character | Arena the game is played in, as the games table names it. |
| `attendance` | integer | Attendance; null for an unplayed game (0 for a played one means unknown). |
| `home_shots_total` | integer | Home team: shots on goal. |
| `home_shots_goal` | integer | Home team: goals. |
| `home_shots_efficiency` | numeric | Home team: shooting percentage (goals per 100 shots); sent as text, 0 or null. |
| `home_powerplay_goals` | integer | Home team: power-play goals. |
| `home_powerplay_num_pp` | integer | Home team: power plays. |
| `home_powerplay_efficiency` | numeric | Home team: power-play percentage; sent as text, 0 or null. |
| `home_expulsions_in_minutes` | integer | Home team: penalty minutes. |
| `away_shots_total` | integer | Away team: shots on goal. |
| `away_shots_goal` | integer | Away team: goals. |
| `away_shots_efficiency` | numeric | Away team: shooting percentage (goals per 100 shots); sent as text, 0 or null. |
| `away_powerplay_goals` | integer | Away team: power-play goals. |
| `away_powerplay_num_pp` | integer | Away team: power plays. |
| `away_powerplay_efficiency` | numeric | Away team: power-play percentage; sent as text, 0 or null. |
| `away_expulsions_in_minutes` | integer | Away team: penalty minutes. |
| `period_scores` | character | Goals per period, JSON list of {Home, Away}; null for an unplayed game. |
| `most_points` | character | Top point scorers of the game, JSON list of {player, team (StatNet id), points}. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#ha_games-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the site has nothing for the request (a season before 2005/06, an unknown or pre-2025/26 game, a phase not played). |
| `ValueError` | a value outside its enumerated set, or a field the site requires is missing. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#ha_games-example}

```python
ha_games(season=2026)
```

_Last validated n/a._

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
| `season` | integer | Season END year (2026 = 2025/26). |
| `rank` | integer | Rank in the table. |
| `team_code_mixed` | character | Upstream standings teamCode: the StatNet id for some clubs (MODO), the display code for others (MORA, NVIF, ÖIK). Join on team_code instead. |
| `team_code` | character | Display code of the team (AIK, MoDo, MORA, NVIF, ÖIK), not the StatNet id. |
| `games_played` | integer | Games played in the season and phase. |
| `regulation_wins` | integer | Regulation wins only (the team table's wins_total also counts OT and shootout wins). |
| `regulation_losses` | integer | Regulation losses only. |
| `ties` | integer | Ties (0 in the overtime era). |
| `overtime_and_shootout_wins` | integer | Overtime wins, shootout wins included. |
| `overtime_and_shootout_losses` | integer | Overtime losses, shootout losses included. |
| `shootouts_wins` | integer | Shootout wins as sent (0 in every sampled row: they are counted in overtime_and_shootout_wins). |
| `shootouts_losses` | integer | Shootout losses as sent (0 in every sampled row). |
| `goals` | integer | Goals scored by the team (goals for). |
| `goals_against` | integer | Goals conceded by the team. |
| `goal_difference` | integer | Goal difference. |
| `total_points` | integer | Standings points of the team. |
| `power_play_perc` | numeric | Power-play percentage; empty-string placeholder '' in every sample. |
| `penalty_kill_perc` | numeric | Penalty-kill percentage; empty-string placeholder '' in every sample. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#ha_standings-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the site has nothing for the request (a season before 2005/06, an unknown or pre-2025/26 game, a phase not played). |
| `ValueError` | a value outside its enumerated set, or a field the site requires is missing. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#ha_standings-example}

```python
ha_standings(season=2026)
```

_Last validated n/a._

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
| `season` | integer | Season END year (2026 = 2025/26). |
| `rank` | integer | Rank in the table. |
| `player_name` | character | Player name (the table carries no player id). |
| `team_statnet_id` | character | StatNet team id (AIK, MODO, MIK, NYB, OSIK), not the display code. |
| `gp` | integer | Games played in the season and phase. |
| `tp` | integer | Points (goals + assists). |
| `g` | integer | Goals scored by the skater. |
| `a` | integer | Assists credited to the skater. |
| `pim` | integer | Penalty minutes. |
| `gwg` | integer | Game-winning goals. |
| `ppg` | integer | Power-play goals. |
| `sog` | integer | Shots on goal by the skater. |
| `hits` | integer | Hits (0 in seasons before tracking). |
| `bks` | integer | Blocked shots (0 in seasons before tracking). |
| `plus` | integer | Even-strength / short-handed goals for while on ice (plus count). |
| `minus` | integer | Goals against while on ice (minus count). |
| `plus_minus` | integer | Plus/minus: the Plus count less the Minus count. |
| `toi_gp` | character | Average time on ice per game, mm:ss string ('0:00' = not tracked, e.g. 2010). |
| `tppg` | numeric | Points per game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#ha_skater_stats-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the site has nothing for the request (a season before 2005/06, an unknown or pre-2025/26 game, a phase not played). |
| `ValueError` | a value outside its enumerated set, or a field the site requires is missing. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#ha_skater_stats-example}

```python
ha_skater_stats(category='summary', season=2026)
```

_Last validated n/a._

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
| `season` | integer | Season END year (2026 = 2025/26). |
| `rank` | integer | Rank in the table. |
| `player_name` | character | Player name (the table carries no player id). |
| `team_statnet_id` | character | StatNet team id (AIK, MODO, MIK, NYB, OSIK), not the display code. |
| `svs` | integer | Saves made by the goalkeeper. |
| `svs_perc` | numeric | Save percentage ('92.61'). |
| `gp` | integer | Games played in the season and phase. |
| `gpi` | integer | Games played in (appeared). |
| `ga` | integer | Goals against: goals allowed in the season and phase. |
| `gaa` | numeric | Goals-against average. |
| `w` | integer | Wins credited to the goalkeeper. |
| `t` | integer | Ties credited to the goalkeeper (0 in the overtime era). |
| `l` | integer | Losses credited to the goalkeeper. |
| `so` | integer | Shutouts recorded by the goalkeeper. |
| `mip` | character | Minutes played, total mm:ss string ('1726:51'). |
| `valid_ranking` | integer | 1 when the goalkeeper qualifies for the ranked list (minimum games), else 0. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#ha_goalie_stats-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the site has nothing for the request (a season before 2005/06, an unknown or pre-2025/26 game, a phase not played). |
| `ValueError` | a value outside its enumerated set, or a field the site requires is missing. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#ha_goalie_stats-example}

```python
ha_goalie_stats(category='summary', season=2026)
```

_Last validated n/a._

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
| `season` | integer | Season END year (2026 = 2025/26). |
| `rank` | integer | Rank in the table. |
| `team_code` | character | Display code of the team (AIK, MoDo, MORA, NVIF, ÖIK), not the StatNet id. |
| `gp` | integer | Games played in the season and phase. |
| `wins_total` | integer | Wins in total, overtime and shootout wins included (otw and sow are subsets). |
| `losses_total` | integer | Losses in total, overtime and shootout losses included (otl and sol are subsets). |
| `leading` | integer | Games or situations leading (StatNet 'Leading'; meaning unverified). |
| `trailing` | integer | Games or situations trailing (StatNet 'Trailing'; meaning unverified). |
| `g` | integer | Goals scored by the team. |
| `ga` | integer | Goals against: goals allowed in the season and phase. |
| `gfg` | integer | Games in which the team scored the first goal (inferred from the label; unverified). |
| `otw` | integer | Overtime wins (a subset of wins_total). |
| `otl` | integer | Overtime losses. |
| `sow` | integer | Shootout wins (a subset of wins_total). |
| `sol` | integer | Shootout losses. |
| `p_gp` | numeric | Points per game. |
| `gpg` | numeric | Goals for per game (number). |
| `gapg` | numeric | Goals against per game (number). |
| `points` | integer | Standings points: 3 per regulation win, 2 per overtime/shootout win, 1 per overtime/shootout loss (2025 IFB: 102 + 14 + 3 = 119). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#ha_team_stats-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the site has nothing for the request (a season before 2005/06, an unknown or pre-2025/26 game, a phase not played). |
| `ValueError` | a value outside its enumerated set, or a field the site requires is missing. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#ha_team_stats-example}

```python
ha_team_stats(category='results', season=2026)
```

_Last validated n/a._

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
| `season` | integer | Season END year (2026 = 2025/26). |
| `team_statnet_id` | character | StatNet team id (AIK, MODO, MIK, NYB, OSIK), not the display code. |
| `team_code` | character | Display code of the team (AIK, MoDo, MORA, NVIF, ÖIK), not the StatNet id. |
| `name` | character | Short club name (the teams list's name). |
| `full_name` | character | Full club name (the teams list's fullName). |
| `logos_icon` | character | Logo: icon-size logo URL. |
| `logos_large` | character | Logo: large logo URL. |
| `logos_small` | character | Logo: small logo rendition (500 px). |
| `logos_medium` | character | Logo: medium logo URL. |
| `logos_xlarge` | character | Logo: extra-large logo URL. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#ha_teams-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the site has nothing for the request (a season before 2005/06, an unknown or pre-2025/26 game, a phase not played). |
| `ValueError` | a value outside its enumerated set, or a field the site requires is missing. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#ha_teams-example}

```python
ha_teams(season=2026)
```

_Last validated n/a._

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
| `rank` | integer | Rank in the table. |
| `player_name` | character | Player name (the table carries no player id). |
| `team_code` | character | Display code of the team (AIK, MoDo, MORA, NVIF, ÖIK), not the StatNet id. |
| `team_statnet_id` | character | StatNet team id (AIK, MODO, MIK, NYB, OSIK), not the display code. |
| `jersey_number` | integer | Jersey number of the player. |
| `position` | character | Playing position: F forward, D defence (G goalkeeper on goalie metrics). |
| `metric` | character | Metric code (echo of the request metric). |
| `metric_value` | character | Value of the metric (a count, a percentage or a clock, as text). |
| `player_statnet_id` | integer | StatNet player id (the same id space as SHL's isa id, verified on one player). |
| `first_name` | character | First name of the player. |
| `family_name` | character | Family name of the player. |
| `slug` | character | Player slug; joins ha_players. |
| `headshot` | character | Headshot URL (medium rendition). |
| `headshot_small` | character | Small headshot URL. |
| `headshot_medium` | character | Medium headshot URL. |
| `team_logo` | character | Logo URL of the player's team. |
| `team_name` | character | Team label as shown on the card (the display team code). |
| `team_main_color` | character | Team primary colour (hex). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#ha_leaders-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the site has nothing for the request (a season before 2005/06, an unknown or pre-2025/26 game, a phase not played). |
| `ValueError` | a value outside its enumerated set, or a field the site requires is missing. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#ha_leaders-example}

```python
ha_leaders(metric='TP')
```

_Last validated n/a._

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
| `family_name` | character | Family name of the player. |
| `first_name` | character | First name of the player. |
| `player_cms_id` | integer | CMS (Strapi) record id of the player (not the StatNet id). |
| `player_document_id` | character | CMS (Strapi) document id of the player. |
| `headshots_large` | character | Headshot URL (large rendition). |
| `headshots_small` | character | Headshot URL (small rendition). |
| `headshots_thumb` | character | Headshot URL (thumb rendition). |
| `headshots_medium` | character | Headshot URL (medium rendition). |
| `position_code` | character | Playing position code (CE centre, LW left wing, ...); positionName spells it out in English. |
| `jersey_number` | integer | Jersey number of the player. |
| `birth_date` | character | Birth date, YYYY-MM-DD. |
| `country` | character | Nationality, IOC/ISO alpha-3 code (SWE, NOR). |
| `height` | integer | Player height in centimetres. |
| `weight` | integer | Player weight in kilograms. |
| `shoots` | character | Shoots / catches: L or R. |
| `youth_team` | character | Youth (development) club. |
| `is_rookie` | logical | True when the CMS directory flags the player as a rookie. |
| `team_player` | character | Card label 'TEAM \\| POS JERSEY \\| Family name'. |
| `slug` | character | Player slug; joins ha_leaders. |
| `position_name` | character | Playing position, English label (Center, Left Wing, ...). |
| `team_cms_id` | integer | CMS (Strapi) record id of the team (81 = AIK). |
| `team_document_id` | character | CMS (Strapi) document id of the team. |
| `team_code` | character | Display code of the team (AIK, MoDo, MORA, NVIF, ÖIK), not the StatNet id. |
| `team_main_color_hex` | character | Primary colour of the team (hex). |
| `team_name` | character | Team label as shown on the card (the display team code). |
| `team_logo_url` | character | Logo URL of the team. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#ha_players-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the site has nothing for the request (a season before 2005/06, an unknown or pre-2025/26 game, a phase not played). |
| `ValueError` | a value outside its enumerated set, or a field the site requires is missing. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#ha_players-example}

```python
ha_players(team='AIK')
```

_Last validated n/a._
