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
| `team_name` | character | Team's display name, e.g. HPK or Tappara. |
| `country_of_birth` | character | Country of birth, 3-letter code. |
| `place_of_birth` | character | Player's place of birth (town or city). |
| `date_of_birth` | character | Date of birth, YYYY-MM-DD. |
| `nationality` | character | Nationality, 3-letter code. |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `role` | character | Position: CENTER, LEFT_WING, RIGHT_WING, LEFT_DEFENSEMAN, RIGHT_DEFENSEMAN, SEVENTH_DEFENSEMAN, GOALIE. |
| `role_code` | character | Position code, Finnish: KH = centre, VL/OL = left/right wing, VP/OP = left/right defence, 7. P = seventh defenceman, MV = goalkeeper. |
| `handedness` | character | Shoots LEFT or RIGHT. |
| `height` | integer | Player's height in centimetres. |
| `weight` | integer | Player's weight in kilograms. |
| `rookie` | logical | True when Liiga flags the player as a rookie. |
| `jersey` | integer | Jersey (sweater) number the player wore. |
| `injured` | logical | True when the player is listed as injured. |
| `suspended` | logical | True when the player is listed as suspended. |
| `removed` | logical | Removed from the roster. |
| `captain` | logical | True when the player is the team captain (C). |
| `alternate_captain` | logical | True when the player is an alternate captain (A). |
| `picture_url` | character | URL of the player's photo. |

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
| `birth_locality_country_code` | character | Player's birth country, 3-letter code (e.g. FIN). |
| `birth_locality_country_name` | character | Player's birth country name, in Finnish (e.g. Suomi). |
| `birth_locality_name` | character | Player's birth place (town or city). |
| `date_of_birth` | character | Date of birth, YYYY-MM-DD. |
| `player_id` | integer | Player id; the same number in lineups, rosters, stat tables and the profile (fihaId). |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `handedness` | character | Shoots LEFT or RIGHT. |
| `height` | integer | Player's height in centimetres. |
| `is_suspended` | logical | True when the player is listed as suspended. |
| `is_removed` | logical | Removed from the roster. |
| `nationality_code` | character | Player's nationality, 3-letter country code (e.g. FIN). |
| `nationality_name` | character | Player's nationality as a country name, in Finnish (e.g. Suomi). |
| `weight` | integer | Player's weight in kilograms. |
| `teams` | character | Map keyed by an id -> one of the player's team seasons (season, jersey, teamId, slug). |

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
| `games` | integer | Games (player, that season and tournament). |
| `played_games` | integer | Played games (player, that season and tournament). |
| `goals` | integer | Goals (player, that season and tournament). |
| `assists` | integer | Assists (player, that season and tournament). |
| `points` | integer | Points (player, that season and tournament). |
| `penalty_minutes` | integer | Penalty minutes (player, that season and tournament). |
| `plus` | integer | On ice for goals for (the + of plus/minus). |
| `minus` | integer | On ice for goals against (the - of plus/minus). |
| `plus_minus` | integer | Plus/minus: on-ice goals for minus on-ice goals against. |
| `power_play_goals` | integer | Power play goals (player, that season and tournament). |
| `short_handed_goals` | integer | Short handed goals (player, that season and tournament). |
| `winning_goals` | integer | Winning goals (player, that season and tournament). |
| `shots` | integer | Shots (player, that season and tournament). |
| `shots_into_goal` | integer | Shots into goal (player, that season and tournament). |
| `shot_percentage` | numeric | Shot percentage (player, that season and tournament). Percent, 0-100, sent as a STRING. |
| `shot_into_goal_percentage` | numeric | Shot into goal percentage (player, that season and tournament). Percent, 0-100, sent as a STRING. |
| `tournament` | character | Tournament of the career row, uppercase: RUNKOSARJA (regular season), PLAYOFFS, PRACTICE (preseason), ... |
| `league_name` | character | Name of the league of that career row, e.g. Liiga. |
| `team_name` | character | Team's display name, e.g. HPK or Tappara. |
| `end_time` | character | ISO 8601 UTC timestamp, not game seconds: the end of that season-tournament for finished seasons (2026-03-17T23:59:59Z for the 2025-26 regular season) and the request time for the season in progress. Looks like the end of the stats window; meaning unverified. |
| `team_short_name` | character | Team abbreviation. |
| `time_on_ice` | integer | Time on ice (player, that season and tournament). Seconds. |
| `save_percentage` | numeric | Save percentage (player, that season and tournament). Percent, 0-100, sent as a STRING. |
| `goals_against_average` | numeric | Goals against per game (goalkeeper), that season and tournament; sent as a string. |
| `win_percentage` | numeric | (wins + 0.5 x ties) / games x 10,000, NOT 0-100 (sent as "4,583.33", comma = thousands separator): divide by 10,000 for the standings 0-1 scale. Practice rows can show 0.00 despite wins; unreliable there. |
| `shut_out` | character | Shutouts (goalkeeper), that season and tournament; sent as a string (e.g. "0"). |
| `faceoffs_won` | integer | Faceoffs won (player, that season and tournament). |
| `faceoffs_percentage` | numeric | Faceoffs percentage (player, that season and tournament). Percent, 0-100, sent as a STRING. |
| `wins` | integer | Wins (player, that season and tournament). |
| `losses` | integer | Losses (player, that season and tournament). |
| `ties` | integer | Ties (player, that season and tournament). |
| `gk_wins` | integer | Goalkeeper wins (player, that season and tournament). |
| `gk_losses` | integer | Goalkeeper losses (player, that season and tournament). |
| `gk_ties` | integer | Goalkeeper ties (player, that season and tournament). |
| `saves` | integer | Saves (player, that season and tournament). |
| `goals_against` | integer | Goals against (player, that season and tournament). |
| `faceoff_total` | integer | Faceoff total (player, that season and tournament). |

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
| `tournament` | character | Tournament of the game (the body's list key): regular, playoffs, practice, qualifications, playout, chl or pitsiturnaus. |
| `game_str` | character | Matchup label, e.g. `HPK - LUK`. |
| `game_id` | integer | Game id, unique only within a season: key a game as (season, game_id). |
| `date` | character | Game start, ISO 8601 UTC. |
| `assists` | integer | Assists (player, this game). |
| `blocked_or_saved_shots` | integer | Blocked or saved shots (player, this game). |
| `faceoffs_total` | integer | Faceoffs total (player, this game). |
| `faceoffs_won` | integer | Faceoffs won (player, this game). |
| `faceoffs_percentage` | numeric | Faceoffs percentage (player, this game). Percent, 0-100, sent as a STRING. |
| `goals` | integer | Goals (player, this game). |
| `minus` | integer | On ice for goals against (the - of plus/minus). |
| `total_points` | integer | Total points (player, this game). |
| `home_team_name` | character | Display name of the home team of the game, e.g. HPK. |
| `away_team_name` | character | Display name of the away team of the game, e.g. Lukko. |
| `penalty_minutes` | integer | Penalty minutes (player, this game). |
| `penaltykill_assists` | integer | Penaltykill assists (player, this game). |
| `penaltykill_goals` | integer | Penaltykill goals (player, this game). |
| `penaltykill_points` | integer | Penaltykill points (player, this game). |
| `plus` | integer | On ice for goals for (the + of plus/minus). |
| `plus_minus` | integer | Plus/minus: on-ice goals for minus on-ice goals against. |
| `powerplay_assists` | integer | Powerplay assists (player, this game). |
| `powerplay_goals` | integer | Powerplay goals (player, this game). |
| `powerplay_seconds` | integer | Powerplay seconds (player, this game). Seconds. |
| `time_on_ice` | integer | Time on ice (player, this game). Seconds. |
| `saves` | integer | Saves (player, this game). |
| `save_percentage` | numeric | Save percentage (player, this game). Percent, 0-100, sent as a STRING. |
| `block_or_saved_shots_percentage` | numeric | Block or saved shots percentage (player, this game). Percent, 0-100, sent as a STRING. |
| `shots` | integer | Shots (player, this game). |
| `shots_into_goal` | integer | Shots into goal (player, this game). |
| `shut_out` | integer | 1 for a goalkeeper's shutout in this game, else 0. |
| `shots_saved` | integer | Shots saved (player, this game). |
| `shots_saved_by_skater` | integer | Shots saved by skater (player, this game). |
| `shot_percentage` | numeric | Shot percentage (player, this game). Percent, 0-100, sent as a STRING. |
| `shot_into_goal_percentage` | numeric | Shot into goal percentage (player, this game). Percent, 0-100, sent as a STRING. |
| `goals_against` | integer | Goals against (player, this game). |
| `goalkeeper` | logical | True when the player is a goalkeeper. |
| `winning_goals` | integer | Winning goals (player, this game). |
| `won` | integer | 1 when the player's team won the game, else 0. |
| `tied` | integer | 1 when Liiga counts the game as a tie for the player's team, else 0 (overtime handling unverified). |
| `lost` | integer | 1 when the player's team lost the game, else 0. |

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
| `team_name` | character | Team's display name, e.g. HPK or Tappara. |
| `team_short_name` | character | Team abbreviation. |
| `role` | character | Position: CENTER, LEFT_WING, RIGHT_WING, LEFT_DEFENSEMAN, RIGHT_DEFENSEMAN, SEVENTH_DEFENSEMAN, GOALIE. |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `nationality` | character | Nationality, 3-letter code. |
| `tournament` | character | Tournament of the summed row as Liiga labels it, e.g. regular. |
| `picture_url` | character | URL of the player's photo. |
| `previous_teams_for_tournament` | character | Earlier teams of a player traded during the tournament. |
| `injured` | logical | True when the player is listed as injured. |
| `jersey` | integer | Jersey (sweater) number the player wore. |
| `last_season` | integer | Last season (END year) of the range in which the player appears. |
| `goalkeeper` | logical | True when the player is a goalkeeper. |
| `games` | integer | Games (player, summed over the season range). |
| `played_games` | integer | Played games (player, summed over the season range). |
| `rookie` | logical | True when Liiga flags the player as a rookie. |
| `suspended` | logical | True when the player is listed as suspended. |
| `removed` | logical | Removed from the roster. |
| `time_on_ice` | integer | Time on ice (player, summed over the season range). Seconds. |
| `current` | logical | Player is on the team now. |
| `is_u20` | logical | True when Liiga flags the player as under 20 (U20). |
| `golden_helmets` | integer | Golden helmets (Liiga's team points-leader helmet; reading unverified), summed over the season range. |
| `alumni_points` | integer | Alumni points (meaning undocumented by Liiga), summed over the season range. |
| `goals` | integer | Goals (player, summed over the season range). |
| `assists` | integer | Assists (player, summed over the season range). |
| `points` | integer | Points (player, summed over the season range). |
| `plus` | integer | On ice for goals for (the + of plus/minus). |
| `minus` | integer | On ice for goals against (the - of plus/minus). |
| `plus_minus` | integer | Plus/minus: on-ice goals for minus on-ice goals against. |
| `penalty_minutes` | integer | Penalty minutes (player, summed over the season range). |
| `powerplay_goals` | integer | Powerplay goals (player, summed over the season range). |
| `penaltykill_goals` | integer | Penaltykill goals (player, summed over the season range). |
| `winning_goals` | integer | Winning goals (player, summed over the season range). |
| `shots` | integer | Shots (player, summed over the season range). |
| `shots_into_goal` | integer | Shots into goal (player, summed over the season range). |
| `expected_goals` | numeric | Expected goals (xG) of the player's shots summed over the season range, from Liiga's undocumented model. |
| `contest_won` | integer | Contests (puck battles; definition unverified) the player won, summed over the season range. |
| `contest_lost` | integer | Contests (puck battles; definition unverified) the player lost, summed over the season range. |
| `goals_to_empty_goal` | integer | Goals to empty goal (player, summed over the season range). |
| `time_on_ice_avg` | numeric | Time on ice (player, summed over the season range). Seconds per game. |
| `contest_won_percentage` | numeric | Share of contests won, 0-100, over the season range (definition unverified). |
| `xge` | numeric | Goals minus expectedGoals over the season range (4 goals on 5.0956 xG = -1.0956). |
| `shot_percentage` | numeric | Shot percentage (player, summed over the season range). Percent, 0-100. |

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

_Last validated n/a._
