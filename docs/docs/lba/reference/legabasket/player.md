---
title: "LBA — Lega Basket Serie A site API (www.legabasket.it/api, Italy) — Player"
sidebar_label: "Player"
sidebar_position: 1
description: "LBA — Lega Basket Serie A site API (www.legabasket.it/api, Italy) — Player — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# LBA — Lega Basket Serie A site API (www.legabasket.it/api, Italy) — Player

## lba_players

Players of a season, 50 per page (walk the pages with page).

**Endpoint URL:** `GET https://www.legabasket.it/api/players/get-players`

**Valid URL:** [https://www.legabasket.it/api/players/get-players?items=50&year=2027](https://www.legabasket.it/api/players/get-players?items=50&year=2027)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `season` |  |  | `Y` | Season END year (2027 = 2026-27). Omitted = the site's default season (unverified). |
| `page` | `page` |  |  | `Y` | Page number, 1-based. Omitted = the first page. |

### Returns {#lba_players-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2027 = 2026-27): the row's START year + 1. |
| `player_id` | integer | Player id (global). |
| `name` | character | Player's given name. |
| `surname` | character | Player's family name. |
| `code` | character | Player code `SURNAME3-NAME3-YY`. |
| `team_name` | character | Team name in that season, with its sponsor (`Nutribullet Treviso Basket`); changes with the sponsor. |
| `year` | integer | Season START year (2026 = 2026-27); sdv END year = year + 1. The season of the team the row places the player on. |
| `player_picture_key` | character | Image key: the image is `{cdn_url}/{key}`, resized copies `{cdn_url}/variants/{key}/thumb` or `/large` (the site's getApiImageUrl). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#lba_players-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (an unknown season, a play-by-play before 2017-18, standings of a finished championship without a round, no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an unknown id (the site answers HTTP 500), a game that is not final, a truncated page, or a body for another season, championship, game, team or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#lba_players-example}

```python
lba_players(season=2027)
```

## lba_player

Player bio with current-season totals, highs and averages (one row).

**Endpoint URL:** `GET https://www.legabasket.it/api/players/get-player-by-id`

**Valid URL:** [https://www.legabasket.it/api/players/get-player-by-id?stats=1&id=969](https://www.legabasket.it/api/players/get-player-by-id?stats=1&id=969)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `id` | `player_id` |  | `Y` |  | Player id (player_id in lba_players or a box score), e.g. 969. Global: one id for a whole career. |

### Returns {#lba_player-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player id (global). |
| `code` | character | Player code `SURNAME3-NAME3-YY`. |
| `name` | character | Player's given name. |
| `surname` | character | Player's family name. |
| `place_of_birth` | character | Birthplace (Italian spelling). |
| `birth_date` | character | Birth date, Date `YYYY-MM-DD`. |
| `updated_at` | character | Timestamp, ISO-8601 with the Italian offset. |
| `ext_code` | character | External (data feed) player code; equals `code` in the samples. |
| `seo_meta_title` | character | SEO title (null in the samples). |
| `seo_meta_description` | character | SEO description (null in the samples). |
| `photo_picture_key` | character | Image key: the image is `{cdn_url}/{key}`, resized copies `{cdn_url}/variants/{key}/thumb` or `/large` (the site's getApiImageUrl). |
| `fullbody_picture_key` | character | Image key: the image is `{cdn_url}/{key}`, resized copies `{cdn_url}/variants/{key}/thumb` or `/large` (the site's getApiImageUrl). Null in every sample. |
| `start_date` | character | Contract start with the team, Date `YYYY-MM-DD`. |
| `end_date` | character | Contract end with the team, Date `YYYY-MM-DD`. |
| `player_number` | character | Jersey number, number sent as a string. |
| `uefa_ratio` | character | Eligibility class for the foreign-player quota: `I`, `E` or `C` (7 / 6 / 1 on the Treviso roster; reading as Italian / EU / non-EU is inferred). |
| `height` | integer | Player's height in centimetres. |
| `weight` | integer | Player's weight in kilograms. |
| `team_id` | integer | Team id (team-season) of his current team. |
| `player_role_description` | character | Position, Italian (see `players[].player_role` of the roster route). |
| `player_alpha3` | character | Nationality, ISO alpha-3. |
| `player_country_name` | character | Nationality, Italian country name (`Italia` = Italy); map to English through `player_alpha3` (ISO 3166 alpha-3), not by translating the name. |
| `player_country` | character | Continent, Italian name (`Europa` = Europe; the other values are presumably `Africa`, `America`, `Asia`, `Oceania`, unobserved). |
| `team_code` | character | `<season>\\|<team code>` of his current team (`2026\\|TVU`). |
| `team_name` | character | Team name in that season, with its sponsor (`Nutribullet Treviso Basket`); changes with the sponsor. |
| `year` | integer | Season START year (2026 = 2026-27); sdv END year = year + 1. The season of the team the row places the player on. |
| `team_logo_key` | character | Image key: the image is `{cdn_url}/{key}`, resized copies `{cdn_url}/variants/{key}/thumb` or `/large` (the site's getApiImageUrl). |
| `stats_played_matches` | integer | Games played this regular season. |
| `stats_totals_highs_rs_player_id` | integer | Player id (global, one per career). |
| `stats_totals_highs_rs_championship_serie_code` | character | Series code, `A1` = Serie A. |
| `stats_totals_highs_rs_quintet_sum` | integer | Current regular-season total of starts (in the starting five). |
| `stats_totals_highs_rs_points_sum` | integer | Current regular-season total of points. |
| `stats_totals_highs_rs_played_minutes_sum` | integer | Current regular-season total of minutes played. |
| `stats_totals_highs_rs_played_matches_sum` | integer | Current regular-season total of games played. |
| `stats_totals_highs_rs_total_matches_sum` | integer | Current regular-season total of team games while the player was listed (a player with 3 of 21 shows 21; inferred). |
| `stats_totals_highs_rs_done_fouls_sum` | integer | Current regular-season total of fouls committed. |
| `stats_totals_highs_rs_suffered_fouls_sum` | integer | Current regular-season total of fouls drawn. |
| `stats_totals_highs_rs_shots_2p_realized_sum` | integer | Current regular-season total of 2-point field goals made. |
| `stats_totals_highs_rs_shots_2p_total_sum` | integer | Current regular-season total of 2-point field goals attempted. |
| `stats_totals_highs_rs_slam_dunk_sum` | integer | Current regular-season total of dunks. |
| `stats_totals_highs_rs_shots_3p_realized_sum` | integer | Current regular-season total of 3-point field goals made. |
| `stats_totals_highs_rs_shots_3p_total_sum` | integer | Current regular-season total of 3-point field goals attempted. |
| `stats_totals_highs_rs_free_throws_realized_sum` | integer | Current regular-season total of free throws made. |
| `stats_totals_highs_rs_free_throws_total_sum` | integer | Current regular-season total of free throws attempted. |
| `stats_totals_highs_rs_offensive_rebound_sum` | integer | Current regular-season total of offensive rebounds. |
| `stats_totals_highs_rs_defensive_rebound_sum` | integer | Current regular-season total of defensive rebounds. |
| `stats_totals_highs_rs_ball_stop_given_sum` | integer | Current regular-season total of blocks. |
| `stats_totals_highs_rs_ball_stop_received_sum` | integer | Current regular-season total of own shots blocked. |
| `stats_totals_highs_rs_lost_balls_sum` | integer | Current regular-season total of turnovers. |
| `stats_totals_highs_rs_regain_balls_sum` | integer | Current regular-season total of steals. |
| `stats_totals_highs_rs_assists_sum` | integer | Current regular-season total of assists. |
| `stats_totals_highs_rs_rating_lega_sum` | integer | Current regular-season total of LBA efficiency rating (`valutazione`). |
| `stats_totals_highs_rs_rating_oer_sum` | numeric | OER over the regular season (1.04 for 3 games: an average despite `_sum`; inferred). |
| `stats_totals_highs_rs_avg_points_sum` | integer | Current regular-season total of plus-minus, despite the name (see the game-log row, where it equals the box score `plus_minus`). |
| `stats_totals_highs_rs_quintet_max` | integer | Current regular-season single-game high of starts (in the starting five). |
| `stats_totals_highs_rs_points_max` | integer | Current regular-season single-game high of points. |
| `stats_totals_highs_rs_played_minutes_max` | integer | Current regular-season single-game high of minutes played. |
| `stats_totals_highs_rs_done_fouls_max` | integer | Current regular-season single-game high of fouls committed. |
| `stats_totals_highs_rs_suffered_fouls_max` | integer | Current regular-season single-game high of fouls drawn. |
| `stats_totals_highs_rs_shots_2p_realized_max` | integer | Current regular-season single-game high of 2-point field goals made. |
| `stats_totals_highs_rs_shots_2p_total_max` | integer | Current regular-season single-game high of 2-point field goals attempted. |
| `stats_totals_highs_rs_shots_2p_perc_max` | integer | Current regular-season single-game high of 2-point field-goal percentage, 0-100. |
| `stats_totals_highs_rs_slam_dunk_max` | integer | Current regular-season single-game high of dunks. |
| `stats_totals_highs_rs_shots_3p_realized_max` | integer | Current regular-season single-game high of 3-point field goals made. |
| `stats_totals_highs_rs_shots_3p_total_max` | integer | Current regular-season single-game high of 3-point field goals attempted. |
| `stats_totals_highs_rs_shots_3p_perc_max` | integer | Current regular-season single-game high of 3-point field-goal percentage, 0-100. |
| `stats_totals_highs_rs_free_throws_realized_max` | integer | Current regular-season single-game high of free throws made. |
| `stats_totals_highs_rs_free_throws_total_max` | integer | Current regular-season single-game high of free throws attempted. |
| `stats_totals_highs_rs_free_throws_perc_max` | integer | Current regular-season single-game high of free-throw percentage, 0-100. |
| `stats_totals_highs_rs_offensive_rebound_max` | integer | Current regular-season single-game high of offensive rebounds. |
| `stats_totals_highs_rs_defensive_rebound_max` | integer | Current regular-season single-game high of defensive rebounds. |
| `stats_totals_highs_rs_total_rebounds_max` | integer | Current regular-season single-game high of total rebounds. |
| `stats_totals_highs_rs_ball_stop_given_max` | integer | Current regular-season single-game high of blocks. |
| `stats_totals_highs_rs_ball_stop_received_max` | integer | Current regular-season single-game high of own shots blocked. |
| `stats_totals_highs_rs_lost_balls_max` | integer | Current regular-season single-game high of turnovers. |
| `stats_totals_highs_rs_regain_balls_max` | integer | Current regular-season single-game high of steals. |
| `stats_totals_highs_rs_assists_max` | integer | Current regular-season single-game high of assists. |
| `stats_totals_highs_rs_rating_lega_max` | integer | Current regular-season single-game high of LBA efficiency rating (`valutazione`). |
| `stats_totals_highs_rs_rating_oer_max` | numeric | Current regular-season single-game high of OER (points / (FGA + turnovers + FTA / 2)). |
| `stats_totals_highs_rs_avg_points_max` | integer | Current regular-season single-game high of plus-minus, despite the name (see the game-log row, where it equals the box score `plus_minus`). |
| `stats_totals_highs_rs_adp_max` | integer | Current regular-season single-game high of plus-minus (as `match_adp`). |
| `stats_totals_highs_rs_id` | integer | Always null (not a key). |
| `stats_avg_rs_player_id` | integer | Player id (global, one per career). |
| `stats_avg_rs_championship_serie_code` | character | Series code, `A1` = Serie A. |
| `stats_avg_rs_quintet_avg` | numeric | Current regular-season per-game average of starts (in the starting five), number sent as a string. |
| `stats_avg_rs_points_avg` | numeric | Current regular-season per-game average of points, number sent as a string. |
| `stats_avg_rs_played_minutes_avg` | numeric | Current regular-season per-game average of minutes played, number sent as a string. |
| `stats_avg_rs_played_matches_avg` | numeric | Always `1.0` (games divided by games), number sent as a string. |
| `stats_avg_rs_total_matches_avg` | numeric | Always `1.0`, number sent as a string. |
| `stats_avg_rs_done_fouls_avg` | numeric | Current regular-season per-game average of fouls committed, number sent as a string. |
| `stats_avg_rs_suffered_fouls_avg` | numeric | Current regular-season per-game average of fouls drawn, number sent as a string. |
| `stats_avg_rs_shots_2p_realized_avg` | numeric | Current regular-season per-game average of 2-point field goals made, number sent as a string. |
| `stats_avg_rs_shots_2p_total_avg` | numeric | Current regular-season per-game average of 2-point field goals attempted, number sent as a string. |
| `stats_avg_rs_slam_dunk_avg` | numeric | Current regular-season per-game average of dunks, number sent as a string. |
| `stats_avg_rs_shots_3p_realized_avg` | numeric | Current regular-season per-game average of 3-point field goals made, number sent as a string. |
| `stats_avg_rs_shots_3p_total_avg` | numeric | Current regular-season per-game average of 3-point field goals attempted, number sent as a string. |
| `stats_avg_rs_free_throws_realized_avg` | numeric | Current regular-season per-game average of free throws made, number sent as a string. |
| `stats_avg_rs_free_throws_total_avg` | numeric | Current regular-season per-game average of free throws attempted, number sent as a string. |
| `stats_avg_rs_offensive_rebound_avg` | numeric | Current regular-season per-game average of offensive rebounds, number sent as a string. |
| `stats_avg_rs_defensive_rebound_avg` | numeric | Current regular-season per-game average of defensive rebounds, number sent as a string. |
| `stats_avg_rs_ball_stop_given_avg` | numeric | Current regular-season per-game average of blocks, number sent as a string. |
| `stats_avg_rs_ball_stop_received_avg` | numeric | Current regular-season per-game average of own shots blocked, number sent as a string. |
| `stats_avg_rs_lost_balls_avg` | numeric | Current regular-season per-game average of turnovers, number sent as a string. |
| `stats_avg_rs_regain_balls_avg` | numeric | Current regular-season per-game average of steals, number sent as a string. |
| `stats_avg_rs_assists_avg` | numeric | Current regular-season per-game average of assists, number sent as a string. |
| `stats_avg_rs_rating_lega_avg` | numeric | Current regular-season per-game average of LBA efficiency rating (`valutazione`), number sent as a string. |
| `stats_avg_rs_rating_oer_avg` | numeric | Per-game average OER (a number, unlike the other `_avg` values). |
| `stats_avg_rs_avg_points_avg` | numeric | Current regular-season per-game average of plus-minus, despite the name (see the game-log row, where it equals the box score `plus_minus`), number sent as a string. |
| `stats_avg_rs_id` | integer | Always null (not a key). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#lba_player-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (an unknown season, a play-by-play before 2017-18, standings of a finished championship without a round, no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an unknown id (the site answers HTTP 500), a game that is not final, a truncated page, or a body for another season, championship, game, team or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#lba_player-example}

```python
lba_player(player_id=969)
```

## lba_player_stats

A player's career by championship: per-game averages, single-game highs and totals.

**Endpoint URL:** `GET https://www.legabasket.it/api/players/get-player-stats`

**Valid URL:** [https://www.legabasket.it/api/players/get-player-stats?id=969](https://www.legabasket.it/api/players/get-player-stats?id=969)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `id` | `player_id` |  | `Y` |  | Player id (player_id in lba_players or a box score), e.g. 969. Global: one id for a whole career. |

### Returns {#lba_player_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player id (global: one id for a whole career); the request's player. |
| `stat_type` | character | Statistic of the row: avg (per game), max (single-game high) or sum (total). |
| `season` | integer | Season END year (2027 = 2026-27): the row's START year + 1. |
| `championship_year` | integer | Season START year (2026 = 2026-27); sdv END year = year + 1. |
| `championship_position` | integer | Display order of the competition within the season (1 = Regular Season; inferred). |
| `championship_name` | character | Competition name (`Regular Season`, `Play Off`, `Coppa Italia`, ...). |
| `team_name` | character | Team name in that season, with its sponsor (`Nutribullet Treviso Basket`); changes with the sponsor. |
| `quintet` | integer | Per-game average of starts (in the starting five) in that season, competition and team. |
| `points` | numeric | Per-game average of points in that season, competition and team. |
| `played_minutes` | numeric | Per-game average of minutes played in that season, competition and team. |
| `played_matches` | integer | Games played in that season, competition and team: a count, the same in the avg, max and sum rows. |
| `total_matches` | integer | Team games while the player was listed (a player with 3 of 21 shows 21; inferred) in that season, competition and team: a count, the same in the avg, max and sum rows. |
| `done_fouls` | integer | Per-game average of fouls committed in that season, competition and team. |
| `suffered_fouls` | integer | Per-game average of fouls drawn in that season, competition and team. |
| `shots_2p_realized` | numeric | Per-game average of 2-point field goals made in that season, competition and team. |
| `shots_2p_total` | integer | Per-game average of 2-point field goals attempted in that season, competition and team. |
| `slam_dunk` | integer | Per-game average of dunks in that season, competition and team. |
| `shots_3p_realized` | integer | Per-game average of 3-point field goals made in that season, competition and team. |
| `shots_3p_total` | integer | Per-game average of 3-point field goals attempted in that season, competition and team. |
| `free_throws_realized` | integer | Per-game average of free throws made in that season, competition and team. |
| `free_throws_total` | integer | Per-game average of free throws attempted in that season, competition and team. |
| `offensive_rebound` | integer | Per-game average of offensive rebounds in that season, competition and team. |
| `defensive_rebound` | integer | Per-game average of defensive rebounds in that season, competition and team. |
| `total_rebound` | integer | Per-game average of total rebounds in that season, competition and team. |
| `ball_stop_given` | integer | Per-game average of blocks in that season, competition and team. |
| `ball_stop_received` | integer | Per-game average of own shots blocked in that season, competition and team. |
| `lost_balls` | integer | Per-game average of turnovers in that season, competition and team. |
| `regain_balls` | integer | Per-game average of steals in that season, competition and team. |
| `assists` | integer | Per-game average of assists in that season, competition and team. |
| `rating_lega` | integer | Per-game average of LBA efficiency rating (`valutazione`) in that season, competition and team. |
| `rating_oer` | numeric | Per-game average of OER (points / (FGA + turnovers + FTA / 2)) in that season, competition and team. |
| `avg_points` | numeric | Per-game average of plus-minus, despite the name (see the game-log row, where it equals the box score `plus_minus`) in that season, competition and team. |
| `aplus_dp` | integer | Per-game average of `aplus_dp`, an unverified metric (not plus-minus or assists: Macura 2026-27 has 9 against plus-minus 8 and 3 assists) in that season, competition and team. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#lba_player_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (an unknown season, a play-by-play before 2017-18, standings of a finished championship without a round, no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an unknown id (the site answers HTTP 500), a game that is not final, a truncated page, or a body for another season, championship, game, team or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#lba_player_stats-example}

```python
lba_player_stats(player_id=969)
```

## lba_players_leaderboard

Season player leaderboard for one category (per-game average).

**Endpoint URL:** `GET https://www.legabasket.it/api/statistics/get-players-statistics`

**Valid URL:** [https://www.legabasket.it/api/statistics/get-players-statistics?cs_id=1&s=2027&cat=points](https://www.legabasket.it/api/statistics/get-players-statistics?cs_id=1&s=2027&cat=points)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `s` | `season` |  |  | `Y` | Season END year (2027 = 2026-27). Omitted = the site's default season (unverified). |
| `cat` | `category` |  |  | `Y` | Category: points, rating_lega, total_rebound, assists, shots_3p_perc, ... (the site's filters.categories). |

### Returns {#lba_players_leaderboard-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2027 = 2026-27): the row's START year + 1. |
| `category` | character | Category of this row (the `cat` param, e.g. `points`). |
| `player_id` | integer | Player id (global, one per career). |
| `name` | character | Given name (players) / team name (teams). |
| `surname` | character | Player's family name. |
| `team_name` | character | Team name in that season, with its sponsor (`Nutribullet Treviso Basket`); changes with the sponsor. |
| `tv_name` | character | Short team name for TV graphics. |
| `year` | integer | Season START year (2026 = 2026-27); sdv END year = year + 1. |
| `category_filter_total` | integer | Season total in the category. |
| `attempts` | integer | Shots attempted in the zone, parsed from num. |
| `presences` | integer | Games the player appeared in, counted for the leaderboard. |
| `minutes` | numeric | Minutes per game (players: decimal string `28.67`) / total minutes (teams: integer). |
| `score_40_min` | numeric | Value per 40 minutes. |
| `score` | numeric | Value in the category under the `ob` order (default `avg` = per game); players send a decimal string, teams a number. |
| `team_id` | integer | Team id (team-season). |
| `player_photo_key` | character | Image key: the image is `{cdn_url}/{key}`, resized copies `{cdn_url}/variants/{key}/thumb` or `/large` (the site's getApiImageUrl). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#lba_players_leaderboard-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (an unknown season, a play-by-play before 2017-18, standings of a finished championship without a round, no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an unknown id (the site answers HTTP 500), a game that is not final, a truncated page, or a body for another season, championship, game, team or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#lba_players_leaderboard-example}

```python
lba_players_leaderboard(category='points', season=2027)
```
