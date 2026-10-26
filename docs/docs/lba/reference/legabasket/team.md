---
title: "Lega Basket Serie A — Lega Basket Serie A site API (www.legabasket.it/api, Italy) — Team"
sidebar_label: "Team"
sidebar_position: 2
description: "Lega Basket Serie A — Lega Basket Serie A site API (www.legabasket.it/api, Italy) — Team — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# Lega Basket Serie A — Lega Basket Serie A site API (www.legabasket.it/api, Italy) — Team

## lba_team_box_score

Team lines of a final game's box score: the team-booked stats and the totals of each side.

**Endpoint URL:** `GET https://www.legabasket.it/api/championships/get-championships-matches-scores-by-id`

**Valid URL:** [https://www.legabasket.it/api/championships/get-championships-matches-scores-by-id?info=true&id=25602](https://www.legabasket.it/api/championships/get-championships-matches-scores-by-id?info=true&id=25602)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `id` | `game_id` |  | `Y` |  | Game id (game_id in lba_calendar), e.g. 25602. Global across seasons. |

### Returns {#lba_team_box_score-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | Game id (global across seasons); joins lba_calendar and lba_game. |
| `season` | Int64 | Season END year (2027 = 2026-27): the row's START year + 1. |
| `side` | String | Which team of the game the row belongs to: home or visitor. |
| `team_id` | Int64 | Team-season id (re-issued every season); join lba_teams for the stable club_id. |
| `row_type` | String | team = stats booked to the team itself (team rebounds, team turnovers); totals = the whole side. |
| `pun` | Int64 | Points (`punti`). (team totals (players + team row)) |
| `falli_c` | Int64 | Fouls committed (`falli commessi`). (team totals (players + team row)) |
| `falli_sf` | Int64 | Fouls drawn (`falli subiti`). (team totals (players + team row)) |
| `falli_tc1` | Int64 | Technical fouls (`falli tecnici`; reading `tc1` as category-1 technicals is inferred). (team totals (players + team row)) |
| `falli_fla` | Int64 | Unsportsmanlike / flagrant fouls (`falli antisportivi`). (team totals (players + team row)) |
| `rimbalzi_o` | Int64 | Offensive rebounds (`rimbalzi offensivi`). (team totals (players + team row)) |
| `rimbalzi_d` | Int64 | Defensive rebounds (`rimbalzi difensivi`). (team totals (players + team row)) |
| `rimbalzi_t` | Int64 | Total rebounds (`rimbalzi totali`). (team totals (players + team row)) |
| `palle_p` | Int64 | Turnovers (`palle perse`). (team totals (players + team row)) |
| `palle_r` | Int64 | Steals (`palle recuperate`). (team totals (players + team row)) |
| `val_lega` | Int64 | LBA efficiency rating (`valutazione Lega`). (team totals (players + team row)) |
| `min` | Int64 | Minutes played, whole minutes (`minuti`); 200 in `totals` = 5 × 40. (team totals (players + team row)) |
| `sec` | Int64 | Seconds played (`secondi`) = `min` × 60 in every sample. (team totals (players + team row)) |
| `t2_r` | Int64 | 2-point field goals made (`tiri da 2 realizzati`). (team totals (players + team row)) |
| `t2_t` | Int64 | 2-point field goals attempted (`tiri da 2 tentati`). (team totals (players + team row)) |
| `t2_perc` | Float64 | 2-point field-goal percentage, 0-100 (number; 0 when no attempt). (team totals (players + team row)) |
| `sc` | Int64 | Dunks (`schiacciate`). (team totals (players + team row)) |
| `t3_r` | Int64 | 3-point field goals made. (team totals (players + team row)) |
| `t3_t` | Int64 | 3-point field goals attempted. (team totals (players + team row)) |
| `t3_perc` | Float64 | 3-point field-goal percentage, 0-100. (team totals (players + team row)) |
| `tl_r` | Int64 | Free throws made (`tiri liberi realizzati`). (team totals (players + team row)) |
| `tl_t` | Int64 | Free throws attempted (`tiri liberi tentati`). (team totals (players + team row)) |
| `tl_perc` | Float64 | Free-throw percentage, 0-100. (team totals (players + team row)) |
| `stoppate_dat` | Int64 | Blocks (`stoppate date`). (team totals (players + team row)) |
| `stoppate_sub` | Int64 | Own shots blocked (`stoppate subite`). (team totals (players + team row)) |
| `ass` | Int64 | Assists. (team totals (players + team row)) |
| `val_oer` | Float64 | OER = points / (FGA + turnovers + FTA / 2); verified on every 2026-27 sample row. (team totals (players + team row)) |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#lba_team_box_score-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (an unknown season, a play-by-play before 2017-18, standings of a finished championship without a round, no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an unknown id (the site answers HTTP 500), a game that is not final, a truncated page, or a body for another season, championship, game, team or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#lba_team_box_score-example}

```python
lba_team_box_score(game_id=25602)
```

## lba_teams

Teams of a season, one row per team-season with the stable club id.

**Endpoint URL:** `GET https://www.legabasket.it/api/teams/get-teams`

**Valid URL:** [https://www.legabasket.it/api/teams/get-teams?items=50&year=2027](https://www.legabasket.it/api/teams/get-teams?items=50&year=2027)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `season` |  |  | `Y` | Season END year (2027 = 2026-27). Omitted = the site's default season (unverified). |

### Returns {#lba_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season END year (2027 = 2026-27): the row's START year + 1. |
| `team_id` | Int64 | Team id (one team-season). |
| `name` | String | Team name of that season (sponsor name). |
| `year` | Int64 | Season START year (2026 = 2026-27); sdv END year = year + 1. |
| `club_id` | Int64 | Club id: the cross-season join key for team ids (only here and in the `team` headers); stability across seasons is unverified. |
| `club_code` | String | Club code (`TVB`, `TS`); whether it is stable across seasons is unverified. |
| `updated_at` | String | Timestamp, ISO-8601 with the Italian offset. |
| `logo_key` | String | Image key: the image is `{cdn_url}/{key}`, resized copies `{cdn_url}/variants/{key}/thumb` or `/large` (the site's getApiImageUrl). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#lba_teams-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (an unknown season, a play-by-play before 2017-18, standings of a finished championship without a round, no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an unknown id (the site answers HTTP 500), a game that is not final, a truncated page, or a body for another season, championship, game, team or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#lba_teams-example}

```python
lba_teams(season=2027)
```

## lba_team_players_stats

Season totals of each player of a team-season.

**Endpoint URL:** `GET https://www.legabasket.it/api/teams/get-team-players-stats`

**Valid URL:** [https://www.legabasket.it/api/teams/get-team-players-stats?cs_id=1&id=1764&s=2027](https://www.legabasket.it/api/teams/get-team-players-stats?cs_id=1&id=1764&s=2027)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `id` | `team_id` |  | `Y` |  | Team id (team_id in lba_teams), e.g. 1764. One team-season: ids are re-issued every season. |
| `s` | `season` |  |  | `Y` | Season END year (2027 = 2026-27) of the team id. Omitted = the site's default (unverified). |

### Returns {#lba_team_players_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | Team-season id (re-issued every season); join lba_teams for the stable club_id. |
| `club_id` | Int64 | Club id: the cross-season join key for team ids (only here and in the `team` headers); stability across seasons is unverified. |
| `club_code` | String | Club code (`TVB`, `TS`); whether it is stable across seasons is unverified. |
| `season` | Int64 | Season END year (2027 = 2026-27): the row's START year + 1. |
| `player_id` | Int64 | Player id (global, one per career). |
| `player_name` | String | Player's given name. |
| `player_surname` | String | Player's family name. |
| `player_season` | Int64 | Season START year (2026 = 2026-27); sdv END year = year + 1. |
| `player_photo_key` | String | Image key: the image is `{cdn_url}/{key}`, resized copies `{cdn_url}/variants/{key}/thumb` or `/large` (the site's getApiImageUrl). |
| `played_matches` | Int64 | Games the player appeared in, in the request's championship series. |
| `total_matches` | Int64 | Team games while the player was listed (inferred). |
| `start_formation` | Int64 | Games the player started, in the request's championship series. |
| `points_sum` | Int64 | Season total of points. |
| `played_minutes_sum` | Int64 | Season total of minutes played. |
| `rating_lega_sum` | Int64 | Season total of LBA efficiency rating (`valutazione`). |
| `rating_oer_sum` | Float64 | Sum of the per-game OER values (3.34 over 3 games), not a season OER. |
| `offensive_rebound_sum` | Int64 | Season total of offensive rebounds. |
| `defensive_rebound_sum` | Int64 | Season total of defensive rebounds. |
| `total_rebound_sum` | Int64 | Season total of total rebounds. |
| `assists_sum` | Int64 | Season total of assists. |
| `regain_balls_sum` | Int64 | Season total of steals. |
| `lost_balls_sum` | Int64 | Season total of turnovers. |
| `ball_stop_given_sum` | Int64 | Season total of blocks. |
| `ball_stop_received_sum` | Int64 | Season total of own shots blocked. |
| `done_fouls_sum` | Int64 | Season total of fouls committed. |
| `suffered_fouls_sum` | Int64 | Season total of fouls drawn. |
| `plus_minus_sum` | Int64 | Season total of plus-minus. |
| `aplus_dp_sum` | Int64 | Season total of `aplus_dp`, an unverified metric (not plus-minus or assists: Macura 2026-27 has 9 against plus-minus 8 and 3 assists). |
| `slam_dunk_sum` | Int64 | Season total of dunks. |
| `shots_2p_realized_sum` | Int64 | Season total of 2-point field goals made. |
| `shots_2p_total_sum` | Int64 | Season total of 2-point field goals attempted. |
| `shots_2p_perc_sum` | Float64 | Season 2-point percentage, 0-100 (a rate despite `_sum`). |
| `shots_3p_realized_sum` | Int64 | Season total of 3-point field goals made. |
| `shots_3p_total_sum` | Int64 | Season total of 3-point field goals attempted. |
| `shots_3p_perc_sum` | Float64 | Season 3-point percentage, 0-100 (a rate despite `_sum`). |
| `free_throws_realized_sum` | Int64 | Season total of free throws made. |
| `free_throws_total_sum` | Int64 | Season total of free throws attempted. |
| `free_throws_perc_sum` | Float64 | Season free-throw percentage, 0-100 (a rate despite `_sum`). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#lba_team_players_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (an unknown season, a play-by-play before 2017-18, standings of a finished championship without a round, no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an unknown id (the site answers HTTP 500), a game that is not final, a truncated page, or a body for another season, championship, game, team or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#lba_team_players_stats-example}

```python
lba_team_players_stats(season=2027, team_id=1764)
```

## lba_teams_leaderboard

Season team leaderboards, offensive or defensive, one row per team and category.

**Endpoint URL:** `GET https://www.legabasket.it/api/statistics/get-teams-statistics-by-type`

**Valid URL:** [https://www.legabasket.it/api/statistics/get-teams-statistics-by-type?cs_id=1&type=teams_offensive&s=2027](https://www.legabasket.it/api/statistics/get-teams-statistics-by-type?cs_id=1&type=teams_offensive&s=2027)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `board` |  | `Y` |  | Board: teams_offensive or teams_defensive. |
| `s` | `season` |  |  | `Y` | Season END year (2027 = 2026-27). Omitted = the site's default season (unverified). |

### Returns {#lba_teams_leaderboard-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season END year (2027 = 2026-27): the row's START year + 1. |
| `category` | String | Category of this row (the `cat` param, e.g. `points`). |
| `name` | String | Given name (players) / team name (teams). |
| `sponsor_name` | String | Sponsor name (empty or null in the samples). |
| `short_name` | String | Short name of the team, e.g. BC Roma. |
| `tv_name` | String | Short team name for TV graphics. |
| `year` | Int64 | Season START year (2026 = 2026-27); sdv END year = year + 1. |
| `logo_key` | String | Image key: the image is `{cdn_url}/{key}`, resized copies `{cdn_url}/variants/{key}/thumb` or `/large` (the site's getApiImageUrl). |
| `attempts` | Int64 | Shots attempted in the zone, parsed from num. |
| `total_played_matches` | Int64 | Games the team has played, counted for the leaderboard. |
| `total_points` | Int64 | Points scored (offensive) or allowed (defensive) in the season. |
| `minutes` | Int64 | Minutes per game (players: decimal string `28.67`) / total minutes (teams: integer). |
| `score_40_min` | Float64 | Value per 40 minutes. |
| `score` | Float64 | Value in the category under the `ob` order (default `avg` = per game); players send a decimal string, teams a number. |
| `team_id` | Int64 | Team id (team-season). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#lba_teams_leaderboard-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (an unknown season, a play-by-play before 2017-18, standings of a finished championship without a round, no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an unknown id (the site answers HTTP 500), a game that is not final, a truncated page, or a body for another season, championship, game, team or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#lba_teams_leaderboard-example}

```python
lba_teams_leaderboard(board='teams_offensive', season=2027)
```
