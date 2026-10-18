---
title: "LBA — Lega Basket Serie A site API (www.legabasket.it/api, Italy) — Team"
sidebar_label: "Team"
sidebar_position: 2
description: "LBA — Lega Basket Serie A site API (www.legabasket.it/api, Italy) — Team — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# LBA — Lega Basket Serie A site API (www.legabasket.it/api, Italy) — Team

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
| `game_id` | integer | Game id (global across seasons); joins lba_calendar and lba_game. |
| `season` | integer | Season END year (2027 = 2026-27): the row's START year + 1. |
| `side` | character | Which team of the game the row belongs to: home or visitor. |
| `team_id` | integer | Team-season id (re-issued every season); join lba_teams for the stable club_id. |
| `row_type` | character | team = stats booked to the team itself (team rebounds, team turnovers); totals = the whole side. |
| `pun` | integer | Points (`punti`). (team totals (players + team row)) |
| `falli_c` | integer | Fouls committed (`falli commessi`). (team totals (players + team row)) |
| `falli_sf` | integer | Fouls drawn (`falli subiti`). (team totals (players + team row)) |
| `falli_tc1` | integer | Technical fouls (`falli tecnici`; reading `tc1` as category-1 technicals is inferred). (team totals (players + team row)) |
| `falli_fla` | integer | Unsportsmanlike / flagrant fouls (`falli antisportivi`). (team totals (players + team row)) |
| `rimbalzi_o` | integer | Offensive rebounds (`rimbalzi offensivi`). (team totals (players + team row)) |
| `rimbalzi_d` | integer | Defensive rebounds (`rimbalzi difensivi`). (team totals (players + team row)) |
| `rimbalzi_t` | integer | Total rebounds (`rimbalzi totali`). (team totals (players + team row)) |
| `palle_p` | integer | Turnovers (`palle perse`). (team totals (players + team row)) |
| `palle_r` | integer | Steals (`palle recuperate`). (team totals (players + team row)) |
| `val_lega` | integer | LBA efficiency rating (`valutazione Lega`). (team totals (players + team row)) |
| `min` | integer | Minutes played, whole minutes (`minuti`); 200 in `totals` = 5 × 40. (team totals (players + team row)) |
| `sec` | integer | Seconds played (`secondi`) = `min` × 60 in every sample. (team totals (players + team row)) |
| `t2_r` | integer | 2-point field goals made (`tiri da 2 realizzati`). (team totals (players + team row)) |
| `t2_t` | integer | 2-point field goals attempted (`tiri da 2 tentati`). (team totals (players + team row)) |
| `t2_perc` | numeric | 2-point field-goal percentage, 0-100 (number; 0 when no attempt). (team totals (players + team row)) |
| `sc` | integer | Dunks (`schiacciate`). (team totals (players + team row)) |
| `t3_r` | integer | 3-point field goals made. (team totals (players + team row)) |
| `t3_t` | integer | 3-point field goals attempted. (team totals (players + team row)) |
| `t3_perc` | numeric | 3-point field-goal percentage, 0-100. (team totals (players + team row)) |
| `tl_r` | integer | Free throws made (`tiri liberi realizzati`). (team totals (players + team row)) |
| `tl_t` | integer | Free throws attempted (`tiri liberi tentati`). (team totals (players + team row)) |
| `tl_perc` | numeric | Free-throw percentage, 0-100. (team totals (players + team row)) |
| `stoppate_dat` | integer | Blocks (`stoppate date`). (team totals (players + team row)) |
| `stoppate_sub` | integer | Own shots blocked (`stoppate subite`). (team totals (players + team row)) |
| `ass` | integer | Assists. (team totals (players + team row)) |
| `val_oer` | numeric | OER = points / (FGA + turnovers + FTA / 2); verified on every 2026-27 sample row. (team totals (players + team row)) |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#lba_team_box_score-example}

```python
lba_team_box_score(game_id=25602)
```

_Last validated n/a._

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
| `season` | integer | Season END year (2027 = 2026-27): the row's START year + 1. |
| `team_id` | integer | Team id (one team-season). |
| `name` | character | Team name of that season (sponsor name). |
| `year` | integer | Season START year (2026 = 2026-27); sdv END year = year + 1. |
| `club_id` | integer | Club id: the cross-season join key for team ids (only here and in the `team` headers); stability across seasons is unverified. |
| `club_code` | character | Club code (`TVB`, `TS`); whether it is stable across seasons is unverified. |
| `updated_at` | character | Timestamp, ISO-8601 with the Italian offset. |
| `logo_key` | character | Image key: the image is `{cdn_url}/{key}`, resized copies `{cdn_url}/variants/{key}/thumb` or `/large` (the site's getApiImageUrl). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#lba_teams-example}

```python
lba_teams(season=2027)
```

_Last validated n/a._

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
| `team_id` | integer | Team-season id (re-issued every season); join lba_teams for the stable club_id. |
| `club_id` | integer | Club id: the cross-season join key for team ids (only here and in the `team` headers); stability across seasons is unverified. |
| `club_code` | character | Club code (`TVB`, `TS`); whether it is stable across seasons is unverified. |
| `season` | integer | Season END year (2027 = 2026-27): the row's START year + 1. |
| `player_id` | integer | Player id (global, one per career). |
| `player_name` | character | Player's given name. |
| `player_surname` | character | Player's family name. |
| `player_season` | integer | Season START year (2026 = 2026-27); sdv END year = year + 1. |
| `player_photo_key` | character | Image key: the image is `{cdn_url}/{key}`, resized copies `{cdn_url}/variants/{key}/thumb` or `/large` (the site's getApiImageUrl). |
| `played_matches` | integer | Games the player appeared in, in the request's championship series. |
| `total_matches` | integer | Team games while the player was listed (inferred). |
| `start_formation` | integer | Games the player started, in the request's championship series. |
| `points_sum` | integer | Season total of points. |
| `played_minutes_sum` | integer | Season total of minutes played. |
| `rating_lega_sum` | integer | Season total of LBA efficiency rating (`valutazione`). |
| `rating_oer_sum` | numeric | Sum of the per-game OER values (3.34 over 3 games), not a season OER. |
| `offensive_rebound_sum` | integer | Season total of offensive rebounds. |
| `defensive_rebound_sum` | integer | Season total of defensive rebounds. |
| `total_rebound_sum` | integer | Season total of total rebounds. |
| `assists_sum` | integer | Season total of assists. |
| `regain_balls_sum` | integer | Season total of steals. |
| `lost_balls_sum` | integer | Season total of turnovers. |
| `ball_stop_given_sum` | integer | Season total of blocks. |
| `ball_stop_received_sum` | integer | Season total of own shots blocked. |
| `done_fouls_sum` | integer | Season total of fouls committed. |
| `suffered_fouls_sum` | integer | Season total of fouls drawn. |
| `plus_minus_sum` | integer | Season total of plus-minus. |
| `aplus_dp_sum` | integer | Season total of `aplus_dp`, an unverified metric (not plus-minus or assists: Macura 2026-27 has 9 against plus-minus 8 and 3 assists). |
| `slam_dunk_sum` | integer | Season total of dunks. |
| `shots_2p_realized_sum` | integer | Season total of 2-point field goals made. |
| `shots_2p_total_sum` | integer | Season total of 2-point field goals attempted. |
| `shots_2p_perc_sum` | numeric | Season 2-point percentage, 0-100 (a rate despite `_sum`). |
| `shots_3p_realized_sum` | integer | Season total of 3-point field goals made. |
| `shots_3p_total_sum` | integer | Season total of 3-point field goals attempted. |
| `shots_3p_perc_sum` | numeric | Season 3-point percentage, 0-100 (a rate despite `_sum`). |
| `free_throws_realized_sum` | integer | Season total of free throws made. |
| `free_throws_total_sum` | integer | Season total of free throws attempted. |
| `free_throws_perc_sum` | numeric | Season free-throw percentage, 0-100 (a rate despite `_sum`). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#lba_team_players_stats-example}

```python
lba_team_players_stats(season=2027, team_id=1764)
```

_Last validated n/a._

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
| `season` | integer | Season END year (2027 = 2026-27): the row's START year + 1. |
| `category` | character | Category of this row (the `cat` param, e.g. `points`). |
| `name` | character | Given name (players) / team name (teams). |
| `sponsor_name` | character | Sponsor name (empty or null in the samples). |
| `short_name` | character | Short name of the team, e.g. BC Roma. |
| `tv_name` | character | Short team name for TV graphics. |
| `year` | integer | Season START year (2026 = 2026-27); sdv END year = year + 1. |
| `logo_key` | character | Image key: the image is `{cdn_url}/{key}`, resized copies `{cdn_url}/variants/{key}/thumb` or `/large` (the site's getApiImageUrl). |
| `attempts` | integer | Shots attempted in the zone, parsed from num. |
| `total_played_matches` | integer | Games the team has played, counted for the leaderboard. |
| `total_points` | integer | Points scored (offensive) or allowed (defensive) in the season. |
| `minutes` | integer | Minutes per game (players: decimal string `28.67`) / total minutes (teams: integer). |
| `score_40_min` | numeric | Value per 40 minutes. |
| `score` | numeric | Value in the category under the `ob` order (default `avg` = per game); players send a decimal string, teams a number. |
| `team_id` | integer | Team id (team-season). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#lba_teams_leaderboard-example}

```python
lba_teams_leaderboard(board='teams_offensive', season=2027)
```

_Last validated n/a._
