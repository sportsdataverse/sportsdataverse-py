---
title: LKL — LKL site API (lkl.lt, Lithuania)
sidebar_label: LKL site API (lkl.lt, Lithuania)
description: "LKL — LKL site API (lkl.lt, Lithuania) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
toc_max_heading_level: 2
---
# LKL — LKL site API (lkl.lt, Lithuania)

`sportsdataverse.basketball.lkl` — 14 endpoints.

> **Note:** Unofficial and unsupported: these are the routes lkl.lt's own game page calls (found in its front-end bundle) and its server-rendered pages. The site's robots.txt allows everything and sets no crawl-delay; `lkl_runtime` still spaces requests at least 1 s apart. SportsDataverse has not ruled on the site's terms, so payloads are not redistributed as release assets.

> **Note:** Coverage: game wrappers serve FINISHED games only (an unfinished game raises AssetFetchError). The box score, summary, score line, statistics and leaders answer for every season from 1993-94 (before 2024-25 the scoring splits and lead statistics are 0). Play-by-play and shots exist from 2024-25 only; an earlier finished game raises NoDataError. From 2016-17 the results season id is also the Genius LiveStats competition id (see the `genius` family) which holds play-by-play for older games; lkl.lt game, team and player ids are lkl.lt's own and do not join Genius ids. The King Mindaugas Cup shares the routes but is not wrapped. Text fields are Lithuanian.

## lkl_game_info

Game header from the game page: status, tip-off, venue, attendance, officials and both teams.

**Endpoint URL:** `GET https://lkl.lt/rungtynes/{game_id}`

**Valid URL:** [https://lkl.lt/rungtynes/11446](https://lkl.lt/rungtynes/11446)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Required. lkl.lt game id (integer), e.g. 11446; ``game_id`` in lkl_results. Not sequential by season before 2018-19. Only a finished game is served. |

### Returns {#lkl_game_info-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | lkl.lt game id (key space: LKL games; the {gameId} of every route). Not sequential by season before 2018-19; not the Genius matchId. A frame parsed from a bare body has a null game_id when the body does not echo it (the runtime stamps it under _request). |
| `game_status` | String | Game state label in Lithuanian: 'Pasibaigė' = finished, the only value served (the runtime refuses an unfinished game). |
| `start_date` | String | Game date (Europe/Vilnius), parsed from the page header 'YYYY m. <month> D d., HH:MM'. |
| `start_time` | String | Tip-off 'HH:MM' local time (Europe/Vilnius by the site's wording; the page carries no offset). |
| `arena` | String | Arena name as printed on the game page header. |
| `attendance` | String | Attendance text as printed ('1097 žiūrovai' = 1097 spectators). |
| `attendance_count` | Int64 | Attendance as an integer, parsed from the attendance text; null when the page has none. |
| `referees` | String | Referee names, comma-separated, crew chief first; 'Nepaskelbta' = not announced (old games). Names only: the site publishes no official's contact or birth data. |
| `has_actions` | Boolean | True when the game has play-by-play events (2024-25 onward); false on older games. |
| `is_streaming` | Boolean | True while a live stream is on; false on finished games. |
| `home_team_id` | Int64 | Home team id (key space: lkl.lt clubs, stable across seasons and renamings; joins the team_id of every other frame). Not the Genius team id. |
| `home_team` | String | Home team name as of that game (historical: 'Lietuvos Rytas' in 2007-08). |
| `home_short_name` | String | Home three-letter team code ('RYT'); can be the club's current code on old games. |
| `home_slug` | String | Home team slug (key space: lkl.lt clubs; the {slug} of lkl.lt/komandos/{slug}); joins the team slugs of lkl_results and lkl_standings. |
| `home_winner` | Boolean | True when the home team won the game. |
| `home_coach` | String | Home head coach's full name as stored for the team: on old games the CURRENT coach, not the historical one. |
| `home_assistant_coach` | String | Home assistant coach's full name (same caveat as the head coach: current staff on old games). |
| `away_team_id` | Int64 | Away team id (key space: lkl.lt clubs, stable across seasons and renamings; joins the team_id of every other frame). Not the Genius team id. |
| `away_team` | String | Away team name as of that game (historical: 'Lietuvos Rytas' in 2007-08). |
| `away_short_name` | String | Away three-letter team code ('RYT'); can be the club's current code on old games. |
| `away_slug` | String | Away team slug (key space: lkl.lt clubs; the {slug} of lkl.lt/komandos/{slug}); joins the team slugs of lkl_results and lkl_standings. |
| `away_winner` | Boolean | True when the away team won the game. |
| `away_coach` | String | Away head coach's full name as stored for the team: on old games the CURRENT coach, not the historical one. |
| `away_assistant_coach` | String | Away assistant coach's full name (same caveat as the head coach: current staff on old games). |

**`return_parsed=False`** — the raw body (`dict`): the JSON of a game route, the game page record, or the merged results / standings record, plus `_request` (the game id, team ids and slug -> player id map, or the season asked for).

### Raises {#lkl_game_info-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Lkl.lt has nothing for the request (an unknown game or standings season, a season the site's selects do not list, a finished game before 2024-25 asked for play-by-play or shots). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body or page for another game or season, an error or changed body, a truncated results list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#lkl_game_info-example}

```python
lkl_game_info(game_id=11446)
```

## lkl_game_players

Game roster from the game page: every player on either team sheet with the lkl.lt player id and slug.

**Endpoint URL:** `GET https://lkl.lt/rungtynes/{game_id}`

**Valid URL:** [https://lkl.lt/rungtynes/11446](https://lkl.lt/rungtynes/11446)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Required. lkl.lt game id (integer), e.g. 11446; ``game_id`` in lkl_results. Not sequential by season before 2018-19. Only a finished game is served. |

### Returns {#lkl_game_players-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | lkl.lt game id (key space: LKL games; the {gameId} of every route). Not sequential by season before 2018-19; not the Genius matchId. A frame parsed from a bare body has a null game_id when the body does not echo it (the runtime stamps it under _request). |
| `team_side` | String | Side of the game the row belongs to: home or away (the game page's teams_info side; the box score, leaders and plays say home / away, and gameStatistics is keyed by team id so the runtime supplies the side). |
| `team_id` | Int64 | lkl.lt team id (key space: lkl.lt clubs, STABLE across seasons and renamings: 6 = Rytas). Joins lkl_game_info home_team_id / away_team_id; not the Genius team id. Null on a frame parsed from a bare body. |
| `player_id` | Int64 | lkl.lt player id (key space: lkl.lt persons, stable across seasons). Joins the shot chart's player_id; the box score, leaders and play-by-play carry the slug, which this frame maps. |
| `player` | String | Full player name as the game page prints it. |
| `jersey` | String | Shirt number as a string (so '00' and '0' stay distinct). |
| `slug` | String | Player slug (key space: lkl.lt persons; the {slug} of lkl.lt/zaidejai/{slug}). Join key to the player_slug of the box score, leaders and play-by-play. |

**`return_parsed=False`** — the raw body (`dict`): the JSON of a game route, the game page record, or the merged results / standings record, plus `_request` (the game id, team ids and slug -> player id map, or the season asked for).

### Raises {#lkl_game_players-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Lkl.lt has nothing for the request (an unknown game or standings season, a season the site's selects do not list, a finished game before 2024-25 asked for play-by-play or shots). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body or page for another game or season, an error or changed body, a truncated results list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#lkl_game_players-example}

```python
lkl_game_players(game_id=11446)
```

## lkl_player_box

Per-player box score of a finished game: minutes, points, shooting splits, rebounds, assists and plus/minus.

**Endpoint URL:** `GET https://lkl.lt/api/livestream/boxscore/{game_id}`

**Valid URL:** [https://lkl.lt/api/livestream/boxscore/11446](https://lkl.lt/api/livestream/boxscore/11446)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Required. lkl.lt game id (integer), e.g. 11446; ``game_id`` in lkl_results. Not sequential by season before 2018-19. Only a finished game is served. |

### Returns {#lkl_player_box-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | lkl.lt game id (key space: LKL games; the {gameId} of every route). Not sequential by season before 2018-19; not the Genius matchId. A frame parsed from a bare body has a null game_id when the body does not echo it (the runtime stamps it under _request). |
| `team_side` | String | Side of the game the row belongs to: home or away (the game page's teams_info side; the box score, leaders and plays say home / away, and gameStatistics is keyed by team id so the runtime supplies the side). |
| `team_id` | Int64 | lkl.lt team id (key space: lkl.lt clubs, STABLE across seasons and renamings: 6 = Rytas). Joins lkl_game_info home_team_id / away_team_id; not the Genius team id. Null on a frame parsed from a bare body. |
| `player_slug` | String | lkl.lt player slug (key space: lkl.lt persons; the {slug} of lkl.lt/zaidejai/{slug}, stable across seasons). Join key to lkl_game_players.slug. |
| `player_id` | Int64 | lkl.lt player id (key space: lkl.lt persons, stable across seasons). Resolved from the player slug through the game roster the runtime stamps from the game page; null on a bare body or a slug the roster lacks. |
| `player` | String | Player name as the site prints it in this route (the box score short form 'I. Surname', other routes the full name). |
| `jersey` | String | Cell: Shirt number, a STRING ('77'); page column # (Marškinėlių numeris). |
| `is_starter` | Boolean | True for the five starters. |
| `is_captain` | Boolean | True for the team captain. |
| `is_active` | Boolean | False in every sample, finished games included; by name the player is on court right now (live games), unverified. |
| `seconds_played` | Int64 | Time on court in seconds (the page's 'mm:ss' minutes: 32:54 = 1974). |
| `points` | Int64 | Cell: Points (TŠK, Taškai). |
| `fg_made` | Int64 | Field goals made, 2PT + 3PT (the 'made' half of the 'made/attempted' cell). |
| `fg_attempted` | Int64 | Field goals attempted, 2PT + 3PT. |
| `fg_pct` | Float64 | Field-goal percentage, 0-100 (the site's one-decimal string; 0 when there were no attempts). |
| `fg2_made` | Int64 | Two-point field goals made. |
| `fg2_attempted` | Int64 | Two-point field goals attempted. |
| `fg2_pct` | Float64 | Two-point percentage, 0-100 (one-decimal string on the site; 0 when there were no attempts). |
| `fg3_made` | Int64 | Three-point field goals made. |
| `fg3_attempted` | Int64 | Three-point field goals attempted. |
| `fg3_pct` | Float64 | Three-point percentage, 0-100 (one-decimal string on the site; 0 when there were no attempts). |
| `ft_made` | Int64 | Free throws made. |
| `ft_attempted` | Int64 | Free throws attempted. |
| `ft_pct` | Float64 | Free-throw percentage, 0-100 (one-decimal string on the site; 0 when there were no attempts). |
| `total_rebounds` | Int64 | Cell: Total rebounds (AK, Atkovoti kamuoliai). |
| `offensive_rebounds` | Int64 | Cell: Offensive rebounds (AKP, Atkovoti kamuoliai puolime). |
| `defensive_rebounds` | Int64 | Cell: Defensive rebounds (AKG, Atkovoti kamuoliai gynyboje). |
| `assists` | Int64 | Cell: Assists (RP, Rezultatyvūs perdavimai). |
| `steals` | Int64 | Cell: Steals (PK, Perimti kamuoliai). |
| `turnovers` | Int64 | Cell: Turnovers (KL, Klaidos). |
| `blocks` | Int64 | Cell: Blocks made (BL, Blokai). |
| `blocks_received` | Int64 | Cell: Shots of this player/team that were blocked (GB, Gauti blokai). |
| `fouls` | Int64 | Cell: Personal fouls committed (AP, Asmeninės pražangos). |
| `fouls_on` | Int64 | Cell: Fouls drawn (IP, Išprovokuotos pražangos). |
| `plus_minus` | Int64 | Cell: Plus/minus while on court (+/- rodiklis). |
| `efficiency` | Int64 | Cell: Efficiency rating (EF, Efektyvumas; see gameStatistics efficiency). |

**`return_parsed=False`** — the raw body (`dict`): the JSON of a game route, the game page record, or the merged results / standings record, plus `_request` (the game id, team ids and slug -> player id map, or the season asked for).

### Raises {#lkl_player_box-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Lkl.lt has nothing for the request (an unknown game or standings season, a season the site's selects do not list, a finished game before 2024-25 asked for play-by-play or shots). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body or page for another game or season, an error or changed body, a truncated results list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#lkl_player_box-example}

```python
lkl_player_box(game_id=11446)
```

## lkl_team_box

Team game totals of both teams: shooting, rebounds, assists, turnovers, fouls, paint, bench and fast-break points.

**Endpoint URL:** `GET https://lkl.lt/api/livestream/boxscore/{game_id}`

**Valid URL:** [https://lkl.lt/api/livestream/boxscore/11446](https://lkl.lt/api/livestream/boxscore/11446)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Required. lkl.lt game id (integer), e.g. 11446; ``game_id`` in lkl_results. Not sequential by season before 2018-19. Only a finished game is served. |

### Returns {#lkl_team_box-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | lkl.lt game id (key space: LKL games; the {gameId} of every route). Not sequential by season before 2018-19; not the Genius matchId. A frame parsed from a bare body has a null game_id when the body does not echo it (the runtime stamps it under _request). |
| `team_id` | Int64 | lkl.lt team id (key space: lkl.lt clubs, STABLE across seasons and renamings: 6 = Rytas). Joins lkl_game_info home_team_id / away_team_id; not the Genius team id. Null on a frame parsed from a bare body. |
| `team_side` | String | Side of the game the row belongs to: home or away (the game page's teams_info side; the box score, leaders and plays say home / away, and gameStatistics is keyed by team id so the runtime supplies the side). |
| `points` | Int64 | Team points: the final score (the capture banks finished games only). |
| `seconds_played` | Int64 | Summed playing time of all the team's players in seconds (12000 = 5 x 40:00; 13500 with one overtime). |
| `field_goals_made_total` | Int64 | Field goals made, 2PT + 3PT. |
| `field_goals_attempted_total` | Int64 | Field goals attempted, 2PT + 3PT. |
| `field_goals_made_2pt` | Int64 | Two-point field goals made. |
| `field_goals_attempted_2pt` | Int64 | Two-point field goals attempted. |
| `field_goals_made_3pt` | Int64 | Three-point field goals made. |
| `field_goals_attempted_3pt` | Int64 | Three-point field goals attempted. |
| `free_throws_made` | Int64 | Free throws made by the team in the game (count). |
| `free_throws_attempted` | Int64 | Free throws attempted by the team in the game (count). |
| `offensive_rebounds` | Int64 | Team offensive rebounds in the game (count). |
| `defensive_rebounds` | Int64 | Team defensive rebounds in the game (count). |
| `total_rebounds` | Int64 | Team rebounds, offensive + defensive (count). |
| `assists` | Int64 | Team assists in the game (count). |
| `steals` | Int64 | Team steals in the game (count). |
| `blocks` | Int64 | Blocks made by the team in the game (count). |
| `blocks_received` | Int64 | Shots of this team that were blocked. |
| `turnovers` | Int64 | Team turnovers in the game (count). |
| `fouls` | Int64 | Fouls committed. |
| `fouls_on` | Int64 | Fouls drawn (fouls committed on this team). |
| `efficiency` | Int64 | Team efficiency rating (Efektyvumas), a PIR-style index (points + rebounds + assists + steals + blocks + fouls drawn - missed shots - turnovers - shots blocked - fouls). The exact formula is unverified: game 11590 home gives 121 where that formula gives 122. |
| `points_off_turnovers` | Int64 | Points off the opponent's turnovers; 0 on the 2007-08 sample (not tracked then). |
| `fast_break_points` | Int64 | Fast-break points; 0 on the 2007-08 sample. |
| `second_chance_points` | Int64 | Second-chance points (after an offensive rebound); 0 on the 2007-08 sample. |
| `points_in_paint` | Int64 | Points in the paint; 0 on the 2007-08 sample. |
| `bench_points` | Int64 | Points by players who did not start. |
| `biggest_lead` | Int64 | This team's biggest lead in points (0 when it never led). |
| `biggest_scoring_run` | Int64 | This team's longest unanswered scoring run, in points. |
| `lead_changes` | Int64 | Lead changes in the game (the same value on both rows). |
| `tie_results` | Int64 | Times the score was tied (the same value on both rows). |
| `time_leading` | Int64 | Seconds this team led during the game (the site sends it as a string; 1892 = 31:32). |
| `field_goals_percentage` | Float64 | Field-goal percentage, 0-100 float (the site sends a two-decimal string). |
| `field_goals_2pt_percentage` | Float64 | Two-point percentage, 0-100 float (the site sends a two-decimal string). |
| `field_goals_3pt_percentage` | Float64 | Three-point percentage, 0-100 float (the site sends a two-decimal string). |
| `free_throws_percentage` | Float64 | Free-throw percentage, 0-100 float (the site sends a two-decimal string). |

**`return_parsed=False`** — the raw body (`dict`): the JSON of a game route, the game page record, or the merged results / standings record, plus `_request` (the game id, team ids and slug -> player id map, or the season asked for).

### Raises {#lkl_team_box-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Lkl.lt has nothing for the request (an unknown game or standings season, a season the site's selects do not list, a finished game before 2024-25 asked for play-by-play or shots). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body or page for another game or season, an error or changed body, a truncated results list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#lkl_team_box-example}

```python
lkl_team_box(game_id=11446)
```

## lkl_period_scores

Points each team scored in each period of a game, overtime included.

**Endpoint URL:** `GET https://lkl.lt/api/livestream/boxscore/{game_id}`

**Valid URL:** [https://lkl.lt/api/livestream/boxscore/11446](https://lkl.lt/api/livestream/boxscore/11446)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Required. lkl.lt game id (integer), e.g. 11446; ``game_id`` in lkl_results. Not sequential by season before 2018-19. Only a finished game is served. |

### Returns {#lkl_period_scores-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | lkl.lt game id (key space: LKL games; the {gameId} of every route). Not sequential by season before 2018-19; not the Genius matchId. A frame parsed from a bare body has a null game_id when the body does not echo it (the runtime stamps it under _request). |
| `team_id` | Int64 | lkl.lt team id (key space: lkl.lt clubs, STABLE across seasons and renamings: 6 = Rytas). Joins lkl_game_info home_team_id / away_team_id; not the Genius team id. Null on a frame parsed from a bare body. |
| `team_side` | String | Side of the game the row belongs to: home or away (the game page's teams_info side; the box score, leaders and plays say home / away, and gameStatistics is keyed by team id so the runtime supplies the side). |
| `period` | Int64 | Period number: 1-4 regulation, 5 the first overtime, 6 the second. |
| `period_type` | String | regular for periods 1-4, overtime from period 5. |
| `points` | Int64 | Points the team scored IN this period (not cumulative); keyed by period in the site's quarter_scores. |

**`return_parsed=False`** — the raw body (`dict`): the JSON of a game route, the game page record, or the merged results / standings record, plus `_request` (the game id, team ids and slug -> player id map, or the season asked for).

### Raises {#lkl_period_scores-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Lkl.lt has nothing for the request (an unknown game or standings season, a season the site's selects do not list, a finished game before 2024-25 asked for play-by-play or shots). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body or page for another game or season, an error or changed body, a truncated results list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#lkl_period_scores-example}

```python
lkl_period_scores(game_id=11446)
```

## lkl_summary

Summary tab: the head-to-head comparison of both teams (shooting, rebounds, assists, bench points...).

**Endpoint URL:** `GET https://lkl.lt/api/livestream/summary/{game_id}`

**Valid URL:** [https://lkl.lt/api/livestream/summary/11446](https://lkl.lt/api/livestream/summary/11446)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Required. lkl.lt game id (integer), e.g. 11446; ``game_id`` in lkl_results. Not sequential by season before 2018-19. Only a finished game is served. |

### Returns {#lkl_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | lkl.lt game id (key space: LKL games; the {gameId} of every route). Not sequential by season before 2018-19; not the Genius matchId. A frame parsed from a bare body has a null game_id when the body does not echo it (the runtime stamps it under _request). |
| `group` | String | Always null here: the flat comparison list has no groups (lkl_game_stats labels them). |
| `stat` | String | English code for the site's Lithuanian label (field_goals, two_pointers, three_pointers, free_throws, rebounds, assists, steals, blocks, turnovers, fouls, points, ...); null for a label this package does not know. |
| `label` | String | Row label in LITHUANIAN: 'Metimai iš žaidimo' = field goals, 'Dvitaškiai metimai' = 2PT, 'Tritaškiai metimai' = 3PT, 'Baudų metimai' = free throws, 'Atkovoti kamuoliai' = rebounds, 'Atkovoti kamuoliai gynyboje' = defensive rebounds, 'Atkovoti kamuoliai puolime' = offensive rebounds, 'Rezultatyvūs perdavimai' = assists, 'Perimti kamuoliai' = steals, 'Blokai' = blocks, 'Klaidos' = turnovers, 'Pražangos' = fouls, 'Taškai' = points, 'Taškai po klaidų' = points off turnovers, 'Taškai greitose atakose' = fast-break points, 'Taškai antru bandymu' = second-chance points, 'Atsarginių žaidėjų taškai' = bench points. |
| `home_value` | String | Shooting rows: 'made/attempted' string ('25/35'); counting rows: integer. |
| `home_percentage` | Float64 | Shooting rows only (absent on counting rows): percentage with ONE decimal and a % sign ('71.4%'). |
| `home_graph_length` | Float64 | Bar length in percent: shooting rows = the percentage as a string ('71.4'); counting rows = this side's share of the two values, 0-100, a number (43 vs 37 -> 53.75). |
| `home_winning` | Boolean | True when this side's bar is highlighted as the better one (both false on a tie). |
| `away_value` | String | Shooting rows: 'made/attempted' string ('25/35'); counting rows: integer. |
| `away_percentage` | Float64 | Shooting rows only (absent on counting rows): percentage with ONE decimal and a % sign ('71.4%'). |
| `away_graph_length` | Float64 | Bar length in percent: shooting rows = the percentage as a string ('71.4'); counting rows = this side's share of the two values, 0-100, a number (43 vs 37 -> 53.75). |
| `away_winning` | Boolean | True when this side's bar is highlighted as the better one (both false on a tie). |

**`return_parsed=False`** — the raw body (`dict`): the JSON of a game route, the game page record, or the merged results / standings record, plus `_request` (the game id, team ids and slug -> player id map, or the season asked for).

### Raises {#lkl_summary-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Lkl.lt has nothing for the request (an unknown game or standings season, a season the site's selects do not list, a finished game before 2024-25 asked for play-by-play or shots). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body or page for another game or season, an error or changed body, a truncated results list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#lkl_summary-example}

```python
lkl_summary(game_id=11446)
```

## lkl_game_score

Score line of a game: both scores and the clock text.

**Endpoint URL:** `GET https://lkl.lt/api/livestream/game-score/{game_id}`

**Valid URL:** [https://lkl.lt/api/livestream/game-score/11446](https://lkl.lt/api/livestream/game-score/11446)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Required. lkl.lt game id (integer), e.g. 11446; ``game_id`` in lkl_results. Not sequential by season before 2018-19. Only a finished game is served. |

### Returns {#lkl_game_score-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | lkl.lt game id (key space: LKL games; the {gameId} of every route). Not sequential by season before 2018-19; not the Genius matchId. A frame parsed from a bare body has a null game_id when the body does not echo it (the runtime stamps it under _request). |
| `home_score` | Int64 | Home team points on the score line. |
| `away_score` | Int64 | Away team points on the score line. |
| `clock` | String | Clock text, NOT a state flag: an old game keeps '1 KĖLINYS - 10:00' after the final; a finished overtime game reads '1 pratęsimas - 00:00'. |

**`return_parsed=False`** — the raw body (`dict`): the JSON of a game route, the game page record, or the merged results / standings record, plus `_request` (the game id, team ids and slug -> player id map, or the season asked for).

### Raises {#lkl_game_score-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Lkl.lt has nothing for the request (an unknown game or standings season, a season the site's selects do not list, a finished game before 2024-25 asked for play-by-play or shots). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body or page for another game or season, an error or changed body, a truncated results list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#lkl_game_score-example}

```python
lkl_game_score(game_id=11446)
```

## lkl_game_stats

Statistics tab: the head-to-head comparison of both teams, one row per statistic.

**Endpoint URL:** `GET https://lkl.lt/api/livestream/game-stats/{game_id}`

**Valid URL:** [https://lkl.lt/api/livestream/game-stats/11446](https://lkl.lt/api/livestream/game-stats/11446)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Required. lkl.lt game id (integer), e.g. 11446; ``game_id`` in lkl_results. Not sequential by season before 2018-19. Only a finished game is served. |

### Returns {#lkl_game_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | lkl.lt game id (key space: LKL games; the {gameId} of every route). Not sequential by season before 2018-19; not the Genius matchId. A frame parsed from a bare body has a null game_id when the body does not echo it (the runtime stamps it under _request). |
| `group` | String | Bar group the tab also shows the line in: shooting (2PT, 3PT, free throws) or main (rebounds, assists, steals); null for lines only in the full list. |
| `stat` | String | English code for the site's Lithuanian label (field_goals, two_pointers, three_pointers, free_throws, rebounds, assists, steals, blocks, turnovers, fouls, points, ...); null for a label this package does not know. |
| `label` | String | Row label in LITHUANIAN: 'Metimai iš žaidimo' = field goals, 'Dvitaškiai metimai' = 2PT, 'Tritaškiai metimai' = 3PT, 'Baudų metimai' = free throws, 'Atkovoti kamuoliai' = rebounds, 'Atkovoti kamuoliai gynyboje' = defensive rebounds, 'Atkovoti kamuoliai puolime' = offensive rebounds, 'Rezultatyvūs perdavimai' = assists, 'Perimti kamuoliai' = steals, 'Blokai' = blocks, 'Klaidos' = turnovers, 'Pražangos' = fouls, 'Taškai' = points, 'Taškai po klaidų' = points off turnovers, 'Taškai greitose atakose' = fast-break points, 'Taškai antru bandymu' = second-chance points, 'Atsarginių žaidėjų taškai' = bench points. |
| `home_value` | String | Shooting rows: 'made/attempted' string ('25/35'); counting rows: integer. |
| `home_percentage` | Float64 | Shooting rows only (absent on counting rows): percentage with ONE decimal and a % sign ('71.4%'). |
| `home_graph_length` | Float64 | Bar length in percent: shooting rows = the percentage as a string ('71.4'); counting rows = this side's share of the two values, 0-100, a number (43 vs 37 -> 53.75). |
| `home_winning` | Boolean | True when this side's bar is highlighted as the better one (both false on a tie). |
| `away_value` | String | Shooting rows: 'made/attempted' string ('25/35'); counting rows: integer. |
| `away_percentage` | Float64 | Shooting rows only (absent on counting rows): percentage with ONE decimal and a % sign ('71.4%'). |
| `away_graph_length` | Float64 | Bar length in percent: shooting rows = the percentage as a string ('71.4'); counting rows = this side's share of the two values, 0-100, a number (43 vs 37 -> 53.75). |
| `away_winning` | Boolean | True when this side's bar is highlighted as the better one (both false on a tie). |

**`return_parsed=False`** — the raw body (`dict`): the JSON of a game route, the game page record, or the merged results / standings record, plus `_request` (the game id, team ids and slug -> player id map, or the season asked for).

### Raises {#lkl_game_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Lkl.lt has nothing for the request (an unknown game or standings season, a season the site's selects do not list, a finished game before 2024-25 asked for play-by-play or shots). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body or page for another game or season, an error or changed body, a truncated results list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#lkl_game_stats-example}

```python
lkl_game_stats(game_id=11446)
```

## lkl_lead_chart

Lead chart of a game: the score margin at each chart point of each period.

**Endpoint URL:** `GET https://lkl.lt/api/livestream/game-stats/{game_id}`

**Valid URL:** [https://lkl.lt/api/livestream/game-stats/11446](https://lkl.lt/api/livestream/game-stats/11446)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Required. lkl.lt game id (integer), e.g. 11446; ``game_id`` in lkl_results. Not sequential by season before 2018-19. Only a finished game is served. |

### Returns {#lkl_lead_chart-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | lkl.lt game id (key space: LKL games; the {gameId} of every route). Not sequential by season before 2018-19; not the Genius matchId. A frame parsed from a bare body has a null game_id when the body does not echo it (the runtime stamps it under _request). |
| `period` | Int64 | Period number: 1-4 regulation, 5 the first overtime, 6 the second. |
| `diff` | Int64 | Home minus away score at that chart point (signed integer). |
| `clock` | String | Clock REMAINING in the period as the site's 'mm:ss:00' text (a trailing ':00'): '10:00:00' is the period start. |
| `score_home` | Int64 | Home cumulative score at that point (the site sends a string). |
| `score_away` | Int64 | Away cumulative score at that point (the site sends a string). |
| `column_height` | Float64 | Bar height for the site's chart: \|diff\| / maxDiff x 50 (percent of the chart). |

**`return_parsed=False`** — the raw body (`dict`): the JSON of a game route, the game page record, or the merged results / standings record, plus `_request` (the game id, team ids and slug -> player id map, or the season asked for).

### Raises {#lkl_lead_chart-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Lkl.lt has nothing for the request (an unknown game or standings season, a season the site's selects do not list, a finished game before 2024-25 asked for play-by-play or shots). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body or page for another game or season, an error or changed body, a truncated results list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#lkl_lead_chart-example}

```python
lkl_lead_chart(game_id=11446)
```

## lkl_leaders

Leaders tab: each team's top players in points, rebounds, assists, efficiency, steals and blocks.

**Endpoint URL:** `GET https://lkl.lt/api/livestream/leaders/{game_id}`

**Valid URL:** [https://lkl.lt/api/livestream/leaders/11446](https://lkl.lt/api/livestream/leaders/11446)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Required. lkl.lt game id (integer), e.g. 11446; ``game_id`` in lkl_results. Not sequential by season before 2018-19. Only a finished game is served. |

### Returns {#lkl_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | lkl.lt game id (key space: LKL games; the {gameId} of every route). Not sequential by season before 2018-19; not the Genius matchId. A frame parsed from a bare body has a null game_id when the body does not echo it (the runtime stamps it under _request). |
| `category` | String | Leader category: points, total_rebounds, assists, efficiency, steals or blocks. |
| `category_label` | String | Category label in Lithuanian ('Pelnomi taškai' = points scored, 'Naudingumas' = efficiency rating). |
| `team_side` | String | Side of the game the row belongs to: home or away (the game page's teams_info side; the box score, leaders and plays say home / away, and gameStatistics is keyed by team id so the runtime supplies the side). |
| `team_id` | Int64 | lkl.lt team id (key space: lkl.lt clubs, STABLE across seasons and renamings: 6 = Rytas). Joins lkl_game_info home_team_id / away_team_id; not the Genius team id. Null on a frame parsed from a bare body. |
| `rank` | Int64 | Place in the team's category list: 1 = the category leader, 2 and up = the runners-up in the order the site lists them (up to 4). |
| `player` | String | Player name as the site prints it in this route (the box score short form 'I. Surname', other routes the full name). |
| `player_slug` | String | lkl.lt player slug (key space: lkl.lt persons; the {slug} of lkl.lt/zaidejai/{slug}, stable across seasons). Join key to lkl_game_players.slug. |
| `player_id` | Int64 | lkl.lt player id (key space: lkl.lt persons, stable across seasons). Resolved from the player slug through the game roster the runtime stamps from the game page; null on a bare body or a slug the roster lacks. |
| `value` | Int64 | The player's count in the row's category. |

**`return_parsed=False`** — the raw body (`dict`): the JSON of a game route, the game page record, or the merged results / standings record, plus `_request` (the game id, team ids and slug -> player id map, or the season asked for).

### Raises {#lkl_leaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Lkl.lt has nothing for the request (an unknown game or standings season, a season the site's selects do not list, a finished game before 2024-25 asked for play-by-play or shots). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body or page for another game or season, an error or changed body, a truncated results list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#lkl_leaders-example}

```python
lkl_leaders(game_id=11446)
```

## lkl_pbp

Play-by-play of a finished game (2024-25 onward), oldest first, with the running score and the lkl.lt player id.

**Endpoint URL:** `GET https://lkl.lt/api/livestream/play-by-play/{game_id}`

**Valid URL:** [https://lkl.lt/api/livestream/play-by-play/11446](https://lkl.lt/api/livestream/play-by-play/11446)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Required. lkl.lt game id (integer), e.g. 11446; ``game_id`` in lkl_results. Not sequential by season before 2018-19. Only a finished game is served. |

### Returns {#lkl_pbp-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | lkl.lt game id (key space: LKL games; the {gameId} of every route). Not sequential by season before 2018-19; not the Genius matchId. A frame parsed from a bare body has a null game_id when the body does not echo it (the runtime stamps it under _request). |
| `event_num` | Int64 | Play sequence number the parser assigns, 1 = the first play of the game (the site lists plays newest first per period, with no id). |
| `period` | Int64 | Period of the play. Regulation lines carry it in the site's body; overtime lines come as one merged list without it, so the parser derives it (oldest overtime = 5, each period-end line closes one): checked on a one-overtime game only, so multi-overtime numbering is unverified. |
| `period_type` | String | regular for periods 1-4, overtime from period 5. |
| `clock` | String | Game clock REMAINING in the period as 'mm:ss' (it counts down from 10:00, from 05:00 in overtime). |
| `clock_seconds` | Int64 | The clock REMAINING as whole seconds (08:34 = 514), derived by the parser. |
| `team_side` | String | Side of the acting team (the line's is_home flag): home or away. |
| `team_id` | Int64 | lkl.lt team id (key space: lkl.lt clubs, STABLE across seasons and renamings: 6 = Rytas). Joins lkl_game_info home_team_id / away_team_id; not the Genius team id. Null on a frame parsed from a bare body. |
| `player` | String | Player name as the site prints it in this route (the box score short form 'I. Surname', other routes the full name). |
| `player_slug` | String | lkl.lt player slug (key space: lkl.lt persons; the {slug} of lkl.lt/zaidejai/{slug}, stable across seasons). Join key to lkl_game_players.slug. |
| `player_id` | Int64 | lkl.lt player id (key space: lkl.lt persons, stable across seasons). Resolved from the player slug through the game roster the runtime stamps from the game page; null on a bare body or a slug the roster lacks. |
| `type` | String | Event type (English code): 2pt, 3pt, freethrow, rebound, assist, steal, block, turnover, foul, foulon (foul drawn), substitution, timeout, jumpball, headcoachchallenge, period, game. |
| `subtype` | String | Event subtype (English code; '' when none). 2pt/3pt: shot kind (jumpshot, layup, drivinglayup, reverselayup, dunk, alleyoop, alleyoopdunk, tipinlayup, tipindunk, hookshot, fadeaway, floatingjumpshot, pullupjumpshot, stepbackjumpshot, turnaroundjumpshot, eurostep). freethrow: 'NofM' = attempt N of M (1of1, 1of2, 2of2, 1of3, 2of3, 3of3). rebound: offensive / defensive. foul: personal, offensive, technical, benchTechnical. turnover: badpass, ballhandling, offensive, outofbounds, travel, backcourt (backcourt violation), 3sec, 24sec. substitution: in / out. timeout: full (team), officials. jumpball: startperiod, won, lost, heldball. headcoachchallenge: the challenged call (foul, outofbounds). period / game: start, end. |
| `description` | String | Event text in Lithuanian, one fixed phrase per type and subtype ('3 taškų metimas pataikytas' = 3-point shot made). |
| `success` | Boolean | Shots and free throws: true = made. Every other type: always true. |
| `is_points_info` | Boolean | True on lines that changed the score (made shots and free throws). |
| `score_home` | Int64 | Home points after the play (on every line, not only scoring lines). |
| `score_away` | Int64 | Away points after the play (on every line, not only scoring lines). |

**`return_parsed=False`** — the raw body (`dict`): the JSON of a game route, the game page record, or the merged results / standings record, plus `_request` (the game id, team ids and slug -> player id map, or the season asked for).

### Raises {#lkl_pbp-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Lkl.lt has nothing for the request (an unknown game or standings season, a season the site's selects do not list, a finished game before 2024-25 asked for play-by-play or shots). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body or page for another game or season, an error or changed body, a truncated results list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#lkl_pbp-example}

```python
lkl_pbp(game_id=11446)
```

## lkl_shots

Every field-goal attempt of a finished game (2024-25 onward) with its court position (Genius LiveStats data).

**Endpoint URL:** `GET https://lkl.lt/api/livestream/shot-chart/{game_id}`

**Valid URL:** [https://lkl.lt/api/livestream/shot-chart/11446](https://lkl.lt/api/livestream/shot-chart/11446)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Required. lkl.lt game id (integer), e.g. 11446; ``game_id`` in lkl_results. Not sequential by season before 2018-19. Only a finished game is served. |

### Returns {#lkl_shots-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | lkl.lt game id (key space: LKL games; the {gameId} of every route). Not sequential by season before 2018-19; not the Genius matchId. A frame parsed from a bare body has a null game_id when the body does not echo it (the runtime stamps it under _request). |
| `shot_id` | Int64 | Shot event id (key space: lkl.lt shot events, increasing over time); joins nothing else and is not the Genius action number. |
| `period` | Int64 | Period number: 1-4 regulation, 5 the first overtime, 6 the second. |
| `period_type` | String | regular for periods 1-4, overtime from period 5. |
| `team_side` | String | Side of the shooter's team (the line's is_home flag): home or away. |
| `team_id` | Int64 | lkl.lt team id (key space: lkl.lt clubs, STABLE across seasons and renamings: 6 = Rytas). Joins lkl_game_info home_team_id / away_team_id; not the Genius team id. Null on a frame parsed from a bare body. |
| `player_id` | Int64 | lkl.lt player id (key space: lkl.lt persons, stable across seasons). Resolved from the player slug through the game roster the runtime stamps from the game page; null on a bare body or a slug the roster lacks. |
| `player_slug` | String | lkl.lt player slug (key space: lkl.lt persons; the {slug} of lkl.lt/zaidejai/{slug}, stable across seasons). Join key to lkl_game_players.slug. |
| `type` | String | 2pt or 3pt (free throws are not in the chart). |
| `made` | Boolean | True when the shot was made, false when missed. |
| `x` | Float64 | Court x position: percent (0-100) of the LENGTH of a horizontal full court from the left baseline (baskets near 5 and 95). Teams switch baskets at half-time; metres (x / 100 x 28) are an inference. |
| `y` | Float64 | Court y position: percent (0-100) of the court WIDTH (baskets at 50). Metres (y / 100 x 15) are an inference; which sideline is y = 0 is unverified. |
| `side` | String | Basket half the shot is at: LEFT (x < 50) or RIGHT, except backcourt heaves (area backcourt). |
| `area` | String | Court zone code (e.g. inthepaint, underbasket, outsideleft); left and right are from the shooter's view, so zones are point-symmetric between the two baskets. |

**`return_parsed=False`** — the raw body (`dict`): the JSON of a game route, the game page record, or the merged results / standings record, plus `_request` (the game id, team ids and slug -> player id map, or the season asked for).

### Raises {#lkl_shots-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Lkl.lt has nothing for the request (an unknown game or standings season, a season the site's selects do not list, a finished game before 2024-25 asked for play-by-play or shots). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body or page for another game or season, an error or changed body, a truncated results list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#lkl_shots-example}

```python
lkl_shots(game_id=11446)
```

## lkl_results

Every played game of one LKL season (all pages of the site's results list, merged), newest first.

**Endpoint URL:** `GET https://lkl.lt/loadResults`

**Valid URL:** [https://lkl.lt/loadResults?season=2026](https://lkl.lt/loadResults?season=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Required. Season END year (2026 = the 2025-26 season). The site keys a season by two opaque ids (a results id and a standings id, neither a year); ``lkl_runtime`` reads them from the site's season selects by the label ``YYYY-YYYY m. sezonas``. A year the selects do not list raises NoDataError. |

### Returns {#lkl_results-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season_id` | Int64 | Results season id (key space: lkl.lt championships; LKL 2026-27 = 49436). From 2016-17 it is the Genius LiveStats competition id (leagueId 110). |
| `season_end_year` | Int64 | Season END year (2026 = 2025-26), read from the season label 'YYYY-YYYY m. sezonas'. |
| `game_id` | Int64 | lkl.lt game id (key space: LKL games; the {gameId} of every route). Not sequential by season before 2018-19; not the Genius matchId. A frame parsed from a bare body has a null game_id when the body does not echo it (the runtime stamps it under _request). |
| `date` | String | Game date from the site's Lithuanian day header; checked to fall in the season (Aug 1 - Jul 31). |
| `weekday` | String | Weekday of the header, translated to English ('šeštadienis' -> 'Saturday'). |
| `time` | String | Tip-off 'HH:MM', Europe/Vilnius. |
| `arena` | String | Arena name ('Arena dar nežinoma' = arena not yet known, on some 2007-08 games). |
| `city` | String | City, from 'Arena , City'; null when the venue line has no city (2007-08 'Arena dar nežinoma'). |
| `home_slug` | String | Home team: Team slug (key space: lkl.lt clubs; the {slug} of https://lkl.lt/komandos/{slug}; one per club, stable across renamings: 'rytas' for Lietuvos Rytas / Rytas). The results and standings rows carry only this; the game page teams_info gives slug + id. |
| `home_code` | String | Home three-letter team code ('LIE'). |
| `home_name` | String | Home team name as printed (the logo alt text; the CURRENT club name even on old games: 2007-08 'Rytas'). |
| `home_score` | Int64 | Home team points in the game (the final score). |
| `away_slug` | String | Away team: Team slug (key space: lkl.lt clubs; the {slug} of https://lkl.lt/komandos/{slug}; one per club, stable across renamings: 'rytas' for Lietuvos Rytas / Rytas). The results and standings rows carry only this; the game page teams_info gives slug + id. |
| `away_code` | String | Away three-letter team code. |
| `away_name` | String | Away team name as printed. |
| `away_score` | Int64 | Away team points in the game (the final score). |
| `winner_highlighted` | String | Which side's score the page sets in bold: home or away (the winner). |

**`return_parsed=False`** — the raw body (`dict`): the JSON of a game route, the game page record, or the merged results / standings record, plus `_request` (the game id, team ids and slug -> player id map, or the season asked for).

### Raises {#lkl_results-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Lkl.lt has nothing for the request (an unknown game or standings season, a season the site's selects do not list, a finished game before 2024-25 asked for play-by-play or shots). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body or page for another game or season, an error or changed body, a truncated results list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#lkl_results-example}

```python
lkl_results(season=2026)
```

## lkl_standings

League table of one LKL season: overall, home and away records, and the final ranking once it is set.

**Endpoint URL:** `GET https://lkl.lt/loadStandings/{season}`

**Valid URL:** [https://lkl.lt/loadStandings/2026](https://lkl.lt/loadStandings/2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Required. Season END year (2026 = the 2025-26 season). The site keys a season by two opaque ids (a results id and a standings id, neither a year); ``lkl_runtime`` reads them from the site's season selects by the label ``YYYY-YYYY m. sezonas``. A year the selects do not list raises NoDataError. |

### Returns {#lkl_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season_id` | Int64 | STANDINGS season id (a separate small key space: 35 = 2026-27, 1 = 2016-17, 24 = 1993-94); NOT the results season id. |
| `season_end_year` | Int64 | Season END year (2026 = 2025-26), read from the season label 'YYYY-YYYY m. sezonas'. |
| `table` | String | Which table the row comes from: full (overall), home, away or final (the final ranking). |
| `position` | Int64 | Rank in the table (Poz.); in the final table the final place. |
| `team_slug` | String | Team slug (key space: lkl.lt clubs; the {slug} of https://lkl.lt/komandos/{slug}; one per club, stable across renamings: 'rytas' for Lietuvos Rytas / Rytas). The results and standings rows carry only this; the game page teams_info gives slug + id. |
| `team_name` | String | Team name as printed for that season ('Lietuvos Rytas' in 2007-08). |
| `played` | Int64 | Games played (Rng., rungtynės); null in the final table. |
| `won` | Int64 | Wins (Per., pergalės); null in the final table. |
| `lost` | Int64 | Losses (Pr., pralaimėjimai); null in the final table. |
| `win_pct` | Float64 | Win percentage 0-100 (Laim.%); null in the home, away and final tables and when the site prints '-' (2007-08). |
| `streak` | Int64 | Current streak (Serija) as a signed integer: 27 = 27 wins in a row, -1 = one loss. Null outside the full table. |
| `points_for_avg` | Float64 | Points scored per game (first number of Tšk. vid.); null in the final table. |
| `points_against_avg` | Float64 | Points allowed per game (second number of Tšk. vid.); null in the final table. |
| `last5` | String | Last five results (Paskutinės 5) as W / L letters in the order the page renders them, left to right; fewer than five early in a season; null in the final table. |

**`return_parsed=False`** — the raw body (`dict`): the JSON of a game route, the game page record, or the merged results / standings record, plus `_request` (the game id, team ids and slug -> player id map, or the season asked for).

### Raises {#lkl_standings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Lkl.lt has nothing for the request (an unknown game or standings season, a season the site's selects do not list, a finished game before 2024-25 asked for play-by-play or shots). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a body or page for another game or season, an error or changed body, a truncated results list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#lkl_standings-example}

```python
lkl_standings(season=2026)
```
