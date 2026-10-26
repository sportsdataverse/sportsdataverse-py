---
title: Spanish FEB — FEB LiveStats API + baloncestoenvivo.feb.es (Spain)
sidebar_label: FEB LiveStats API + baloncestoenvivo.feb.es (Spain)
description: "Spanish FEB — FEB LiveStats API + baloncestoenvivo.feb.es (Spain) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
toc_max_heading_level: 2
---
# Spanish FEB — FEB LiveStats API + baloncestoenvivo.feb.es (Spain)

`sportsdataverse.basketball.feb` — 11 endpoints.

> **Note:** Legal notice: the FEB's aviso legal asserts intellectual-property rights over the site's content and databases (the EU database right). It has no explicit scraping clause and robots.txt returns 404 on both hosts, but SportsDataverse has not ruled on it: fetching on your own machine is fine; a published data release or a hosted mirror needs a legal sign-off first.

> **Note:** Unofficial and unsupported: the LiveStats API is the federation's own widget backend, not a documented public API. It needs a bearer JWT, which is public: `feb_runtime` reads it from the site's home page at run time, keeps it in memory for the process (24 h validity) and never prints, logs or writes it.

> **Note:** Coverage: the game routes answer from 2020-21 (Liga Femenina Endesa from its first game; Primera FEB from 2021-03-12, so its earlier 2020-21 games answer 404, as do all earlier seasons). 2020-21 play-by-play is text only. The schedule pages go back to 1996-97 (`season=1997`). Segunda FEB and the youth and Tercera FEB groups share the same routes; `group` is the site's menu group (1 Primera FEB, 2 Segunda FEB, 4 Liga Femenina Endesa).

> **Note:** Seasons are END years everywhere in this package (`season=2027` is 2026-27); the site's own `t` is the START year, so the runtime sends `t = season - 1` (`t=2026` is 2026-27).

## feb_game_info

Game header: competition, tip-off, venue, officials, status, both teams and the score (every game route carries it; this wrapper reads the lightest, /TeamStats).

**Endpoint URL:** `GET https://intrafeb.feb.es/LiveStats.API/api/v1/TeamStats/{game_id}`

**Valid URL:** [https://intrafeb.feb.es/LiveStats.API/api/v1/TeamStats/2535725](https://intrafeb.feb.es/LiveStats.API/api/v1/TeamStats/2535725)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Required. FEB game id (integer), e.g. 2535725; ``game_id`` in feb_schedule / feb_results / feb_overview. |

### Returns {#feb_game_info-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | FEB game id (one global sequence): the {gameId} of every LiveStats route and the p= of Partido.aspx. Game bodies do not print it, so it comes from the request; a frame parsed from a bare body has a null game_id. |
| `comp_id` | Int64 | Competition id (constant across seasons): 115 Primera FEB (LEB Oro), 118 Segunda FEB, 210 Liga Femenina Endesa, 214 LF Challenge, 218 LF-2, 111 Tercera FEB. Not the site's menu group. |
| `competition` | String | Competition name as printed that season, uppercase ('PRIMERA FEB'; 2020-21 'LIGA LEB ORO'). |
| `group_label` | String | The game's group (grupo) label, trimmed: 'Único' = the single group, '"A"' = group A. It is the page's group label, not the site's menu group number. |
| `period` | Int64 | Current or last period number; 4 for a finished regulation game. |
| `clock` | String | 'FINAL' once the game is over, else the game clock 'mm:ss' remaining in the period. |
| `start_time` | String | Tip-off in Spanish local time (Europe/Madrid) as a naive datetime: the source carries no offset, so the zone is assumed. |
| `venue` | String | Venue (pabellon) name. |
| `address` | String | Venue street address, free text. |
| `referee1` | String | Crew chief (arbitro principal), 'SURNAMES, NAME'. |
| `referee2` | String | First umpire (arbitro auxiliar 1), 'SURNAMES, NAME'. |
| `referee3` | String | Second umpire (arbitro auxiliar 2), 'SURNAMES, NAME'; null when the game had two referees. |
| `status` | Int64 | Game state code: 3 = finished (status_text FINISHED). Null on 2020-21 games. |
| `status_text` | String | Game state label in English ('FINISHED'). Null on 2020-21 games. |
| `home_team_id` | Int64 | Home team-season id (key space: FEB team registrations, new every season). |
| `home_team` | String | Home team name for the season (sponsor name, uppercase). |
| `home_team_code` | String | Home federation team code, 8 digits as a string. Null on 2020-21 games. |
| `home_club_code` | String | Home federation club code, 6 digits as a string. Null on 2020-21 games. |
| `home_score` | Int64 | Home points: the final score when clock is FINAL, else the live score. |
| `away_team_id` | Int64 | Away team-season id (key space: FEB team registrations, new every season). |
| `away_team` | String | Away team name for the season (sponsor name, uppercase). |
| `away_team_code` | String | Away federation team code, 8 digits as a string. Null on 2020-21 games. |
| `away_club_code` | String | Away federation club code, 6 digits as a string. Null on 2020-21 games. |
| `away_score` | Int64 | Away points: the final score when clock is FINAL, else the live score. |

**`return_parsed=False`** — the raw body (`dict`): the LiveStats envelope (all ten blocks, only the route's filled) or the schedule page record, plus `_request` (the game id, or group and season asked for).

### Raises {#feb_game_info-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The FEB has nothing for the request (a game the API does not serve, an unknown group, a schedule page with no games, an empty overview window). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected bearer token, an error or changed envelope, a page for another season or group). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#feb_game_info-example}

```python
feb_game_info(game_id=2535725)
```

## feb_period_scores

Points each team scored in each period of a game.

**Endpoint URL:** `GET https://intrafeb.feb.es/LiveStats.API/api/v1/TeamStats/{game_id}`

**Valid URL:** [https://intrafeb.feb.es/LiveStats.API/api/v1/TeamStats/2535725](https://intrafeb.feb.es/LiveStats.API/api/v1/TeamStats/2535725)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Required. FEB game id (integer), e.g. 2535725; ``game_id`` in feb_schedule / feb_results / feb_overview. |

### Returns {#feb_period_scores-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | FEB game id (one global sequence): the {gameId} of every LiveStats route and the p= of Partido.aspx. Game bodies do not print it, so it comes from the request; a frame parsed from a bare body has a null game_id. |
| `period` | Int64 | Period number (1-4 regulation; overtime numbering is unverified, no overtime game is in the samples). |
| `home_points` | Int64 | Points the home team scored IN this period (not cumulative). |
| `away_points` | Int64 | Points the away team scored IN this period (not cumulative). |

**`return_parsed=False`** — the raw body (`dict`): the LiveStats envelope (all ten blocks, only the route's filled) or the schedule page record, plus `_request` (the game id, or group and season asked for).

### Raises {#feb_period_scores-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The FEB has nothing for the request (a game the API does not serve, an unknown group, a schedule page with no games, an empty overview window). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected bearer token, an error or changed envelope, a page for another season or group). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#feb_period_scores-example}

```python
feb_period_scores(game_id=2535725)
```

## feb_team_stats

Team totals of both teams in a game: shooting, rebounds, assists, turnovers, fouls.

**Endpoint URL:** `GET https://intrafeb.feb.es/LiveStats.API/api/v1/TeamStats/{game_id}`

**Valid URL:** [https://intrafeb.feb.es/LiveStats.API/api/v1/TeamStats/2535725](https://intrafeb.feb.es/LiveStats.API/api/v1/TeamStats/2535725)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Required. FEB game id (integer), e.g. 2535725; ``game_id`` in feb_schedule / feb_results / feb_overview. |

### Returns {#feb_team_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | FEB game id (one global sequence): the {gameId} of every LiveStats route and the p= of Partido.aspx. Game bodies do not print it, so it comes from the request; a frame parsed from a bare body has a null game_id. |
| `team_side` | String | Side of the game the row belongs to: home or away (the feed's TEAM[0] / TEAM[1], or its 1/2 and 0/1 side codes, normalised). |
| `team_id` | Int64 | Team-season id (key space: FEB team registrations; a NEW id every season, e.g. HLA Alicante = 979989 in 2025-26 and 1008687 in 2026-27). Joins the schedule's home_team_id / away_team_id; not a club or player id. |
| `team` | String | Team name as registered for the season (sponsor name, uppercase: 'HLA ALICANTE'). |
| `team_code` | String | Federation team code, 8 digits as a string: the 6-digit club_code plus a 2-digit team suffix ('03058600'). Null on 2020-21 games. |
| `club_code` | String | Federation club code, 6 digits as a string (the first 6 digits of team_code). Whether it is stable across seasons is unverified. Null on 2020-21 games. |
| `seconds_played` | Int64 | Seconds on court (the feed's min, an integer count of seconds: 1064 = 17:44; a team row sums its players, 12000 = 200:00). Null on /TeamStats rows. |
| `min_formatted` | String | Time played as 'mm:ss'; '0:00' on /TeamStats rows (only box-score totals carry a value). |
| `pts` | Int64 | Team points: the final score when the game is over, else the live score. |
| `p1m` | Int64 | Free throws made (tiros libres; integer as a string). |
| `p1a` | Int64 | Free throws attempted (integer as a string). |
| `p1p` | Float64 | Free-throw percentage, 0-100, as a string with a DECIMAL COMMA ('77,3'); '0,0' when no attempts. |
| `p2m` | Int64 | Two-point field goals made (integer as a string). |
| `p2a` | Int64 | Two-point field goals attempted (integer as a string). |
| `p2p` | Float64 | Two-point percentage, 0-100, string with a decimal comma ('54,1'). |
| `p3m` | Int64 | Three-point field goals made (integer as a string). |
| `p3a` | Int64 | Three-point field goals attempted (integer as a string). |
| `p3p` | Float64 | Three-point percentage, 0-100, string with a decimal comma ('47,6'). |
| `fgm` | Int64 | Field goals made, 2PT + 3PT (tiros de campo; integer as a string). |
| `fga` | Int64 | Field goals attempted, 2PT + 3PT (integer as a string). |
| `fgp` | Float64 | Field-goal percentage, 0-100, string with a decimal comma ('51,7'). |
| `ro` | Int64 | Offensive rebounds (rebotes ofensivos; integer as a string). |
| `rd` | Int64 | Defensive rebounds (rebotes defensivos; integer as a string). |
| `rt` | Int64 | Total rebounds (rebotes totales; integer as a string). |
| `assist` | Int64 | Assists (asistencias; integer as a string). |
| `st` | Int64 | Steals (balones robados, page column BR; integer as a string). |
| `to` | Int64 | Turnovers (balones perdidos, page column BP; integer as a string). |
| `bs` | Int64 | Blocks made (tapones a favor, page column TF; integer as a string). |
| `tc` | Int64 | Shots of this team that were blocked (tapones en contra, page column TC). Null on /TeamStats rows. |
| `mt` | Int64 | Dunks (mates, page column MT). Null on /TeamStats rows. |
| `pf` | Int64 | Personal fouls committed (faltas cometidas, page column FC; integer as a string). |
| `rf` | Int64 | Fouls received / drawn (faltas recibidas, page column FR; integer as a string). |
| `fouls` | Int64 | Team fouls committed in the game (equals pf on the samples). |
| `val` | Int64 | FEB efficiency rating (valoracion). Null on /TeamStats rows, filled on box-score totals. |
| `fastb` | Int64 | Fast-break points. Null on every sampled game: the federation's stat crews do not fill it. |
| `ptsto` | Int64 | Points off turnovers. Null on every sampled game: the federation's stat crews do not fill it. |
| `secpts` | Int64 | Second-chance points. Null on every sampled game: the federation's stat crews do not fill it. |
| `bench` | Int64 | Bench points. Null on every sampled game: the federation's stat crews do not fill it. |
| `paint` | Int64 | Points in the paint. Null on every sampled game: the federation's stat crews do not fill it. |

**`return_parsed=False`** — the raw body (`dict`): the LiveStats envelope (all ten blocks, only the route's filled) or the schedule page record, plus `_request` (the game id, or group and season asked for).

### Raises {#feb_team_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The FEB has nothing for the request (a game the API does not serve, an unknown group, a schedule page with no games, an empty overview window). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected bearer token, an error or changed envelope, a page for another season or group). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#feb_team_stats-example}

```python
feb_team_stats(game_id=2535725)
```

## feb_player_box

Per-player box score of a game: minutes, points, shooting splits, rebounds, assists, fouls and plus/minus.

**Endpoint URL:** `GET https://intrafeb.feb.es/LiveStats.API/api/v1/BoxScore/{game_id}`

**Valid URL:** [https://intrafeb.feb.es/LiveStats.API/api/v1/BoxScore/2535725](https://intrafeb.feb.es/LiveStats.API/api/v1/BoxScore/2535725)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Required. FEB game id (integer), e.g. 2535725; ``game_id`` in feb_schedule / feb_results / feb_overview. |

### Returns {#feb_player_box-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | FEB game id (one global sequence): the {gameId} of every LiveStats route and the p= of Partido.aspx. Game bodies do not print it, so it comes from the request; a frame parsed from a bare body has a null game_id. |
| `team_side` | String | Side of the game the row belongs to: home or away (the feed's TEAM[0] / TEAM[1], or its 1/2 and 0/1 side codes, normalised). |
| `team_id` | Int64 | Team-season id (key space: FEB team registrations; a NEW id every season, e.g. HLA Alicante = 979989 in 2025-26 and 1008687 in 2026-27). Joins the schedule's home_team_id / away_team_id; not a club or player id. |
| `team` | String | Team name as registered for the season (sponsor name, uppercase: 'HLA ALICANTE'). |
| `player_id` | Int64 | FEB person id: stable across seasons and clubs (A. Jorda Rodriguez = 1676991 in 2025-26 and 2026-27). Joins the box score, play-by-play, shots and leaders; not the per-season licence_id. |
| `shirt_number` | String | Shirt number (dorsal) as a string, so '00' and '0' stay distinct. The shot feed names the shooter by this number. |
| `player` | String | Player name as 'I. SURNAME(S)', uppercase ('V. AGUILAR HERNANDEZ'). |
| `starter` | Boolean | True when the player was in the starting five (the page's I column), false when on the bench. |
| `captain` | Boolean | True for the team captain. Null on 2020-21 games, where the feed does not send it. |
| `seconds_played` | Int64 | Seconds on court (the feed's min, an integer count of seconds: 1064 = 17:44; a team row sums its players, 12000 = 200:00). Null on /TeamStats rows. |
| `min_formatted` | String | Time played as 'mm:ss'. |
| `pts` | Int64 | Points scored (the team's points on team rows; the final score when the clock is FINAL, else live). |
| `p1m` | Int64 | Free throws made (tiros libres; integer as a string). |
| `p1a` | Int64 | Free throws attempted (integer as a string). |
| `p1p` | Float64 | Free-throw percentage, 0-100, as a string with a DECIMAL COMMA ('77,3'); '0,0' when no attempts. |
| `p2m` | Int64 | Two-point field goals made (integer as a string). |
| `p2a` | Int64 | Two-point field goals attempted (integer as a string). |
| `p2p` | Float64 | Two-point percentage, 0-100, string with a decimal comma ('54,1'). |
| `p3m` | Int64 | Three-point field goals made (integer as a string). |
| `p3a` | Int64 | Three-point field goals attempted (integer as a string). |
| `p3p` | Float64 | Three-point percentage, 0-100, string with a decimal comma ('47,6'). |
| `fgm` | Int64 | Field goals made, 2PT + 3PT (tiros de campo; integer as a string). |
| `fga` | Int64 | Field goals attempted, 2PT + 3PT (integer as a string). |
| `fgp` | Float64 | Field-goal percentage, 0-100, string with a decimal comma ('51,7'). |
| `ro` | Int64 | Offensive rebounds (rebotes ofensivos; integer as a string). |
| `rd` | Int64 | Defensive rebounds (rebotes defensivos; integer as a string). |
| `rt` | Int64 | Total rebounds (rebotes totales; integer as a string). |
| `reb` | Int64 | Total rebounds (the same count as rt). |
| `assist` | Int64 | Assists (asistencias; integer as a string). |
| `st` | Int64 | Steals (balones robados, page column BR; integer as a string). |
| `to` | Int64 | Turnovers (balones perdidos, page column BP; integer as a string). |
| `bs` | Int64 | Blocks made (tapones a favor, page column TF; integer as a string). |
| `tc` | Int64 | Shots of this player/team that were blocked (tapones en contra / recibidos, page column TC; integer as a string). |
| `mt` | Int64 | Dunks (mates, page column MT; integer as a string). |
| `pf` | Int64 | Personal fouls committed (faltas cometidas, page column FC; integer as a string). |
| `rf` | Int64 | Fouls received / drawn (faltas recibidas, page column FR; integer as a string). |
| `pllss` | Int64 | Plus/minus while the player was on court (balance, page column +/-), signed integer. |
| `val` | Int64 | FEB efficiency rating (valoracion): points + rebounds + assists + steals + blocks + fouls drawn - missed shots - turnovers - shots blocked - fouls committed. |

**`return_parsed=False`** — the raw body (`dict`): the LiveStats envelope (all ten blocks, only the route's filled) or the schedule page record, plus `_request` (the game id, or group and season asked for).

### Raises {#feb_player_box-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The FEB has nothing for the request (a game the API does not serve, an unknown group, a schedule page with no games, an empty overview window). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected bearer token, an error or changed envelope, a page for another season or group). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#feb_player_box-example}

```python
feb_player_box(game_id=2535725)
```

## feb_pbp

Play-by-play of a game (KeyFacts): every shot, rebound, foul, substitution and timeout, in Spanish text with coded actions from 2025-26.

**Endpoint URL:** `GET https://intrafeb.feb.es/LiveStats.API/api/v1/KeyFacts/{game_id}`

**Valid URL:** [https://intrafeb.feb.es/LiveStats.API/api/v1/KeyFacts/2535725](https://intrafeb.feb.es/LiveStats.API/api/v1/KeyFacts/2535725)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Required. FEB game id (integer), e.g. 2535725; ``game_id`` in feb_schedule / feb_results / feb_overview. |

### Returns {#feb_pbp-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | FEB game id (one global sequence): the {gameId} of every LiveStats route and the p= of Partido.aspx. Game bodies do not print it, so it comes from the request; a frame parsed from a bare body has a null game_id. |
| `play_num` | Int64 | Play sequence number, 1 = the first play of the game; unique per game. The parser sorts by it (the feed lists newest first). |
| `period` | Int64 | Period number of the play. |
| `clock` | String | Game clock REMAINING in the period as 'mm:ss' (it counts down from 10:00). |
| `clock_seconds` | Int64 | The clock as whole seconds remaining in the period (08:34 = 514), derived by the parser. |
| `team_side` | String | Side of the acting team: home (feed code 1) or away (2); null on period lines. |
| `team_id` | Int64 | Team-season id of the acting team (new every season); null on period lines and 2020-21 games. |
| `player_id` | Int64 | FEB person id of the actor (stable across seasons); null on team, period and 2020-21 lines. |
| `license_id` | Int64 | Per-season player licence id of the actor (changes when the player re-registers; A. Jorda Rodriguez = 10901171 in 2025-26, 11266668 in 2026-27). Null on team and period lines and 2020-21 games. |
| `action` | String | Action code: shoot, fthrow, rebound, assist, recovery (steal), lose (turnover), blockshot, foul, subst, timeout, period. Null on 2020-21 games. |
| `text` | String | Play description in Spanish: '(TEAM) I. SURNAME: ACTION (running count)'; 2020-21 games use older wording and are text only. |
| `score_home` | Int64 | Home score AFTER the play; filled on scoring plays only. |
| `score_away` | Int64 | Away score AFTER the play; filled on scoring plays only. |
| `x` | Float64 | Court x position of a shoot line: percent (0-100) of the length of a horizontal full-court image, origin top-left (the same scale as feb_shots). Null when the action has none and on 2020-21 games. |
| `y` | Float64 | Court y position of a shoot line: percent (0-100) of the width from the top edge (the same scale as feb_shots). Null when the action has none and on 2020-21 games. |
| `log_param1` | String | Action parameter 1 (string). Team and player actions: a pseudo-GUID '00000000-0000-000{side}-0000-{game id, 12 digits}' (side 1 home, 2 away); period lines: the period number. |
| `log_param2` | String | Action parameter 2 (string). The actor's licence id on shoot, fthrow, foul, rebound, assist, recovery and blockshot lines; period lines: '0' start, '1' end; lose: '0' or '1' (meaning unverified). |
| `log_param3` | String | Action parameter 3 (string). fthrow: '1' made, '0' missed; subst: the outgoing player's licence; foul: a foul-type code. Others unverified. |
| `log_param4` | String | Action parameter 4 (string). shoot and fthrow: '1' made, '0' missed; foul: the licence of the fouled player; lose: a licence (role unverified). |
| `log_param5` | String | Action parameter 5 (string); action-dependent, meaning unverified. |
| `log_param6` | String | Action parameter 6 (string). shoot: shot value '2' or '3'; fthrow: '1'; foul: free throws awarded. |
| `log_param7` | String | Action parameter 7 (string); action-dependent, meaning unverified. |
| `log_param8` | String | Action parameter 8 (string): a small integer on shoot, foul and lose lines (by distribution a zone or sub-type code); unverified. |
| `log_param9` | String | Action parameter 9 (string); blank or null in every sampled game. |
| `log_param10` | String | Action parameter 10 (string): '1' on almost every foul; meaning unverified. |
| `log_time` | String | Wall-clock time the play was logged, Spanish local time, naive datetime. Null on 2020-21 games. |
| `log_time_utc` | String | The log time in UTC, naive datetime. Null on 2020-21 games. |

**`return_parsed=False`** — the raw body (`dict`): the LiveStats envelope (all ten blocks, only the route's filled) or the schedule page record, plus `_request` (the game id, or group and season asked for).

### Raises {#feb_pbp-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The FEB has nothing for the request (a game the API does not serve, an unknown group, a schedule page with no games, an empty overview window). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected bearer token, an error or changed envelope, a page for another season or group). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#feb_pbp-example}

```python
feb_pbp(game_id=2535725)
```

## feb_shots

Every field-goal attempt of a game with its court position (percent of a full-court image).

**Endpoint URL:** `GET https://intrafeb.feb.es/LiveStats.API/api/v1/ShotChart/{game_id}`

**Valid URL:** [https://intrafeb.feb.es/LiveStats.API/api/v1/ShotChart/2535725](https://intrafeb.feb.es/LiveStats.API/api/v1/ShotChart/2535725)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Required. FEB game id (integer), e.g. 2535725; ``game_id`` in feb_schedule / feb_results / feb_overview. |

### Returns {#feb_shots-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | FEB game id (one global sequence): the {gameId} of every LiveStats route and the p= of Partido.aspx. Game bodies do not print it, so it comes from the request; a frame parsed from a bare body has a null game_id. |
| `period` | Int64 | Period number of the attempt. |
| `clock` | String | Game clock REMAINING in the period at the shot, 'mm:ss' (the play-by-play line's time). |
| `clock_seconds` | Int64 | The clock as whole seconds remaining in the period (08:34 = 514), derived by the parser. |
| `team_side` | String | Side of the shooter: home (feed code 0) or away (feed code 1; play-by-play uses 1/2). |
| `team_id` | Int64 | Team-season id of the shooter's team (from the shot chart's TEAM list); new every season. |
| `shirt_number` | String | Shirt number (dorsal) as a string, so '00' and '0' stay distinct. The shot feed names the shooter by this number. |
| `player_id` | Int64 | FEB person id of the shooter, resolved from side + shirt number in the game's roster; null when that pair is not unique. The feed itself carries no player id. |
| `made` | Boolean | True when the shot was made, false when missed. Free throws are not in the feed. |
| `x` | Float64 | Court x position: percent (0-100) of the length of a horizontal full-court image, origin top-left. Teams change ends at half-time; metres (x/100 x 28) are an unverified inference. |
| `y` | Float64 | Court y position: percent (0-100) of the width of a horizontal full-court image from the top edge; baskets sit near y = 50. Metres (y/100 x 15) are an unverified inference. |

**`return_parsed=False`** — the raw body (`dict`): the LiveStats envelope (all ten blocks, only the route's filled) or the schedule page record, plus `_request` (the game id, or group and season asked for).

### Raises {#feb_shots-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The FEB has nothing for the request (a game the API does not serve, an unknown group, a schedule page with no games, an empty overview window). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected bearer token, an error or changed envelope, a page for another season or group). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#feb_shots-example}

```python
feb_shots(game_id=2535725)
```

## feb_leaders

Each team's top players of a game by points, rebounds, assists and fouls.

**Endpoint URL:** `GET https://intrafeb.feb.es/LiveStats.API/api/v1/Ranking/{game_id}`

**Valid URL:** [https://intrafeb.feb.es/LiveStats.API/api/v1/Ranking/2535725](https://intrafeb.feb.es/LiveStats.API/api/v1/Ranking/2535725)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Required. FEB game id (integer), e.g. 2535725; ``game_id`` in feb_schedule / feb_results / feb_overview. |

### Returns {#feb_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | FEB game id (one global sequence): the {gameId} of every LiveStats route and the p= of Partido.aspx. Game bodies do not print it, so it comes from the request; a frame parsed from a bare body has a null game_id. |
| `team_side` | String | Side of the game the row belongs to: home or away (the feed's TEAM[0] / TEAM[1], or its 1/2 and 0/1 side codes, normalised). |
| `team_id` | Int64 | Team-season id (key space: FEB team registrations; a NEW id every season, e.g. HLA Alicante = 979989 in 2025-26 and 1008687 in 2026-27). Joins the schedule's home_team_id / away_team_id; not a club or player id. |
| `team` | String | Team name as registered for the season (sponsor name, uppercase: 'HLA ALICANTE'). |
| `category` | String | Leader category: points, rebounds, assists or fouls (the feed's Puntos, Rebotes, Asistencias, Faltas). |
| `rank` | Int64 | Place in the team's category list, 1 = the leader, in the order the feed lists them. |
| `player_id` | Int64 | FEB person id: stable across seasons and clubs (A. Jorda Rodriguez = 1676991 in 2025-26 and 2026-27). Joins the box score, play-by-play, shots and leaders; not the per-season licence_id. |
| `shirt_number` | String | Shirt number (dorsal) as a string, so '00' and '0' stay distinct. The shot feed names the shooter by this number. |
| `player` | String | Player name as 'I. SURNAME(S)', uppercase ('V. AGUILAR HERNANDEZ'). |
| `starter` | Boolean | True when the player was in the starting five (the page's I column), false when on the bench. |
| `captain` | Boolean | True for the team captain. Null on 2020-21 games, where the feed does not send it. |
| `value` | Int64 | The player's count in the row's category (points, rebounds, assists or personal fouls). |

**`return_parsed=False`** — the raw body (`dict`): the LiveStats envelope (all ten blocks, only the route's filled) or the schedule page record, plus `_request` (the game id, or group and season asked for).

### Raises {#feb_leaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The FEB has nothing for the request (a game the API does not serve, an unknown group, a schedule page with no games, an empty overview window). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected bearer token, an error or changed envelope, a page for another season or group). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#feb_leaders-example}

```python
feb_leaders(game_id=2535725)
```

## feb_overview

Today's (or the latest) games of every live FEB competition, with scores and status.

**Endpoint URL:** `GET https://intrafeb.feb.es/LiveStats.API/api/v1/overview`

**Valid URL:** [https://intrafeb.feb.es/LiveStats.API/api/v1/overview](https://intrafeb.feb.es/LiveStats.API/api/v1/overview)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#feb_overview-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `comp_id` | Int64 | Competition id (constant across seasons): 115 Primera FEB (LEB Oro), 118 Segunda FEB, 210 Liga Femenina Endesa, 214 LF Challenge, 218 LF-2, 111 Tercera FEB. Not the site's menu group. |
| `comp_code` | Int64 | Season-specific competition edition code (e.g. 23473 = Primera FEB 2026-27). |
| `competition` | String | Competition name as printed that season, uppercase ('PRIMERA FEB'; 2020-21 'LIGA LEB ORO'). |
| `game_id` | Int64 | FEB game id (one global sequence): the {gameId} of every LiveStats route. |
| `round_id` | Int64 | Round id; 0 in every sample (the route does not fill it). |
| `stage` | String | Stage code as a string; '0' in every sample (meaning unverified). |
| `venue` | String | Name of the venue (pabellon) where the game is played. |
| `start_time` | String | Tip-off in Spanish local time as a naive datetime (the source has no offset). |
| `start_time_utc` | String | Tip-off in UTC as a naive datetime. |
| `period` | Int64 | Current or last period (4 when finished). |
| `clock` | String | 'FINAL' once finished, else the game clock ('00:00' before tip-off). |
| `status` | Int64 | Game state code: 3 = finished, 1 = scheduled (seen 2026-10-12); the live codes are unverified. |
| `home_team_id` | Int64 | Home team-season id (new every season; joins the schedule pages). |
| `home_team` | String | Home team name for the season. |
| `home_team_code` | String | Home federation team code, 8 digits as a string. |
| `home_club_code` | String | Home federation club code, 6 digits as a string. |
| `home_abbreviation` | String | Home team three-letter abbreviation ('PAL'). |
| `home_score` | Int64 | Home points (0 before tip-off). |
| `away_team_id` | Int64 | Away team-season id (new every season; joins the schedule pages). |
| `away_team` | String | Away team name for the season. |
| `away_team_code` | String | Away federation team code, 8 digits as a string. |
| `away_club_code` | String | Away federation club code, 6 digits as a string. |
| `away_abbreviation` | String | Away team three-letter abbreviation. |
| `away_score` | Int64 | Away points (0 before tip-off). |
| `period_scores` | String | Points per period as 'home-away' pairs joined by ';' ('16-16;19-16;10-21;14-14'); null before tip-off. |

**`return_parsed=False`** — the raw body (`dict`): the LiveStats envelope (all ten blocks, only the route's filled) or the schedule page record, plus `_request` (the game id, or group and season asked for).

### Raises {#feb_overview-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The FEB has nothing for the request (a game the API does not serve, an unknown group, a schedule page with no games, an empty overview window). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected bearer token, an error or changed envelope, a page for another season or group). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#feb_overview-example}

```python
feb_overview()
```

## feb_schedule

Full schedule of one group (phase) of a competition season: every round, game, result and team-season id.

**Endpoint URL:** `GET https://baloncestoenvivo.feb.es/calendario.aspx`

**Valid URL:** [https://baloncestoenvivo.feb.es/calendario.aspx?group=1&season=2027](https://baloncestoenvivo.feb.es/calendario.aspx?group=1&season=2027)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `group` | `group` |  | `Y` |  | Required. The site's menu group (integer): 1 Primera FEB, 2 Segunda FEB, 4 Liga Femenina Endesa, 9 and 10 LF-2, 67 LF Challenge, 73 Copa de España, 74 Liga U. A group is not a competition id (see ``comp_id``); an unknown group raises NoDataError. |
| `season` | `season` |  | `Y` |  | Season END year (2027 = 2026-27). ``feb_runtime`` sends the site's START-year key ``t = season - 1``. |

### Returns {#feb_schedule-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `group` | Int64 | The site's menu group (1 Primera FEB, 2 Segunda FEB, 4 Liga Femenina Endesa); not a competition id. |
| `season` | Int64 | Season END year (2027 = 2026-27), the site's START-year key plus one. |
| `competition` | String | Competition name on the page header ('PRIMERA FEB'). |
| `round` | Int64 | Round number (jornada) the game belongs to. |
| `round_label` | String | Round heading as printed ('Jornada 4 16/10/2026'). |
| `round_date` | String | Date of the round (the heading's date); played games carry no date of their own on this page. |
| `game_id` | Int64 | FEB game id (one global sequence): the {gameId} of every LiveStats route and the p= of Partido.aspx. Game bodies do not print it, so it comes from the request; a frame parsed from a bare body has a null game_id. |
| `home_team_id` | Int64 | Home team-season id (new every season; joins feb_overview and the game wrappers). |
| `home_team` | String | Home team name, uppercase. |
| `away_team_id` | Int64 | Away team-season id (new every season). |
| `away_team` | String | Away team name, uppercase. |
| `result_text` | String | Result cell verbatim: 'home-away' points when played; before the game the calendar shows the Spanish weekday, date and time and the results page shows '*-*'. |
| `home_score` | Int64 | Home points; null before the game. |
| `away_score` | Int64 | Away points; null before the game. |
| `status` | String | played when the result cell holds a score, scheduled otherwise. |
| `weekday` | String | English weekday of a scheduled game (translated from the Spanish cell); null for played games. |
| `date` | String | Game date; null for played games on the schedule page (use round_date). |
| `time` | String | Tip-off 'HH:MM', Spanish local time; null for played games on the schedule page. |

**`return_parsed=False`** — the raw body (`dict`): the LiveStats envelope (all ten blocks, only the route's filled) or the schedule page record, plus `_request` (the game id, or group and season asked for).

### Raises {#feb_schedule-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The FEB has nothing for the request (a game the API does not serve, an unknown group, a schedule page with no games, an empty overview window). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected bearer token, an error or changed envelope, a page for another season or group). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#feb_schedule-example}

```python
feb_schedule(group=1, season=2027)
```

## feb_results

Latest round's results and the next round's fixtures of a competition season.

**Endpoint URL:** `GET https://baloncestoenvivo.feb.es/resultados.aspx`

**Valid URL:** [https://baloncestoenvivo.feb.es/resultados.aspx?group=1&season=2027](https://baloncestoenvivo.feb.es/resultados.aspx?group=1&season=2027)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `group` | `group` |  | `Y` |  | Required. The site's menu group (integer): 1 Primera FEB, 2 Segunda FEB, 4 Liga Femenina Endesa, 9 and 10 LF-2, 67 LF Challenge, 73 Copa de España, 74 Liga U. A group is not a competition id (see ``comp_id``); an unknown group raises NoDataError. |
| `season` | `season` |  | `Y` |  | Season END year (2027 = 2026-27). ``feb_runtime`` sends the site's START-year key ``t = season - 1``. |

### Returns {#feb_results-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `group` | Int64 | The site's menu group (1 Primera FEB, 2 Segunda FEB, 4 Liga Femenina Endesa); not a competition id. |
| `season` | Int64 | Season END year (2027 = 2026-27), the site's START-year key plus one. |
| `competition` | String | Competition name on the page header ('PRIMERA FEB'). |
| `comp_id` | Int64 | Competition id (CompID) read from the page; ties the site group to the API (group 1 -> 115, 2 -> 118, 4 -> 210). |
| `section` | String | Which table the row comes from: results (the latest round) or next_round (the following fixtures). |
| `round_title` | String | Title of the table as printed ('Resultados Jornada 3(09/10/2026)'). |
| `game_id` | Int64 | FEB game id (one global sequence): the {gameId} of every LiveStats route and the p= of Partido.aspx. Game bodies do not print it, so it comes from the request; a frame parsed from a bare body has a null game_id. |
| `home_team_id` | Int64 | Home team-season id (new every season; joins feb_overview and the game wrappers). |
| `home_team` | String | Home team name, uppercase. |
| `away_team_id` | Int64 | Away team-season id (new every season). |
| `away_team` | String | Away team name, uppercase. |
| `result_text` | String | Result cell verbatim: 'home-away' points when played; before the game the calendar shows the Spanish weekday, date and time and the results page shows '*-*'. |
| `home_score` | Int64 | Home points; null before the game. |
| `away_score` | Int64 | Away points; null before the game. |
| `status` | String | played when the result cell holds a score, scheduled otherwise. |
| `date` | String | Game date; null for played games on the schedule page (use round_date). |
| `time` | String | Tip-off 'HH:MM', Spanish local time; null for played games on the schedule page. |

**`return_parsed=False`** — the raw body (`dict`): the LiveStats envelope (all ten blocks, only the route's filled) or the schedule page record, plus `_request` (the game id, or group and season asked for).

### Raises {#feb_results-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The FEB has nothing for the request (a game the API does not serve, an unknown group, a schedule page with no games, an empty overview window). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected bearer token, an error or changed envelope, a page for another season or group). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#feb_results-example}

```python
feb_results(group=1, season=2027)
```

## feb_standings

League table of a competition season (live standings, or the final classification once it is over).

**Endpoint URL:** `GET https://baloncestoenvivo.feb.es/resultados.aspx`

**Valid URL:** [https://baloncestoenvivo.feb.es/resultados.aspx?group=1&season=2027](https://baloncestoenvivo.feb.es/resultados.aspx?group=1&season=2027)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `group` | `group` |  | `Y` |  | Required. The site's menu group (integer): 1 Primera FEB, 2 Segunda FEB, 4 Liga Femenina Endesa, 9 and 10 LF-2, 67 LF Challenge, 73 Copa de España, 74 Liga U. A group is not a competition id (see ``comp_id``); an unknown group raises NoDataError. |
| `season` | `season` |  | `Y` |  | Season END year (2027 = 2026-27). ``feb_runtime`` sends the site's START-year key ``t = season - 1``. |

### Returns {#feb_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `group` | Int64 | The site's menu group (1 Primera FEB, 2 Segunda FEB, 4 Liga Femenina Endesa); not a competition id. |
| `season` | Int64 | Season END year (2027 = 2026-27), the site's START-year key plus one. |
| `competition` | String | Competition name on the page header ('PRIMERA FEB'). |
| `comp_id` | Int64 | Competition id (CompID) read from the page; ties the site group to the API (group 1 -> 115, 2 -> 118, 4 -> 210). |
| `table` | String | live = the in-season league table; final = the final classification once the season is over. |
| `position` | Int64 | Rank in the table (Po). |
| `team_id` | Int64 | Team-season id (new every season; joins the schedule and game wrappers). |
| `team` | String | Team name as printed in the table (sponsor name, uppercase). |
| `played` | Int64 | Games played (PJ); null in the final classification. |
| `won` | Int64 | Games won (PG); null in the final classification. |
| `lost` | Int64 | Games lost (PP); null in the final classification. |
| `points_for` | Int64 | Points scored (PF); null in the final classification. |
| `points_against` | Int64 | Points conceded (PC); null in the final classification. |
| `point_diff` | Int64 | Point difference (DIF), signed; null in the final classification. |
| `win_pct` | Float64 | Win percentage, 0-100 (the page's '100,0%', decimal comma converted). |
| `streak` | Int64 | Current streak as a signed integer: +3 = three wins in a row, -2 = two losses. |
| `streak_text` | String | The streak cell verbatim ('+3'). |

**`return_parsed=False`** — the raw body (`dict`): the LiveStats envelope (all ten blocks, only the route's filled) or the schedule page record, plus `_request` (the game id, or group and season asked for).

### Raises {#feb_standings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The FEB has nothing for the request (a game the API does not serve, an unknown group, a schedule page with no games, an empty overview window). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected bearer token, an error or changed envelope, a page for another season or group). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#feb_standings-example}

```python
feb_standings(group=1, season=2027)
```
