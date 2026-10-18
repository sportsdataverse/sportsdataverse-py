---
title: FEB — FEB LiveStats API + baloncestoenvivo.feb.es (Spain)
sidebar_label: FEB LiveStats API + baloncestoenvivo.feb.es (Spain)
description: "FEB — FEB LiveStats API + baloncestoenvivo.feb.es (Spain) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
toc_max_heading_level: 2
---
# FEB — FEB LiveStats API + baloncestoenvivo.feb.es (Spain)

`sportsdataverse.basketball.feb` — 11 endpoints.

> **Note:** Legal notice (sdv-internal-refs/feb/README.md): the FEB's aviso legal asserts intellectual-property rights over the site's content and databases (the EU database right). It has no explicit scraping clause and robots.txt returns 404 on both hosts, but SportsDataverse has not ruled on it: fetching on your own machine is fine; a published data release or a hosted mirror needs a legal sign-off first.

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
| `game_id` | integer | FEB game id (one global sequence): the {gameId} of every LiveStats route and the p= of Partido.aspx. Game bodies do not print it, so it comes from the request; a frame parsed from a bare body has a null game_id. |
| `comp_id` | integer | Competition id (constant across seasons): 115 Primera FEB (LEB Oro), 118 Segunda FEB, 210 Liga Femenina Endesa, 214 LF Challenge, 218 LF-2, 111 Tercera FEB. Not the site's menu group. |
| `competition` | character | Competition name as printed that season, uppercase ('PRIMERA FEB'; 2020-21 'LIGA LEB ORO'). |
| `group_label` | character | The game's group (grupo) label, trimmed: 'Único' = the single group, '"A"' = group A. It is the page's group label, not the site's menu group number. |
| `period` | integer | Current or last period number; 4 for a finished regulation game. |
| `clock` | character | 'FINAL' once the game is over, else the game clock 'mm:ss' remaining in the period. |
| `start_time` | character | Tip-off in Spanish local time (Europe/Madrid) as a naive datetime: the source carries no offset, so the zone is assumed. |
| `venue` | character | Venue (pabellon) name. |
| `address` | character | Venue street address, free text. |
| `referee1` | character | Crew chief (arbitro principal), 'SURNAMES, NAME'. |
| `referee2` | character | First umpire (arbitro auxiliar 1), 'SURNAMES, NAME'. |
| `referee3` | character | Second umpire (arbitro auxiliar 2), 'SURNAMES, NAME'; null when the game had two referees. |
| `status` | integer | Game state code: 3 = finished (status_text FINISHED). Null on 2020-21 games. |
| `status_text` | character | Game state label in English ('FINISHED'). Null on 2020-21 games. |
| `home_team_id` | integer | Home team-season id (key space: FEB team registrations, new every season). |
| `home_team` | character | Home team name for the season (sponsor name, uppercase). |
| `home_team_code` | character | Home federation team code, 8 digits as a string. Null on 2020-21 games. |
| `home_club_code` | character | Home federation club code, 6 digits as a string. Null on 2020-21 games. |
| `home_score` | integer | Home points: the final score when clock is FINAL, else the live score. |
| `away_team_id` | integer | Away team-season id (key space: FEB team registrations, new every season). |
| `away_team` | character | Away team name for the season (sponsor name, uppercase). |
| `away_team_code` | character | Away federation team code, 8 digits as a string. Null on 2020-21 games. |
| `away_club_code` | character | Away federation club code, 6 digits as a string. Null on 2020-21 games. |
| `away_score` | integer | Away points: the final score when clock is FINAL, else the live score. |

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

_Last validated n/a._

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
| `game_id` | integer | FEB game id (one global sequence): the {gameId} of every LiveStats route and the p= of Partido.aspx. Game bodies do not print it, so it comes from the request; a frame parsed from a bare body has a null game_id. |
| `period` | integer | Period number (1-4 regulation; overtime numbering is unverified, no overtime game is in the samples). |
| `home_points` | integer | Points the home team scored IN this period (not cumulative). |
| `away_points` | integer | Points the away team scored IN this period (not cumulative). |

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

_Last validated n/a._

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
| `game_id` | integer | FEB game id (one global sequence): the {gameId} of every LiveStats route and the p= of Partido.aspx. Game bodies do not print it, so it comes from the request; a frame parsed from a bare body has a null game_id. |
| `team_side` | character | Side of the game the row belongs to: home or away (the feed's TEAM[0] / TEAM[1], or its 1/2 and 0/1 side codes, normalised). |
| `team_id` | integer | Team-season id (key space: FEB team registrations; a NEW id every season, e.g. HLA Alicante = 979989 in 2025-26 and 1008687 in 2026-27). Joins the schedule's home_team_id / away_team_id; not a club or player id. |
| `team` | character | Team name as registered for the season (sponsor name, uppercase: 'HLA ALICANTE'). |
| `team_code` | character | Federation team code, 8 digits as a string: the 6-digit club_code plus a 2-digit team suffix ('03058600'). Null on 2020-21 games. |
| `club_code` | character | Federation club code, 6 digits as a string (the first 6 digits of team_code). Whether it is stable across seasons is unverified. Null on 2020-21 games. |
| `seconds_played` | integer | Seconds on court (the feed's min, an integer count of seconds: 1064 = 17:44; a team row sums its players, 12000 = 200:00). Null on /TeamStats rows. |
| `min_formatted` | character | Time played as 'mm:ss'; '0:00' on /TeamStats rows (only box-score totals carry a value). |
| `pts` | integer | Team points: the final score when the game is over, else the live score. |
| `p1m` | integer | Free throws made (tiros libres; integer as a string). |
| `p1a` | integer | Free throws attempted (integer as a string). |
| `p1p` | numeric | Free-throw percentage, 0-100, as a string with a DECIMAL COMMA ('77,3'); '0,0' when no attempts. |
| `p2m` | integer | Two-point field goals made (integer as a string). |
| `p2a` | integer | Two-point field goals attempted (integer as a string). |
| `p2p` | numeric | Two-point percentage, 0-100, string with a decimal comma ('54,1'). |
| `p3m` | integer | Three-point field goals made (integer as a string). |
| `p3a` | integer | Three-point field goals attempted (integer as a string). |
| `p3p` | numeric | Three-point percentage, 0-100, string with a decimal comma ('47,6'). |
| `fgm` | integer | Field goals made, 2PT + 3PT (tiros de campo; integer as a string). |
| `fga` | integer | Field goals attempted, 2PT + 3PT (integer as a string). |
| `fgp` | numeric | Field-goal percentage, 0-100, string with a decimal comma ('51,7'). |
| `ro` | integer | Offensive rebounds (rebotes ofensivos; integer as a string). |
| `rd` | integer | Defensive rebounds (rebotes defensivos; integer as a string). |
| `rt` | integer | Total rebounds (rebotes totales; integer as a string). |
| `assist` | integer | Assists (asistencias; integer as a string). |
| `st` | integer | Steals (balones robados, page column BR; integer as a string). |
| `to` | integer | Turnovers (balones perdidos, page column BP; integer as a string). |
| `bs` | integer | Blocks made (tapones a favor, page column TF; integer as a string). |
| `tc` | integer | Shots of this team that were blocked (tapones en contra, page column TC). Null on /TeamStats rows. |
| `mt` | integer | Dunks (mates, page column MT). Null on /TeamStats rows. |
| `pf` | integer | Personal fouls committed (faltas cometidas, page column FC; integer as a string). |
| `rf` | integer | Fouls received / drawn (faltas recibidas, page column FR; integer as a string). |
| `fouls` | integer | Team fouls committed in the game (equals pf on the samples). |
| `val` | integer | FEB efficiency rating (valoracion). Null on /TeamStats rows, filled on box-score totals. |
| `fastb` | integer | Fast-break points. Null on every sampled game: the federation's stat crews do not fill it. |
| `ptsto` | integer | Points off turnovers. Null on every sampled game: the federation's stat crews do not fill it. |
| `secpts` | integer | Second-chance points. Null on every sampled game: the federation's stat crews do not fill it. |
| `bench` | integer | Bench points. Null on every sampled game: the federation's stat crews do not fill it. |
| `paint` | integer | Points in the paint. Null on every sampled game: the federation's stat crews do not fill it. |

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

_Last validated n/a._

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
| `game_id` | integer | FEB game id (one global sequence): the {gameId} of every LiveStats route and the p= of Partido.aspx. Game bodies do not print it, so it comes from the request; a frame parsed from a bare body has a null game_id. |
| `team_side` | character | Side of the game the row belongs to: home or away (the feed's TEAM[0] / TEAM[1], or its 1/2 and 0/1 side codes, normalised). |
| `team_id` | integer | Team-season id (key space: FEB team registrations; a NEW id every season, e.g. HLA Alicante = 979989 in 2025-26 and 1008687 in 2026-27). Joins the schedule's home_team_id / away_team_id; not a club or player id. |
| `team` | character | Team name as registered for the season (sponsor name, uppercase: 'HLA ALICANTE'). |
| `player_id` | integer | FEB person id: stable across seasons and clubs (A. Jorda Rodriguez = 1676991 in 2025-26 and 2026-27). Joins the box score, play-by-play, shots and leaders; not the per-season licence_id. |
| `shirt_number` | character | Shirt number (dorsal) as a string, so '00' and '0' stay distinct. The shot feed names the shooter by this number. |
| `player` | character | Player name as 'I. SURNAME(S)', uppercase ('V. AGUILAR HERNANDEZ'). |
| `starter` | logical | True when the player was in the starting five (the page's I column), false when on the bench. |
| `captain` | logical | True for the team captain. Null on 2020-21 games, where the feed does not send it. |
| `seconds_played` | integer | Seconds on court (the feed's min, an integer count of seconds: 1064 = 17:44; a team row sums its players, 12000 = 200:00). Null on /TeamStats rows. |
| `min_formatted` | character | Time played as 'mm:ss'. |
| `pts` | integer | Points scored (the team's points on team rows; the final score when the clock is FINAL, else live). |
| `p1m` | integer | Free throws made (tiros libres; integer as a string). |
| `p1a` | integer | Free throws attempted (integer as a string). |
| `p1p` | numeric | Free-throw percentage, 0-100, as a string with a DECIMAL COMMA ('77,3'); '0,0' when no attempts. |
| `p2m` | integer | Two-point field goals made (integer as a string). |
| `p2a` | integer | Two-point field goals attempted (integer as a string). |
| `p2p` | numeric | Two-point percentage, 0-100, string with a decimal comma ('54,1'). |
| `p3m` | integer | Three-point field goals made (integer as a string). |
| `p3a` | integer | Three-point field goals attempted (integer as a string). |
| `p3p` | numeric | Three-point percentage, 0-100, string with a decimal comma ('47,6'). |
| `fgm` | integer | Field goals made, 2PT + 3PT (tiros de campo; integer as a string). |
| `fga` | integer | Field goals attempted, 2PT + 3PT (integer as a string). |
| `fgp` | numeric | Field-goal percentage, 0-100, string with a decimal comma ('51,7'). |
| `ro` | integer | Offensive rebounds (rebotes ofensivos; integer as a string). |
| `rd` | integer | Defensive rebounds (rebotes defensivos; integer as a string). |
| `rt` | integer | Total rebounds (rebotes totales; integer as a string). |
| `reb` | integer | Total rebounds (the same count as rt). |
| `assist` | integer | Assists (asistencias; integer as a string). |
| `st` | integer | Steals (balones robados, page column BR; integer as a string). |
| `to` | integer | Turnovers (balones perdidos, page column BP; integer as a string). |
| `bs` | integer | Blocks made (tapones a favor, page column TF; integer as a string). |
| `tc` | integer | Shots of this player/team that were blocked (tapones en contra / recibidos, page column TC; integer as a string). |
| `mt` | integer | Dunks (mates, page column MT; integer as a string). |
| `pf` | integer | Personal fouls committed (faltas cometidas, page column FC; integer as a string). |
| `rf` | integer | Fouls received / drawn (faltas recibidas, page column FR; integer as a string). |
| `pllss` | integer | Plus/minus while the player was on court (balance, page column +/-), signed integer. |
| `val` | integer | FEB efficiency rating (valoracion): points + rebounds + assists + steals + blocks + fouls drawn - missed shots - turnovers - shots blocked - fouls committed. |

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

_Last validated n/a._

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
| `game_id` | integer | FEB game id (one global sequence): the {gameId} of every LiveStats route and the p= of Partido.aspx. Game bodies do not print it, so it comes from the request; a frame parsed from a bare body has a null game_id. |
| `play_num` | integer | Play sequence number, 1 = the first play of the game; unique per game. The parser sorts by it (the feed lists newest first). |
| `period` | integer | Period number of the play. |
| `clock` | character | Game clock REMAINING in the period as 'mm:ss' (it counts down from 10:00). |
| `clock_seconds` | integer | The clock as whole seconds remaining in the period (08:34 = 514), derived by the parser. |
| `team_side` | character | Side of the acting team: home (feed code 1) or away (2); null on period lines. |
| `team_id` | integer | Team-season id of the acting team (new every season); null on period lines and 2020-21 games. |
| `player_id` | integer | FEB person id of the actor (stable across seasons); null on team, period and 2020-21 lines. |
| `license_id` | integer | Per-season player licence id of the actor (changes when the player re-registers; A. Jorda Rodriguez = 10901171 in 2025-26, 11266668 in 2026-27). Null on team and period lines and 2020-21 games. |
| `action` | character | Action code: shoot, fthrow, rebound, assist, recovery (steal), lose (turnover), blockshot, foul, subst, timeout, period. Null on 2020-21 games. |
| `text` | character | Play description in Spanish: '(TEAM) I. SURNAME: ACTION (running count)'; 2020-21 games use older wording and are text only. |
| `score_home` | integer | Home score AFTER the play; filled on scoring plays only. |
| `score_away` | integer | Away score AFTER the play; filled on scoring plays only. |
| `x` | numeric | Court x position of a shoot line: percent (0-100) of the length of a horizontal full-court image, origin top-left (the same scale as feb_shots). Null when the action has none and on 2020-21 games. |
| `y` | numeric | Court y position of a shoot line: percent (0-100) of the width from the top edge (the same scale as feb_shots). Null when the action has none and on 2020-21 games. |
| `log_param1` | character | Action parameter 1 (string). Team and player actions: a pseudo-GUID '00000000-0000-000{side}-0000-{game id, 12 digits}' (side 1 home, 2 away); period lines: the period number. |
| `log_param2` | character | Action parameter 2 (string). The actor's licence id on shoot, fthrow, foul, rebound, assist, recovery and blockshot lines; period lines: '0' start, '1' end; lose: '0' or '1' (meaning unverified). |
| `log_param3` | character | Action parameter 3 (string). fthrow: '1' made, '0' missed; subst: the outgoing player's licence; foul: a foul-type code. Others unverified. |
| `log_param4` | character | Action parameter 4 (string). shoot and fthrow: '1' made, '0' missed; foul: the licence of the fouled player; lose: a licence (role unverified). |
| `log_param5` | character | Action parameter 5 (string); action-dependent, meaning unverified. |
| `log_param6` | character | Action parameter 6 (string). shoot: shot value '2' or '3'; fthrow: '1'; foul: free throws awarded. |
| `log_param7` | character | Action parameter 7 (string); action-dependent, meaning unverified. |
| `log_param8` | character | Action parameter 8 (string): a small integer on shoot, foul and lose lines (by distribution a zone or sub-type code); unverified. |
| `log_param9` | character | Action parameter 9 (string); blank or null in every sampled game. |
| `log_param10` | character | Action parameter 10 (string): '1' on almost every foul; meaning unverified. |
| `log_time` | character | Wall-clock time the play was logged, Spanish local time, naive datetime. Null on 2020-21 games. |
| `log_time_utc` | character | The log time in UTC, naive datetime. Null on 2020-21 games. |

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

_Last validated n/a._

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
| `game_id` | integer | FEB game id (one global sequence): the {gameId} of every LiveStats route and the p= of Partido.aspx. Game bodies do not print it, so it comes from the request; a frame parsed from a bare body has a null game_id. |
| `period` | integer | Period number of the attempt. |
| `clock` | character | Game clock REMAINING in the period at the shot, 'mm:ss' (the play-by-play line's time). |
| `clock_seconds` | integer | The clock as whole seconds remaining in the period (08:34 = 514), derived by the parser. |
| `team_side` | character | Side of the shooter: home (feed code 0) or away (feed code 1; play-by-play uses 1/2). |
| `team_id` | integer | Team-season id of the shooter's team (from the shot chart's TEAM list); new every season. |
| `shirt_number` | character | Shirt number (dorsal) as a string, so '00' and '0' stay distinct. The shot feed names the shooter by this number. |
| `player_id` | integer | FEB person id of the shooter, resolved from side + shirt number in the game's roster; null when that pair is not unique. The feed itself carries no player id. |
| `made` | logical | True when the shot was made, false when missed. Free throws are not in the feed. |
| `x` | numeric | Court x position: percent (0-100) of the length of a horizontal full-court image, origin top-left. Teams change ends at half-time; metres (x/100 x 28) are an unverified inference. |
| `y` | numeric | Court y position: percent (0-100) of the width of a horizontal full-court image from the top edge; baskets sit near y = 50. Metres (y/100 x 15) are an unverified inference. |

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

_Last validated n/a._

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
| `game_id` | integer | FEB game id (one global sequence): the {gameId} of every LiveStats route and the p= of Partido.aspx. Game bodies do not print it, so it comes from the request; a frame parsed from a bare body has a null game_id. |
| `team_side` | character | Side of the game the row belongs to: home or away (the feed's TEAM[0] / TEAM[1], or its 1/2 and 0/1 side codes, normalised). |
| `team_id` | integer | Team-season id (key space: FEB team registrations; a NEW id every season, e.g. HLA Alicante = 979989 in 2025-26 and 1008687 in 2026-27). Joins the schedule's home_team_id / away_team_id; not a club or player id. |
| `team` | character | Team name as registered for the season (sponsor name, uppercase: 'HLA ALICANTE'). |
| `category` | character | Leader category: points, rebounds, assists or fouls (the feed's Puntos, Rebotes, Asistencias, Faltas). |
| `rank` | integer | Place in the team's category list, 1 = the leader, in the order the feed lists them. |
| `player_id` | integer | FEB person id: stable across seasons and clubs (A. Jorda Rodriguez = 1676991 in 2025-26 and 2026-27). Joins the box score, play-by-play, shots and leaders; not the per-season licence_id. |
| `shirt_number` | character | Shirt number (dorsal) as a string, so '00' and '0' stay distinct. The shot feed names the shooter by this number. |
| `player` | character | Player name as 'I. SURNAME(S)', uppercase ('V. AGUILAR HERNANDEZ'). |
| `starter` | logical | True when the player was in the starting five (the page's I column), false when on the bench. |
| `captain` | logical | True for the team captain. Null on 2020-21 games, where the feed does not send it. |
| `value` | integer | The player's count in the row's category (points, rebounds, assists or personal fouls). |

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

_Last validated n/a._

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
| `comp_id` | integer | Competition id (constant across seasons): 115 Primera FEB (LEB Oro), 118 Segunda FEB, 210 Liga Femenina Endesa, 214 LF Challenge, 218 LF-2, 111 Tercera FEB. Not the site's menu group. |
| `comp_code` | integer | Season-specific competition edition code (e.g. 23473 = Primera FEB 2026-27). |
| `competition` | character | Competition name as printed that season, uppercase ('PRIMERA FEB'; 2020-21 'LIGA LEB ORO'). |
| `game_id` | integer | FEB game id (one global sequence): the {gameId} of every LiveStats route. |
| `round_id` | integer | Round id; 0 in every sample (the route does not fill it). |
| `stage` | character | Stage code as a string; '0' in every sample (meaning unverified). |
| `venue` | character | Name of the venue (pabellon) where the game is played. |
| `start_time` | character | Tip-off in Spanish local time as a naive datetime (the source has no offset). |
| `start_time_utc` | character | Tip-off in UTC as a naive datetime. |
| `period` | integer | Current or last period (4 when finished). |
| `clock` | character | 'FINAL' once finished, else the game clock ('00:00' before tip-off). |
| `status` | integer | Game state code: 3 = finished, 1 = scheduled (seen 2026-10-12); the live codes are unverified. |
| `home_team_id` | integer | Home team-season id (new every season; joins the schedule pages). |
| `home_team` | character | Home team name for the season. |
| `home_team_code` | character | Home federation team code, 8 digits as a string. |
| `home_club_code` | character | Home federation club code, 6 digits as a string. |
| `home_abbreviation` | character | Home team three-letter abbreviation ('PAL'). |
| `home_score` | integer | Home points (0 before tip-off). |
| `away_team_id` | integer | Away team-season id (new every season; joins the schedule pages). |
| `away_team` | character | Away team name for the season. |
| `away_team_code` | character | Away federation team code, 8 digits as a string. |
| `away_club_code` | character | Away federation club code, 6 digits as a string. |
| `away_abbreviation` | character | Away team three-letter abbreviation. |
| `away_score` | integer | Away points (0 before tip-off). |
| `period_scores` | character | Points per period as 'home-away' pairs joined by ';' ('16-16;19-16;10-21;14-14'); null before tip-off. |

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

_Last validated n/a._

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
| `group` | integer | The site's menu group (1 Primera FEB, 2 Segunda FEB, 4 Liga Femenina Endesa); not a competition id. |
| `season` | integer | Season END year (2027 = 2026-27), the site's START-year key plus one. |
| `competition` | character | Competition name on the page header ('PRIMERA FEB'). |
| `round` | integer | Round number (jornada) the game belongs to. |
| `round_label` | character | Round heading as printed ('Jornada 4 16/10/2026'). |
| `round_date` | character | Date of the round (the heading's date); played games carry no date of their own on this page. |
| `game_id` | integer | FEB game id (one global sequence): the {gameId} of every LiveStats route and the p= of Partido.aspx. Game bodies do not print it, so it comes from the request; a frame parsed from a bare body has a null game_id. |
| `home_team_id` | integer | Home team-season id (new every season; joins feb_overview and the game wrappers). |
| `home_team` | character | Home team name, uppercase. |
| `away_team_id` | integer | Away team-season id (new every season). |
| `away_team` | character | Away team name, uppercase. |
| `result_text` | character | Result cell verbatim: 'home-away' points when played; before the game the calendar shows the Spanish weekday, date and time and the results page shows '*-*'. |
| `home_score` | integer | Home points; null before the game. |
| `away_score` | integer | Away points; null before the game. |
| `status` | character | played when the result cell holds a score, scheduled otherwise. |
| `weekday` | character | English weekday of a scheduled game (translated from the Spanish cell); null for played games. |
| `date` | character | Game date; null for played games on the schedule page (use round_date). |
| `time` | character | Tip-off 'HH:MM', Spanish local time; null for played games on the schedule page. |

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

_Last validated n/a._

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
| `group` | integer | The site's menu group (1 Primera FEB, 2 Segunda FEB, 4 Liga Femenina Endesa); not a competition id. |
| `season` | integer | Season END year (2027 = 2026-27), the site's START-year key plus one. |
| `competition` | character | Competition name on the page header ('PRIMERA FEB'). |
| `comp_id` | integer | Competition id (CompID) read from the page; ties the site group to the API (group 1 -> 115, 2 -> 118, 4 -> 210). |
| `section` | character | Which table the row comes from: results (the latest round) or next_round (the following fixtures). |
| `round_title` | character | Title of the table as printed ('Resultados Jornada 3(09/10/2026)'). |
| `game_id` | integer | FEB game id (one global sequence): the {gameId} of every LiveStats route and the p= of Partido.aspx. Game bodies do not print it, so it comes from the request; a frame parsed from a bare body has a null game_id. |
| `home_team_id` | integer | Home team-season id (new every season; joins feb_overview and the game wrappers). |
| `home_team` | character | Home team name, uppercase. |
| `away_team_id` | integer | Away team-season id (new every season). |
| `away_team` | character | Away team name, uppercase. |
| `result_text` | character | Result cell verbatim: 'home-away' points when played; before the game the calendar shows the Spanish weekday, date and time and the results page shows '*-*'. |
| `home_score` | integer | Home points; null before the game. |
| `away_score` | integer | Away points; null before the game. |
| `status` | character | played when the result cell holds a score, scheduled otherwise. |
| `date` | character | Game date; null for played games on the schedule page (use round_date). |
| `time` | character | Tip-off 'HH:MM', Spanish local time; null for played games on the schedule page. |

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

_Last validated n/a._

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
| `group` | integer | The site's menu group (1 Primera FEB, 2 Segunda FEB, 4 Liga Femenina Endesa); not a competition id. |
| `season` | integer | Season END year (2027 = 2026-27), the site's START-year key plus one. |
| `competition` | character | Competition name on the page header ('PRIMERA FEB'). |
| `comp_id` | integer | Competition id (CompID) read from the page; ties the site group to the API (group 1 -> 115, 2 -> 118, 4 -> 210). |
| `table` | character | live = the in-season league table; final = the final classification once the season is over. |
| `position` | integer | Rank in the table (Po). |
| `team_id` | integer | Team-season id (new every season; joins the schedule and game wrappers). |
| `team` | character | Team name as printed in the table (sponsor name, uppercase). |
| `played` | integer | Games played (PJ); null in the final classification. |
| `won` | integer | Games won (PG); null in the final classification. |
| `lost` | integer | Games lost (PP); null in the final classification. |
| `points_for` | integer | Points scored (PF); null in the final classification. |
| `points_against` | integer | Points conceded (PC); null in the final classification. |
| `point_diff` | integer | Point difference (DIF), signed; null in the final classification. |
| `win_pct` | numeric | Win percentage, 0-100 (the page's '100,0%', decimal comma converted). |
| `streak` | integer | Current streak as a signed integer: +3 = three wins in a row, -2 = two losses. |
| `streak_text` | character | The streak cell verbatim ('+3'). |

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

_Last validated n/a._
