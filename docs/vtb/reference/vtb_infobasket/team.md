# VTB — VTB United League Infobasket (Russia) — Team: stats–season_stats

> VTB — VTB United League Infobasket (Russia) — Team: stats–season_stats — function reference in sdv-py, the SportsDataverse Python package.

## vtb_infobasket_team_stats

Season totals of every team of a season (all stages).

**Endpoint URL:** `GET https://org.infobasket.su/Comp/GetTeamStats/{season}`

**Valid URL:** [https://org.infobasket.su/Comp/GetTeamStats/2026?page=0&tag=vtb](https://org.infobasket.su/Comp/GetTeamStats/2026?page=0&tag=vtb)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26); the wrapper maps it to the season-level Infobasket CompID with vtb_infobasket_seasons (one extra request per tag, cached). |
| `tag` | `tag` |  |  | `Y` | Competition tag scoping the profile: vtb (VTB United League, default), vtbyouth, wbc, wpremier or rfb. The season table is read for this tag. |

### Returns {#vtb_infobasket_team_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season END year (2026 = 2025-26): the season the request asked for, stamped by the runtime. |
| `team_id` | Int64 | Infobasket team id (TeamID): one club's entry in a competition family (Lokomotiv Kuban = 2650). Joins to /Widget/Team*/{teamId} and equals the v2 API `teamId`. |
| `game_count` | Int64 | Games in the totals (the whole competition, all stages, for the season-level CompID). |
| `points` | Int64 | Points. The wire's null is 0 here (null counts are filled). |
| `shot1` | Int64 | Free throws attempted. The wire's null is 0 here (null counts are filled). |
| `goal1` | Int64 | Free throws made. The wire's null is 0 here (null counts are filled). |
| `shot2` | Int64 | 2-point field goals attempted. The wire's null is 0 here (null counts are filled). |
| `goal2` | Int64 | 2-point field goals made. The wire's null is 0 here (null counts are filled). |
| `shot3` | Int64 | 3-point field goals attempted. The wire's null is 0 here (null counts are filled). |
| `goal3` | Int64 | 3-point field goals made. The wire's null is 0 here (null counts are filled). |
| `shot23` | Int64 | Field goals (2 + 3 points) attempted. The wire's null is 0 here (null counts are filled). |
| `goal23` | Int64 | Field goals (2 + 3 points) made. The wire's null is 0 here (null counts are filled). |
| `shot1_percent` | Float64 | Free-throw percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `shot2_percent` | Float64 | 2-point percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `shot3_percent` | Float64 | 3-point percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `shot23_percent` | Float64 | Field-goal percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `assist` | Int64 | Assists. The wire's null is 0 here (null counts are filled). |
| `blocks` | Int64 | Blocked shots. The wire's null is 0 here (null counts are filled). |
| `def_rebound` | Int64 | Defensive rebounds. The wire's null is 0 here (null counts are filled). |
| `off_rebound` | Int64 | Offensive rebounds. The wire's null is 0 here (null counts are filled). |
| `rebound` | Int64 | Total rebounds. The wire's null is 0 here (null counts are filled). |
| `steal` | Int64 | Steals. The wire's null is 0 here (null counts are filled). |
| `turnover` | Int64 | Turnovers. The wire's null is 0 here (null counts are filled). |
| `team_def_rebound` | Int64 | Team defensive rebounds (not credited to a player). The wire's null is 0 here (null counts are filled). |
| `team_off_rebound` | Int64 | Team offensive rebounds (not credited to a player). The wire's null is 0 here (null counts are filled). |
| `team_rebound` | Int64 | Team rebounds (not credited to a player). The wire's null is 0 here (null counts are filled). |
| `team_steal` | Int64 | Team steals (not credited to a player). The wire's null is 0 here (null counts are filled). |
| `team_turnover` | Int64 | Team turnovers (not credited to a player, e.g. shot-clock violations). The wire's null is 0 here (null counts are filled). |
| `kpi` | Int64 | Efficiency index (KPI, the platform's 'КПИ'); formula unverified. |
| `avg_points` | Float64 | Per-game average of: points; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_assist` | Float64 | Per-game average of: assists; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_blocks` | Float64 | Per-game average of: blocked shots; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_def_rebound` | Float64 | Per-game average of: defensive rebounds; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_off_rebound` | Float64 | Per-game average of: offensive rebounds; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_rebound` | Float64 | Per-game average of: total rebounds; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_steal` | Float64 | Per-game average of: steals; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_turnover` | Float64 | Per-game average of: turnovers; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_foul` | Float64 | Per-game average of: personal fouls committed; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_opponent_foul` | Float64 | Per-game average of: fouls drawn; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_plus_minus` | Float64 | Per-game average of: plus-minus: point differential while on the court; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_team_def_rebound` | Float64 | Per-game average of: team defensive rebounds (not credited to a player); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_team_off_rebound` | Float64 | Per-game average of: team offensive rebounds (not credited to a player); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_team_rebound` | Float64 | Per-game average of: team rebounds (not credited to a player); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_team_steal` | Float64 | Per-game average of: team steals (not credited to a player); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_team_turnover` | Float64 | Per-game average of: team turnovers (not credited to a player, e.g. shot-clock violations); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `foul` | Int64 | Personal fouls committed. The wire's null is 0 here (null counts are filled). |
| `opponent_foul` | Int64 | Fouls drawn. The wire's null is 0 here (null counts are filled). |
| `seconds` | Int64 | Playing time in seconds. |
| `played_time` | String | Playing time as 'minutes:seconds' (team rows sum the five players: '200:00' = 40 minutes x 5). |
| `avg_played_time` | String | Average playing time per game as 'minutes:seconds'. |
| `plus_minus` | Int64 | Plus-minus: point differential while on the court. Null where the host sends none. |
| `team_short_name_ru` | String | The the team's short name in Russian (as the competition names it). |
| `team_short_name_en` | String | The the team's short name in English (as the competition names it). |
| `team_name_ru` | String | The the team's name in Russian (as the competition names it). |
| `team_name_en` | String | The the team's name in English (as the competition names it). |
| `team_region_name_ru` | String | The the team's region or city in Russian (as the competition names it). |
| `team_region_name_en` | String | The the team's region or city in English (as the competition names it). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#vtb_infobasket_team_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (an empty list, a league the API does not know, a season with no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope or string (HTTP 200), a game that is not final, a partial or possibly truncated page, or a body for another game, person, team, competition, league or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season the competition tag does not have, or an argument that is not an integer id. |

### Example {#vtb_infobasket_team_stats-example}

```python
vtb_infobasket_team_stats(season=2026)
```

## vtb_infobasket_team_stats_players

Season totals of every player of every team of a season.

**Endpoint URL:** `GET https://org.infobasket.su/Comp/GetTeamStats/{season}`

**Valid URL:** [https://org.infobasket.su/Comp/GetTeamStats/2026?page=0&tag=vtb](https://org.infobasket.su/Comp/GetTeamStats/2026?page=0&tag=vtb)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26); the wrapper maps it to the season-level Infobasket CompID with vtb_infobasket_seasons (one extra request per tag, cached). |
| `tag` | `tag` |  |  | `Y` | Competition tag scoping the profile: vtb (VTB United League, default), vtbyouth, wbc, wpremier or rfb. The season table is read for this tag. |

### Returns {#vtb_infobasket_team_stats_players-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season END year (2026 = 2025-26): the season the request asked for, stamped by the runtime. |
| `team_id` | Int64 | Infobasket TeamID (2650 = Lokomotiv Kuban); the v2 teamId. Not a club-record id and not a registration id. |
| `person_id` | Int64 | Infobasket person id (PersonID): one person (player, coach or official) across clubs and seasons. Joins to /Widget/PlayerPage/{personId} (players) and /Widget/CoachPage/{personId} (coaches). |
| `player_number` | Int64 | Shirt number as an integer (0 when the player has none or wears 00). |
| `display_number` | String | Shirt number as displayed ('0', '7'); a coach's is the coach order (1 = head coach). |
| `is_active` | Boolean | True while the person is on the current roster. |
| `active_status` | String | Roster status text ('Accepted'), empty otherwise. |
| `points` | Int64 | Points. The wire's null is 0 here (null counts are filled). |
| `shot1` | Int64 | Free throws attempted. The wire's null is 0 here (null counts are filled). |
| `goal1` | Int64 | Free throws made. The wire's null is 0 here (null counts are filled). |
| `shot2` | Int64 | 2-point field goals attempted. The wire's null is 0 here (null counts are filled). |
| `goal2` | Int64 | 2-point field goals made. The wire's null is 0 here (null counts are filled). |
| `shot3` | Int64 | 3-point field goals attempted. The wire's null is 0 here (null counts are filled). |
| `goal3` | Int64 | 3-point field goals made. The wire's null is 0 here (null counts are filled). |
| `shot23` | Int64 | Field goals (2 + 3 points) attempted. The wire's null is 0 here (null counts are filled). |
| `goal23` | Int64 | Field goals (2 + 3 points) made. The wire's null is 0 here (null counts are filled). |
| `shot1_percent` | Float64 | Free-throw percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `shot2_percent` | Float64 | 2-point percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `shot3_percent` | Float64 | 3-point percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `shot23_percent` | Float64 | Field-goal percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `assist` | Int64 | Assists. The wire's null is 0 here (null counts are filled). |
| `blocks` | Int64 | Blocked shots. The wire's null is 0 here (null counts are filled). |
| `def_rebound` | Int64 | Defensive rebounds. The wire's null is 0 here (null counts are filled). |
| `off_rebound` | Int64 | Offensive rebounds. The wire's null is 0 here (null counts are filled). |
| `rebound` | Int64 | Total rebounds. The wire's null is 0 here (null counts are filled). |
| `steal` | Int64 | Steals. The wire's null is 0 here (null counts are filled). |
| `turnover` | Int64 | Turnovers. The wire's null is 0 here (null counts are filled). |
| `foul` | Int64 | Personal fouls committed. The wire's null is 0 here (null counts are filled). |
| `opponent_foul` | Int64 | Fouls drawn. The wire's null is 0 here (null counts are filled). |
| `plus_minus` | Int64 | Plus-minus: point differential while on the court. Null where the host sends none. |
| `kpi` | Int64 | Efficiency index (KPI, the platform's 'КПИ'); formula unverified. |
| `avg_points` | Float64 | Per-game average of: points; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_assist` | Float64 | Per-game average of: assists; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_blocks` | Float64 | Per-game average of: blocked shots; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_def_rebound` | Float64 | Per-game average of: defensive rebounds; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_off_rebound` | Float64 | Per-game average of: offensive rebounds; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_rebound` | Float64 | Per-game average of: total rebounds; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_steal` | Float64 | Per-game average of: steals; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_turnover` | Float64 | Per-game average of: turnovers; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_foul` | Float64 | Per-game average of: personal fouls committed; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_opponent_foul` | Float64 | Per-game average of: fouls drawn; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_plus_minus` | Float64 | Per-game average of: plus-minus: point differential while on the court; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_kpi` | Float64 | Per-game average of: efficiency index (kpi, the platform's 'кпи'); formula unverified; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `seconds` | Int64 | Playing time in seconds. |
| `played_time` | String | Playing time as 'minutes:seconds' (team rows sum the five players: '200:00' = 40 minutes x 5). |
| `avg_played_time` | String | Average playing time per game as 'minutes:seconds'. |
| `in_game_count` | Int64 | Games actually played (on the court). |
| `game_count` | Int64 | Games in the totals (the whole competition, all stages, for the season-level CompID). |
| `start_count` | Int64 | Games the person started. |
| `person_last_name_ru` | String | Last name, Russian. |
| `person_last_name_en` | String | Last name, English. |
| `person_first_name_ru` | String | First name, Russian. |
| `person_first_name_en` | String | First name, English. |
| `person_full_name_ru` | String | Full name, 'Last First', Russian. |
| `person_full_name_en` | String | Full name, 'Last First', English. |
| `person_gender` | Int64 | Person gender: 1 on every sampled (male) person. |
| `birth_date` | String | Birth date (ISO 8601 date) of a player: public bio, kept; coaches' and officials' birth data is never output. |
| `height` | Int64 | Height in centimetres. |
| `weight` | Int64 | Weight in kilograms. |
| `player_position` | Int64 | Player position code from the host (1 point guard, 2 shooting guard, 3 small forward ... 5 center; see position_id for the named position). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#vtb_infobasket_team_stats_players-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (an empty list, a league the API does not know, a season with no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope or string (HTTP 200), a game that is not final, a partial or possibly truncated page, or a body for another game, person, team, competition, league or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season the competition tag does not have, or an argument that is not an integer id. |

### Example {#vtb_infobasket_team_stats_players-example}

```python
vtb_infobasket_team_stats_players(season=2026)
```

## vtb_infobasket_team_stats_coaches

Technical fouls and games of every coach of every team of a season.

**Endpoint URL:** `GET https://org.infobasket.su/Comp/GetTeamStats/{season}`

**Valid URL:** [https://org.infobasket.su/Comp/GetTeamStats/2026?page=0&tag=vtb](https://org.infobasket.su/Comp/GetTeamStats/2026?page=0&tag=vtb)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26); the wrapper maps it to the season-level Infobasket CompID with vtb_infobasket_seasons (one extra request per tag, cached). |
| `tag` | `tag` |  |  | `Y` | Competition tag scoping the profile: vtb (VTB United League, default), vtbyouth, wbc, wpremier or rfb. The season table is read for this tag. |

### Returns {#vtb_infobasket_team_stats_coaches-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season END year (2026 = 2025-26): the season the request asked for, stamped by the runtime. |
| `team_id` | Int64 | Infobasket TeamID (2650 = Lokomotiv Kuban); the v2 teamId. Not a club-record id and not a registration id. |
| `person_id` | Int64 | Infobasket person id (PersonID): one person (player, coach or official) across clubs and seasons. Joins to /Widget/PlayerPage/{personId} (players) and /Widget/CoachPage/{personId} (coaches). |
| `player_number` | Int64 | Coach order (1 head coach). |
| `display_number` | String | Coach order (1 head coach). |
| `is_active` | Boolean | True while the person is on the current roster. |
| `active_status` | String | Unobserved: null in every sample, so its type and meaning are unconfirmed. |
| `foul_c` | Int64 | Technical fouls on the head coach for his own conduct (scoresheet 'C'). |
| `foul_b` | Int64 | Technical fouls on the head coach for bench conduct (scoresheet 'B'). |
| `foul_d` | Int64 | Unobserved: null in every sample, so its type and meaning are unconfirmed. |
| `foul` | Int64 | Personal fouls committed. The wire's null is 0 here (null counts are filled). |
| `game_count` | Int64 | Games in the totals (the whole competition, all stages, for the season-level CompID). |
| `person_last_name_ru` | String | Last name, Russian. |
| `person_last_name_en` | String | Last name, English. |
| `person_first_name_ru` | String | First name, Russian. |
| `person_first_name_en` | String | First name, English. |
| `person_full_name_ru` | String | Full name, 'Last First', Russian. |
| `person_full_name_en` | String | Full name, 'Last First', English. |
| `person_gender` | Int64 | Person gender: 1 on every sampled (male) person. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#vtb_infobasket_team_stats_coaches-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (an empty list, a league the API does not know, a season with no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope or string (HTTP 200), a game that is not final, a partial or possibly truncated page, or a body for another game, person, team, competition, league or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season the competition tag does not have, or an argument that is not an integer id. |

### Example {#vtb_infobasket_team_stats_coaches-example}

```python
vtb_infobasket_team_stats_coaches(season=2026)
```

## vtb_infobasket_team_preview

Season totals of a game's two teams (the pre-game preview).

**Endpoint URL:** `GET https://org.infobasket.su/Comp/GetTeamStatsForPreview/{game_id}`

**Valid URL:** [https://org.infobasket.su/Comp/GetTeamStatsForPreview/921391?season=2026&tag=vtb](https://org.infobasket.su/Comp/GetTeamStatsForPreview/921391?season=2026&tag=vtb)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Infobasket GameID (game_id in vtb_infobasket_calendar; the v2 matches' match_id), e.g. 921391. Global across seasons. |
| `season` | `season` |  |  | `Y` | Season END year (2026 = 2025-26); the wrapper maps it to the season-level Infobasket CompID with vtb_infobasket_seasons (one extra request per tag, cached). Optional (the host accepts the call without it). |
| `tag` | `tag` |  |  | `Y` | Competition tag scoping the profile: vtb (VTB United League, default), vtbyouth, wbc, wpremier or rfb. The season table is read for this tag. |

### Returns {#vtb_infobasket_team_preview-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | Infobasket GameID of the game (the v2 matches' match_id); global across seasons. |
| `team_id` | Int64 | Infobasket team id (TeamID): one club's entry in a competition family (Lokomotiv Kuban = 2650). Joins to /Widget/Team*/{teamId} and equals the v2 API `teamId`. |
| `game_count` | Int64 | Games in the totals (the whole competition, all stages, for the season-level CompID). |
| `points` | Int64 | Points. The wire's null is 0 here (null counts are filled). |
| `shot1` | Int64 | Free throws attempted. The wire's null is 0 here (null counts are filled). |
| `goal1` | Int64 | Free throws made. The wire's null is 0 here (null counts are filled). |
| `shot2` | Int64 | 2-point field goals attempted. The wire's null is 0 here (null counts are filled). |
| `goal2` | Int64 | 2-point field goals made. The wire's null is 0 here (null counts are filled). |
| `shot3` | Int64 | 3-point field goals attempted. The wire's null is 0 here (null counts are filled). |
| `goal3` | Int64 | 3-point field goals made. The wire's null is 0 here (null counts are filled). |
| `shot23` | Int64 | Field goals (2 + 3 points) attempted. The wire's null is 0 here (null counts are filled). |
| `goal23` | Int64 | Field goals (2 + 3 points) made. The wire's null is 0 here (null counts are filled). |
| `shot1_percent` | Float64 | Free-throw percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `shot2_percent` | Float64 | 2-point percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `shot3_percent` | Float64 | 3-point percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `shot23_percent` | Float64 | Field-goal percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `assist` | Int64 | Assists. The wire's null is 0 here (null counts are filled). |
| `blocks` | Int64 | Blocked shots. The wire's null is 0 here (null counts are filled). |
| `def_rebound` | Int64 | Defensive rebounds. The wire's null is 0 here (null counts are filled). |
| `off_rebound` | Int64 | Offensive rebounds. The wire's null is 0 here (null counts are filled). |
| `rebound` | Int64 | Total rebounds. The wire's null is 0 here (null counts are filled). |
| `steal` | Int64 | Steals. The wire's null is 0 here (null counts are filled). |
| `turnover` | Int64 | Turnovers. The wire's null is 0 here (null counts are filled). |
| `team_def_rebound` | Int64 | Team defensive rebounds (not credited to a player). The wire's null is 0 here (null counts are filled). |
| `team_off_rebound` | Int64 | Team offensive rebounds (not credited to a player). The wire's null is 0 here (null counts are filled). |
| `team_rebound` | Int64 | Team rebounds (not credited to a player). The wire's null is 0 here (null counts are filled). |
| `team_steal` | Int64 | Team steals (not credited to a player). The wire's null is 0 here (null counts are filled). |
| `team_turnover` | Int64 | Team turnovers (not credited to a player, e.g. shot-clock violations). The wire's null is 0 here (null counts are filled). |
| `kpi` | Int64 | Efficiency index (KPI, the platform's 'КПИ'); formula unverified. |
| `avg_points` | Float64 | Per-game average of: points; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_assist` | Float64 | Per-game average of: assists; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_blocks` | Float64 | Per-game average of: blocked shots; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_def_rebound` | Float64 | Per-game average of: defensive rebounds; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_off_rebound` | Float64 | Per-game average of: offensive rebounds; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_rebound` | Float64 | Per-game average of: total rebounds; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_steal` | Float64 | Per-game average of: steals; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_turnover` | Float64 | Per-game average of: turnovers; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_foul` | Float64 | Per-game average of: personal fouls committed; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_opponent_foul` | Float64 | Per-game average of: fouls drawn; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_plus_minus` | Float64 | Per-game average of: plus-minus: point differential while on the court; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_team_def_rebound` | Float64 | Per-game average of: team defensive rebounds (not credited to a player); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_team_off_rebound` | Float64 | Per-game average of: team offensive rebounds (not credited to a player); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_team_rebound` | Float64 | Per-game average of: team rebounds (not credited to a player); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_team_steal` | Float64 | Per-game average of: team steals (not credited to a player); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_team_turnover` | Float64 | Per-game average of: team turnovers (not credited to a player, e.g. shot-clock violations); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `foul` | Int64 | Personal fouls committed. The wire's null is 0 here (null counts are filled). |
| `opponent_foul` | Int64 | Fouls drawn. The wire's null is 0 here (null counts are filled). |
| `seconds` | Int64 | Playing time in seconds. |
| `played_time` | String | Playing time as 'minutes:seconds' (team rows sum the five players: '200:00' = 40 minutes x 5). |
| `avg_played_time` | String | Average playing time per game as 'minutes:seconds'. |
| `plus_minus` | Int64 | Plus-minus: point differential while on the court. Null where the host sends none. |
| `team_short_name_ru` | String | The the team's short name in Russian (as the competition names it). |
| `team_short_name_en` | String | The the team's short name in English (as the competition names it). |
| `team_name_ru` | String | The the team's name in Russian (as the competition names it). |
| `team_name_en` | String | The the team's name in English (as the competition names it). |
| `team_region_name_ru` | String | The the team's region or city in Russian (as the competition names it). |
| `team_region_name_en` | String | The the team's region or city in English (as the competition names it). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#vtb_infobasket_team_preview-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (an empty list, a league the API does not know, a season with no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope or string (HTTP 200), a game that is not final, a partial or possibly truncated page, or a body for another game, person, team, competition, league or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season the competition tag does not have, or an argument that is not an integer id. |

### Example {#vtb_infobasket_team_preview-example}

```python
vtb_infobasket_team_preview(game_id=921391, season=2026)
```

## vtb_infobasket_team_box_score

Team box score of a finished game, with the head coach's technical fouls.

**Endpoint URL:** `GET https://org.infobasket.su/Widget/GetOnline/{game_id}`

**Valid URL:** [https://org.infobasket.su/Widget/GetOnline/921391?format=json](https://org.infobasket.su/Widget/GetOnline/921391?format=json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Infobasket GameID (game_id in vtb_infobasket_calendar; the v2 matches' match_id), e.g. 921391. Global across seasons. |

### Returns {#vtb_infobasket_team_box_score-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | Infobasket GameID of the game (the v2 matches' match_id); global across seasons. |
| `is_home` | Boolean | True for team 1, the team listed first (team A, the home side in the calendar); the body has no explicit home flag (unverified). |
| `team_number` | Int64 | Side: 1 team A (home), 2 team B (away). |
| `team_id` | Int64 | Infobasket team id (TeamID): one club's entry in a competition family (Lokomotiv Kuban = 2650). Joins to /Widget/Team*/{teamId} and equals the v2 API `teamId`. The scope check compares both sides with the request. |
| `score` | Int64 | Final points of the team. |
| `points` | Int64 | Points. The wire's null is 0 here (null counts are filled). |
| `shot1` | Int64 | Free throws attempted. The wire's null is 0 here (null counts are filled). |
| `goal1` | Int64 | Free throws made. The wire's null is 0 here (null counts are filled). |
| `shot2` | Int64 | 2-point field goals attempted. The wire's null is 0 here (null counts are filled). |
| `goal2` | Int64 | 2-point field goals made. The wire's null is 0 here (null counts are filled). |
| `shot3` | Int64 | 3-point field goals attempted. The wire's null is 0 here (null counts are filled). |
| `goal3` | Int64 | 3-point field goals made. The wire's null is 0 here (null counts are filled). |
| `shot1_percent` | Float64 | Free-throw percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `shot2_percent` | Float64 | 2-point percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `shot3_percent` | Float64 | 3-point percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `assist` | Int64 | Assists. The wire's null is 0 here (null counts are filled). |
| `blocks` | Int64 | Blocked shots. The wire's null is 0 here (null counts are filled). |
| `def_rebound` | Int64 | Defensive rebounds. The wire's null is 0 here (null counts are filled). |
| `off_rebound` | Int64 | Offensive rebounds. The wire's null is 0 here (null counts are filled). |
| `rebound` | Int64 | Total rebounds. The wire's null is 0 here (null counts are filled). |
| `steal` | Int64 | Steals. The wire's null is 0 here (null counts are filled). |
| `turnover` | Int64 | Turnovers. The wire's null is 0 here (null counts are filled). |
| `team_def_rebound` | Int64 | Team defensive rebounds (not credited to a player). The wire's null is 0 here (null counts are filled). |
| `team_off_rebound` | Int64 | Team offensive rebounds (not credited to a player). The wire's null is 0 here (null counts are filled). |
| `team_rebound` | Int64 | Team rebounds (not credited to a player). The wire's null is 0 here (null counts are filled). |
| `team_steal` | Int64 | Unobserved: null in every sample, so its type and meaning are unconfirmed. |
| `team_turnover` | Int64 | Team turnovers (not credited to a player, e.g. shot-clock violations). The wire's null is 0 here (null counts are filled). |
| `foul` | Int64 | Personal fouls committed by the team. |
| `opponent_foul` | Int64 | Fouls drawn. The wire's null is 0 here (null counts are filled). |
| `seconds` | Int64 | Playing time in seconds. |
| `played_time` | String | Playing time as 'minutes:seconds' (team rows sum the five players: '200:00' = 40 minutes x 5). |
| `plus_minus` | Int64 | Unobserved: null in every sample, so its type and meaning are unconfirmed. |
| `team_short_name_ru` | String | The the team's short name in Russian (as the competition names it). |
| `team_short_name_en` | String | The the team's short name in English (as the competition names it). |
| `team_name_ru` | String | The the team's name in Russian (as the competition names it). |
| `team_name_en` | String | The the team's name in English (as the competition names it). |
| `team_region_name_ru` | String | The the team's region or city in Russian (as the competition names it). |
| `team_region_name_en` | String | The the team's region or city in English (as the competition names it). |
| `coach_person_id` | Int64 | Infobasket person id (PersonID): one person (player, coach or official) across clubs and seasons. Joins to /Widget/PlayerPage/{personId} (players) and /Widget/CoachPage/{personId} (coaches). |
| `coach_name_en` | String | Head coach's name in English, as 'Last First'. |
| `coach_name_ru` | String | Head coach's name in Russian, as 'Last First'. |
| `coach_foul_c` | Int64 | Technical fouls on the head coach for his own conduct (scoresheet 'C'). |
| `coach_foul_b` | Int64 | Technical fouls on the head coach for bench conduct (scoresheet 'B'). |
| `coach_foul_d` | Int64 | Disqualifying fouls on the coach (scoresheet 'D'). |
| `coach_foul` | Int64 | Fouls charged to the coach (C + B + D). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#vtb_infobasket_team_box_score-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (an empty list, a league the API does not know, a season with no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope or string (HTTP 200), a game that is not final, a partial or possibly truncated page, or a body for another game, person, team, competition, league or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season the competition tag does not have, or an argument that is not an integer id. |

### Example {#vtb_infobasket_team_box_score-example}

```python
vtb_infobasket_team_box_score(game_id=921391)
```

## vtb_infobasket_team

Club profile: names, region, country, status.

**Endpoint URL:** `GET https://org.infobasket.su/Widget/TeamPage/{team_id}`

**Valid URL:** [https://org.infobasket.su/Widget/TeamPage/2650?format=json&lang=en&tag=vtb](https://org.infobasket.su/Widget/TeamPage/2650?format=json&lang=en&tag=vtb)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Infobasket TeamID (team_id in the calendar, a box score or the v2 matches), e.g. 2650 (Lokomotiv Kuban). |
| `lang` | `lang` |  |  | `Y` | Label language (en or ru). Most bodies carry both languages whatever is asked; the leader categories' labels follow it. |
| `tag` | `tag` |  |  | `Y` | Competition tag scoping the profile: vtb (VTB United League, default), vtbyouth, wbc, wpremier or rfb. The season table is read for this tag. |

### Returns {#vtb_infobasket_team-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | Infobasket team id (TeamID): one club's entry in a competition family (Lokomotiv Kuban = 2650). Joins to /Widget/Team*/{teamId} and equals the v2 API `teamId`. The scope check compares it with the request. |
| `team_type` | Int64 | Team type code: 1 on the club record. |
| `team_name_ru` | String | Club name, Russian. |
| `team_name_en` | String | Club name, English. |
| `team_short_name_ru` | String | Club short name, Russian. |
| `team_short_name_en` | String | Club short name, English. |
| `team_abc_name_ru` | String | Club abbreviation, Russian (the full name in the sample). |
| `team_abc_name_en` | String | Club abbreviation, English. |
| `team_gender` | Int64 | Team gender code: 0 on every sample (men). |
| `team_region` | Int64 | Infobasket region id of the club's city. |
| `team_country` | Int64 | Country id of the club (1 = Russia, unverified). |
| `team_logo` | Int64 | Logo flag; 0 in the samples. |
| `team_uniform1` | Int64 | First uniform colour code. |
| `team_uniform2` | Int64 | Second uniform colour code. |
| `team_status` | Int64 | Club record status (1 active). |
| `team_record_id` | Int64 | Id of the club-record name (CurrentTeamName.TeamID, 2259 for team 2650): a different key space from team_id. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#vtb_infobasket_team-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (an empty list, a league the API does not know, a season with no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope or string (HTTP 200), a game that is not final, a partial or possibly truncated page, or a body for another game, person, team, competition, league or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season the competition tag does not have, or an argument that is not an integer id. |

### Example {#vtb_infobasket_team-example}

```python
vtb_infobasket_team(team_id=2650)
```

## vtb_infobasket_team_seasons

The seasons a club has data for (END year and the club's competition id).

**Endpoint URL:** `GET https://org.infobasket.su/Widget/GetTeamSeasons/{team_id}`

**Valid URL:** [https://org.infobasket.su/Widget/GetTeamSeasons/2650?format=json&lang=en&tag=vtb](https://org.infobasket.su/Widget/GetTeamSeasons/2650?format=json&lang=en&tag=vtb)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Infobasket TeamID (team_id in the calendar, a box score or the v2 matches), e.g. 2650 (Lokomotiv Kuban). |
| `lang` | `lang` |  |  | `Y` | Label language (en or ru). Most bodies carry both languages whatever is asked; the leader categories' labels follow it. |
| `tag` | `tag` |  |  | `Y` | Competition tag scoping the profile: vtb (VTB United League, default), vtbyouth, wbc, wpremier or rfb. The season table is read for this tag. |

### Returns {#vtb_infobasket_team_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | Infobasket TeamID (2650 = Lokomotiv Kuban); the v2 teamId. Not a club-record id and not a registration id. |
| `season` | Int64 | Season END year of the club's season entry (2027 = 2026/2027). |
| `comp_id` | Int64 | Competition id of the season in the club's rfb tag (56293 for 2027), not the VTB tag's season CompID. |
| `season_name` | String | Season name in Russian ('Сезон 2026/2027' = 'Season 2026/2027'). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#vtb_infobasket_team_seasons-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (an empty list, a league the API does not know, a season with no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope or string (HTTP 200), a game that is not final, a partial or possibly truncated page, or a body for another game, person, team, competition, league or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season the competition tag does not have, or an argument that is not an integer id. |

### Example {#vtb_infobasket_team_seasons-example}

```python
vtb_infobasket_team_seasons(team_id=2650)
```

## vtb_infobasket_team_comps

The competitions a club played in one season.

**Endpoint URL:** `GET https://org.infobasket.su/Widget/GetTeamComps/{team_id}`

**Valid URL:** [https://org.infobasket.su/Widget/GetTeamComps/2650?format=json&season=2026&lang=en&tag=vtb](https://org.infobasket.su/Widget/GetTeamComps/2650?format=json&season=2026&lang=en&tag=vtb)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Infobasket TeamID (team_id in the calendar, a box score or the v2 matches), e.g. 2650 (Lokomotiv Kuban). |
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26); the wrapper maps it to the season-level Infobasket CompID with vtb_infobasket_seasons (one extra request per tag, cached). Required. |
| `lang` | `lang` |  |  | `Y` | Label language (en or ru). Most bodies carry both languages whatever is asked; the leader categories' labels follow it. |
| `tag` | `tag` |  |  | `Y` | Competition tag scoping the profile: vtb (VTB United League, default), vtbyouth, wbc, wpremier or rfb. The season table is read for this tag. |

### Returns {#vtb_infobasket_team_comps-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | Infobasket TeamID (2650 = Lokomotiv Kuban); the v2 teamId. Not a club-record id and not a registration id. |
| `season` | Int64 | Season END year (2026 = 2025-26): the season the request asked for, stamped by the runtime. |
| `comp_id` | Int64 | Competition id (the season or a stage of it). |
| `comp_type` | Int64 | Stage type code: 0 season, regular season or group; 10 playoff container; 5 and 7 playoff series; -5 seen in season lists. Meanings inferred from the stage names, unverified. |
| `comp_sort` | Int64 | Sort order among sibling stages. |
| `level` | Int64 | Depth of the competition in the tree (1 = season level). |
| `comp_name_ru` | String | Competition name, Russian ('Единая лига ВТБ' = 'VTB United League'). |
| `comp_name_en` | String | Stage name (e.g. 'Regular Season'), English. |
| `parent_comp_id` | Int64 | Unobserved: null in every sample, so its type and meaning are unconfirmed. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#vtb_infobasket_team_comps-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (an empty list, a league the API does not know, a season with no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope or string (HTTP 200), a game that is not final, a partial or possibly truncated page, or a body for another game, person, team, competition, league or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season the competition tag does not have, or an argument that is not an integer id. |

### Example {#vtb_infobasket_team_comps-example}

```python
vtb_infobasket_team_comps(season=2026, team_id=2650)
```

## vtb_infobasket_team_games

A club's games in one season (the calendar's row shape).

**Endpoint URL:** `GET https://org.infobasket.su/Widget/TeamGames/{team_id}`

**Valid URL:** [https://org.infobasket.su/Widget/TeamGames/2650?format=json&season=2026&tag=vtb](https://org.infobasket.su/Widget/TeamGames/2650?format=json&season=2026&tag=vtb)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Infobasket TeamID (team_id in the calendar, a box score or the v2 matches), e.g. 2650 (Lokomotiv Kuban). |
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26); the wrapper maps it to the season-level Infobasket CompID with vtb_infobasket_seasons (one extra request per tag, cached). Required. |
| `tag` | `tag` |  |  | `Y` | Competition tag scoping the profile: vtb (VTB United League, default), vtbyouth, wbc, wpremier or rfb. The season table is read for this tag. |

### Returns {#vtb_infobasket_team_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | Infobasket TeamID (2650 = Lokomotiv Kuban); the v2 teamId. Not a club-record id and not a registration id. |
| `season` | Int64 | Season END year (2026 = 2025-26): the season the request asked for, stamped by the runtime. |
| `comp_id` | Int64 | Season-level Infobasket CompID of the season (50714 = 2025-26): the id every season-keyed route takes. |
| `game_id` | Int64 | Infobasket game id (GameID); equals the v2 API `matchId`. Joins to /Widget/GetOnline/{gameId} and /Widget/GamePage/{gameId}. |
| `game_number` | String | Game number within the competition, e.g. 'RS-002' (RS = regular season); playoff games use the series code. |
| `game_date` | String | Game date as an ISO 8601 date (YYYY-MM-DD); the wire sends DD.MM.YYYY. |
| `has_time` | Boolean | True when a tip-off time is set. |
| `game_time` | String | Tip-off local time, 'HH:MM'. |
| `game_time_msk` | String | Tip-off time in Moscow time ('HH:MM' on calendar rows, 'HH.MM' in GetOnline). |
| `game_date_time` | String | Tip-off instant as an ISO 8601 string in Moscow time with its +03:00 offset (the wire sends a .NET /Date(ms)/ value). |
| `game_date_time_moscow` | String | Tip-off instant as an ISO 8601 string in Moscow time with its +03:00 offset (the wire's second .NET date; equal to game_date_time in every sample). |
| `game_status` | Int64 | Game status: 0 planned, 1 completed, 2 live (the site's widget code). Samples are completed games. |
| `team_a_id` | Int64 | Team A (home) TeamID. Infobasket team id (TeamID): one club's entry in a competition family (Lokomotiv Kuban = 2650). Joins to /Widget/Team*/{teamId} and equals the v2 API `teamId`. |
| `team_b_id` | Int64 | Team B (away) TeamID. Infobasket team id (TeamID): one club's entry in a competition family (Lokomotiv Kuban = 2650). Joins to /Widget/Team*/{teamId} and equals the v2 API `teamId`. |
| `short_team_name_aru` | String | Team short name of team A (home), Russian. |
| `short_team_name_bru` | String | Team short name of team B (away), Russian. |
| `team_name_aru` | String | Team name with city in brackets of team A (home), Russian. |
| `team_name_bru` | String | Team name with city in brackets of team B (away), Russian. |
| `comp_team_name_aru` | String | Team name as registered in the competition of team A (home), Russian. |
| `comp_team_name_bru` | String | Team name as registered in the competition of team B (away), Russian. |
| `region_team_name_aru` | String | Team's city or region of team A (home), Russian. |
| `region_team_name_bru` | String | Team's city or region of team B (away), Russian. |
| `short_team_name_aen` | String | Team short name of team A (home), English. |
| `short_team_name_ben` | String | Team short name of team B (away), English. |
| `team_name_aen` | String | Team name with city in brackets of team A (home), English. |
| `team_name_ben` | String | Team name with city in brackets of team B (away), English. |
| `comp_team_name_aen` | String | Team name as registered in the competition of team A (home), English. |
| `comp_team_name_ben` | String | Team name as registered in the competition of team B (away), English. |
| `region_team_name_aen` | String | Team's city or region of team A (home), English. |
| `region_team_name_ben` | String | Team's city or region of team B (away), English. |
| `score_a` | Int64 | Final score of team A (home). |
| `score_b` | Int64 | Final score of team B (away). |
| `tv_ru` | String | TV channels, in Russian. |
| `tv_en` | String | TV channels, in English. |
| `region_ru` | String | City or region of the game's venue in Russian. |
| `region_en` | String | City or region of the game's venue in English. |
| `arena_en` | String | Arena name, English. |
| `arena_ru` | String | Arena, Russian ('СК' = sports complex). |
| `arena_id` | Int64 | Infobasket arena id; equals the v2 API `venueId` (11687 = SK Basket-Hall, Krasnodar). |
| `game_attendance` | Int64 | Spectators at the game (null when not recorded). |
| `comp_name_ru` | String | Stage name, Russian ('Регулярный чемпионат' = 'Regular Season', 'Группа А' = 'Group A'). |
| `comp_name_en` | String | Stage name (e.g. 'Regular Season'), English. |
| `league_name_ru` | String | League name, Russian ('Единая лига ВТБ' / 'Чемпионат Единой лиги ВТБ' = 'VTB United League' / 'VTB United League Championship'). |
| `league_name_en` | String | League name, English. |
| `league_short_name_ru` | String | League short name, Russian ('ВТБ' = 'VTB'). |
| `league_short_name_en` | String | League short name, English. |
| `gender` | Int64 | Competition gender: 0 on every VTB sample (men). Other values unverified. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#vtb_infobasket_team_games-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (an empty list, a league the API does not know, a season with no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope or string (HTTP 200), a game that is not final, a partial or possibly truncated page, or a body for another game, person, team, competition, league or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season the competition tag does not have, or an argument that is not an integer id. |

### Example {#vtb_infobasket_team_games-example}

```python
vtb_infobasket_team_games(season=2026, team_id=2650)
```

## vtb_infobasket_team_season_stats

A club's season totals in one season.

**Endpoint URL:** `GET https://org.infobasket.su/Widget/TeamStats/{team_id}`

**Valid URL:** [https://org.infobasket.su/Widget/TeamStats/2650?format=json&season=2026&lang=en&tag=vtb](https://org.infobasket.su/Widget/TeamStats/2650?format=json&season=2026&lang=en&tag=vtb)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Infobasket TeamID (team_id in the calendar, a box score or the v2 matches), e.g. 2650 (Lokomotiv Kuban). |
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26); the wrapper maps it to the season-level Infobasket CompID with vtb_infobasket_seasons (one extra request per tag, cached). Required. |
| `lang` | `lang` |  |  | `Y` | Label language (en or ru). Most bodies carry both languages whatever is asked; the leader categories' labels follow it. |
| `tag` | `tag` |  |  | `Y` | Competition tag scoping the profile: vtb (VTB United League, default), vtbyouth, wbc, wpremier or rfb. The season table is read for this tag. |

### Returns {#vtb_infobasket_team_season_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season END year (2026 = 2025-26): the season the request asked for, stamped by the runtime. |
| `team_id` | Int64 | Infobasket team id (TeamID): one club's entry in a competition family (Lokomotiv Kuban = 2650). Joins to /Widget/Team*/{teamId} and equals the v2 API `teamId`. The scope check compares it with the request. |
| `game_count` | Int64 | Games in the totals (the whole competition, all stages, for the season-level CompID). |
| `points` | Int64 | Points. The wire's null is 0 here (null counts are filled). |
| `shot1` | Int64 | Free throws attempted. The wire's null is 0 here (null counts are filled). |
| `goal1` | Int64 | Free throws made. The wire's null is 0 here (null counts are filled). |
| `shot2` | Int64 | 2-point field goals attempted. The wire's null is 0 here (null counts are filled). |
| `goal2` | Int64 | 2-point field goals made. The wire's null is 0 here (null counts are filled). |
| `shot3` | Int64 | 3-point field goals attempted. The wire's null is 0 here (null counts are filled). |
| `goal3` | Int64 | 3-point field goals made. The wire's null is 0 here (null counts are filled). |
| `shot23` | Int64 | Field goals (2 + 3 points) attempted. The wire's null is 0 here (null counts are filled). |
| `goal23` | Int64 | Field goals (2 + 3 points) made. The wire's null is 0 here (null counts are filled). |
| `shot1_percent` | Float64 | Free-throw percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `shot2_percent` | Float64 | 2-point percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `shot3_percent` | Float64 | 3-point percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `shot23_percent` | Float64 | Field-goal percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `assist` | Int64 | Assists. The wire's null is 0 here (null counts are filled). |
| `blocks` | Int64 | Blocked shots. The wire's null is 0 here (null counts are filled). |
| `def_rebound` | Int64 | Defensive rebounds. The wire's null is 0 here (null counts are filled). |
| `off_rebound` | Int64 | Offensive rebounds. The wire's null is 0 here (null counts are filled). |
| `rebound` | Int64 | Total rebounds. The wire's null is 0 here (null counts are filled). |
| `steal` | Int64 | Steals. The wire's null is 0 here (null counts are filled). |
| `turnover` | Int64 | Turnovers. The wire's null is 0 here (null counts are filled). |
| `team_def_rebound` | Int64 | Team defensive rebounds (not credited to a player). The wire's null is 0 here (null counts are filled). |
| `team_off_rebound` | Int64 | Team offensive rebounds (not credited to a player). The wire's null is 0 here (null counts are filled). |
| `team_rebound` | Int64 | Team rebounds (not credited to a player). The wire's null is 0 here (null counts are filled). |
| `team_steal` | Int64 | Team steals (not credited to a player). The wire's null is 0 here (null counts are filled). |
| `team_turnover` | Int64 | Team turnovers (not credited to a player, e.g. shot-clock violations). The wire's null is 0 here (null counts are filled). |
| `kpi` | Int64 | Efficiency index (KPI, the platform's 'КПИ'); formula unverified. |
| `avg_points` | Float64 | Per-game average of: points; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_assist` | Float64 | Per-game average of: assists; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_blocks` | Float64 | Per-game average of: blocked shots; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_def_rebound` | Float64 | Per-game average of: defensive rebounds; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_off_rebound` | Float64 | Per-game average of: offensive rebounds; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_rebound` | Float64 | Per-game average of: total rebounds; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_steal` | Float64 | Per-game average of: steals; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_turnover` | Float64 | Per-game average of: turnovers; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_foul` | Float64 | Per-game average of: personal fouls committed; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_opponent_foul` | Float64 | Per-game average of: fouls drawn; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_plus_minus` | Float64 | Per-game average of: plus-minus: point differential while on the court; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_team_def_rebound` | Float64 | Per-game average of: team defensive rebounds (not credited to a player); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_team_off_rebound` | Float64 | Per-game average of: team offensive rebounds (not credited to a player); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_team_rebound` | Float64 | Per-game average of: team rebounds (not credited to a player); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_team_steal` | Float64 | Per-game average of: team steals (not credited to a player); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_team_turnover` | Float64 | Per-game average of: team turnovers (not credited to a player, e.g. shot-clock violations); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `foul` | Int64 | Personal fouls committed. The wire's null is 0 here (null counts are filled). |
| `opponent_foul` | Int64 | Fouls drawn. The wire's null is 0 here (null counts are filled). |
| `seconds` | Int64 | Playing time in seconds. |
| `played_time` | String | Playing time as 'minutes:seconds' (team rows sum the five players: '200:00' = 40 minutes x 5). |
| `avg_played_time` | String | Average playing time per game as 'minutes:seconds'. |
| `plus_minus` | Int64 | Plus-minus: point differential while on the court. Null where the host sends none. |
| `team_short_name_ru` | String | The the team's short name in Russian (as the competition names it). |
| `team_short_name_en` | String | The the team's short name in English (as the competition names it). |
| `team_name_ru` | String | The the team's name in Russian (as the competition names it). |
| `team_name_en` | String | The the team's name in English (as the competition names it). |
| `team_region_name_ru` | String | The the team's region or city in Russian (as the competition names it). |
| `team_region_name_en` | String | The the team's region or city in English (as the competition names it). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#vtb_infobasket_team_season_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (an empty list, a league the API does not know, a season with no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope or string (HTTP 200), a game that is not final, a partial or possibly truncated page, or a body for another game, person, team, competition, league or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season the competition tag does not have, or an argument that is not an integer id. |

### Example {#vtb_infobasket_team_season_stats-example}

```python
vtb_infobasket_team_season_stats(season=2026, team_id=2650)
```
