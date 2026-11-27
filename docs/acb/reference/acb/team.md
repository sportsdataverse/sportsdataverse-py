# ACB — ACB API (api2.acb.com, Spain) — Team

> ACB — ACB API (api2.acb.com, Spain) — Team — function reference in sdv-py, the SportsDataverse Python package.

## acb_team_box

Team totals of a game per period: everything the team did, and the part credited to the team itself.

**Endpoint URL:** `GET https://api2.acb.com/api/matchdata/Result/boxscores`

**Valid URL:** [https://api2.acb.com/api/matchdata/Result/boxscores?matchId=105401](https://api2.acb.com/api/matchdata/Result/boxscores?matchId=105401)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `matchId` | `match_id` |  | `Y` |  | Required. ACB match id (integer), one global sequence for every competition: legacy ids (seasons up to 2019-20) sit below about 20000 (12559 = the 2005-06 final); ids from 2020-21 start around 100000 (105401 = Barca 89-86 Valencia Basket, Liga 2026-27 round 3). |

### Returns {#acb_team_box-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | Int64 | ACB match id (one global sequence for every competition; legacy ids are below about 20000 up to 2019-20, ids from 2020-21 start around 100000). Taken from the request when the body does not name the game. |
| `edition_id` | Int64 | Season id (editionId) = season START year - 1935 (91 = 2026-27). The wrappers take the END year, `season`. |
| `season` | Int64 | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `team_side` | String | Side of the game the team played: home or away (the feed lists the home team first). |
| `team_id` | Int64 | Season-team entry id: one club in one competition edition, NEW every season (Barca = 4467 in Liga 2026-27). Not the club: use club_id to follow a club across seasons. |
| `club_id` | Int64 | Stable ACB club id (2 = Barca, 9 = Real Madrid, 13 = Valencia Basket): the same across seasons and sponsor names, and the club_id request argument. |
| `team` | String | Team name as registered for that season (sponsor names change; the club id does not). |
| `team_abbr` | String | Three-letter team abbreviation (BAR, RMB, VBC). |
| `quarter` | Int64 | Period: 0 = the whole game, 1-4 the quarters, 5+ overtimes (games before about 2010 carry only 0). |
| `scope` | String | total = everything the team did (its players plus the team's own events); team = the part credited to the team itself rather than a player (team rebounds, team turnovers). |
| `points` | Int64 | Points, team total (players + team) |
| `free_throws_made` | Int64 | Free throws made, team total (players + team) |
| `free_throws_attempted` | Int64 | Free throws attempted, team total (players + team) |
| `two_pointers_made` | Int64 | Two-point field goals made, team total (players + team) |
| `two_pointers_attempted` | Int64 | Two-point field goals attempted, team total (players + team) |
| `three_pointers_made` | Int64 | Three-point field goals made, team total (players + team) |
| `three_pointers_attempted` | Int64 | Three-point field goals attempted, team total (players + team) |
| `assists` | Int64 | Assists, team total (players + team) |
| `off_rebounds` | Int64 | Offensive rebounds, team total (players + team) |
| `def_rebounds` | Int64 | Defensive rebounds, team total (players + team) |
| `total_rebounds` | Int64 | Total rebounds, team total (players + team) |
| `steals` | Int64 | Steals, team total (players + team) |
| `turnovers` | Int64 | Turnovers, team total (players + team) |
| `blocks` | Int64 | Shots blocked (by this player or team), team total (players + team) |
| `received_blocks` | Int64 | Own shots blocked by an opponent, team total (players + team) |
| `dunks` | Int64 | Dunks, team total (players + team) |
| `personal_fouls` | Int64 | Fouls committed, team total (players + team) |
| `fouls_drawn` | Int64 | Fouls received, team total (players + team) |
| `plus_minus` | Int64 | Point differential while on court (+/-), team total (players + team) |
| `rating` | Int64 | Valoracion: ACB's efficiency rating (PIR) = points + rebounds + assists + steals + blocks + fouls received - missed field goals - missed free throws - turnovers - shots blocked by an opponent - fouls committed, team total (players + team) |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_team_box-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The ACB has nothing for the request (an unknown id, play-by-play or shots before 2016-17, a club with no games that season); a game outside a playoff series is zero rows. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_team_box-example}

```python
acb_team_box(match_id=105401)
```

## acb_team_comparison

Team-versus-team comparison bars of a game per period.

**Endpoint URL:** `GET https://api2.acb.com/api/matchdata/Overview/match-team-comparison`

**Valid URL:** [https://api2.acb.com/api/matchdata/Overview/match-team-comparison?matchId=105401](https://api2.acb.com/api/matchdata/Overview/match-team-comparison?matchId=105401)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `matchId` | `match_id` |  | `Y` |  | Required. ACB match id (integer), one global sequence for every competition: legacy ids (seasons up to 2019-20) sit below about 20000 (12559 = the 2005-06 final); ids from 2020-21 start around 100000 (105401 = Barca 89-86 Valencia Basket, Liga 2026-27 round 3). |

### Returns {#acb_team_comparison-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | Int64 | ACB match id (one global sequence for every competition; legacy ids are below about 20000 up to 2019-20, ids from 2020-21 start around 100000). Taken from the request when the body does not name the game. |
| `quarter` | Int64 | Period of the bar: 0 = the whole game, 1-4 quarters. |
| `stat` | String | Compared statistic (free_throws, two_pointers, three_pointers, field_goals, rebounds, assists, steals, turnovers, personal_fouls, time_outs, points_off_turnover, paint_points, second_chance_points, fast_break_points, bench_points ...). |
| `home_value` | Float64 | Home value of the bar: a percentage 0-100 for the shooting statistics (the API's '79,17%' decimal comma parsed), a count otherwise. |
| `away_value` | Float64 | Away value of the bar, same units as home_value. |
| `home_made` | Int64 | Home shots made, from the 'made/attempted' subtitle; null for the non-shooting statistics. |
| `home_attempted` | Int64 | Home shots attempted, from the subtitle; null for the non-shooting statistics. |
| `away_made` | Int64 | Away shots made, from the subtitle; null for the non-shooting statistics. |
| `away_attempted` | Int64 | Away shots attempted, from the subtitle; null for the non-shooting statistics. |
| `home_is_highlighted` | Boolean | True when the site highlights the home bar as the better value. |
| `away_is_highlighted` | Boolean | True when the site highlights the away bar as the better value. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_team_comparison-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The ACB has nothing for the request (an unknown id, play-by-play or shots before 2016-17, a club with no games that season); a game outside a playoff series is zero rows. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_team_comparison-example}

```python
acb_team_comparison(match_id=105401)
```

## acb_teams

Teams entered in a competition edition.

**Endpoint URL:** `GET https://api2.acb.com/api/seasondata/TeamProfile/teams`

**Valid URL:** [https://api2.acb.com/api/seasondata/TeamProfile/teams?season=2027](https://api2.acb.com/api/seasondata/TeamProfile/teams?season=2027)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competitionId` | `competition_id` |  |  | `Y` | Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa (youth). Omitted = the API's default competition. |
| `season` | `season` |  |  | `Y` | Season END year (2027 = 2026-27; the first ACB season, 1983-84, is 1984). ``acb_runtime`` sends the API's ``editionId`` = season - 1936. Omitted = the current season. |

### Returns {#acb_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | Season-team entry of the selected edition (new every season). |
| `club_id` | Int64 | Club id, stable across seasons and sponsor names (2 = Barça, 9 = Real Madrid, 13 = Valencia Basket): the `clubId` request param and every `homeClubId` / `awayClubId`. |
| `team` | String | Team name that season (sponsor names change) |
| `team_abbr` | String | Three-letter abbreviation (BAR, RMB) |
| `competition_id` | Int64 | Competition of the selected entry. |
| `edition_id` | Int64 | Season id of the selected entry (START year - 1935). |
| `season` | Int64 | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `latest_team_id` | Int64 | The club's most recent season-team entry. |
| `latest_team` | String | The club's most recent team name. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_teams-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The ACB has nothing for the request (an unknown id, play-by-play or shots before 2016-17, a club with no games that season); a game outside a playoff series is zero rows. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_teams-example}

```python
acb_teams(season=2027)
```

## acb_team_matches

A club's games in a competition edition.

**Endpoint URL:** `GET https://api2.acb.com/api/seasondata/TeamProfile/team-matches`

**Valid URL:** [https://api2.acb.com/api/seasondata/TeamProfile/team-matches?clubId=2&season=2027](https://api2.acb.com/api/seasondata/TeamProfile/team-matches?clubId=2&season=2027)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `clubId` | `club_id` |  | `Y` |  | Required. ACB club id (integer), stable across seasons and sponsor names (2 = Barca, 9 = Real Madrid, 13 = Valencia Basket). Not the season-team ``team_id``. |
| `competitionId` | `competition_id` |  |  | `Y` | Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa (youth). Omitted = the API's default competition. |
| `season` | `season` |  |  | `Y` | Season END year (2027 = 2026-27; the first ACB season, 1983-84, is 1984). ``acb_runtime`` sends the API's ``editionId`` = season - 1936. Omitted = the current season. |

### Returns {#acb_team_matches-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | Int64 | Match id. Legacy ids (seasons up to 2019-20) sit below about 20000 (12559 = 2005-06 final, 17718 = 2016-17 final); from 2020-21 they start at about 100000 (102920 in 2021-22, 105401 in 2026-27). One key space across competitions: the matchId param of every matchdata route. |
| `round_type` | String | Phase code: LR = Liga Regular (regular season), PO = playoffs (a bundle value, not in the samples); null on every row of /Menu/competition-data and /TeamProfile/next-matches |
| `round_id` | Int64 | Round id: global across seasons, not the round number (6017 = Liga 2026-27 round 3; 2600 = 2016-17 round 1) |
| `round_number` | Int64 | Round number within the phase |
| `group_id` | Int64 | Group id in group-stage competitions |
| `subphase_id` | Int64 | Sub-phase id of the round (playoff stages); null in the regular season. |
| `subphase_number` | Int64 | Sub-phase number of the round; null in the regular season. |
| `week_id` | Int64 | Week id: global across seasons (2989 = the week of 2026-10-10) |
| `status` | String | Match state (x-match-statuses) |
| `is_on_half_time` | Boolean | Not sent by this route (always null). |
| `current_quarter` | Int64 | Not sent by this route (always null). |
| `home_score` | Int64 | Home points (null before the match) |
| `away_score` | Int64 | Away points (null before the match) |
| `start` | String | Tip-off, ISO 8601 UTC |
| `home_wins` | Int64 | Not sent by this route (series records are in acb_matchlist). |
| `home_losses` | Int64 | Not sent by this route (series records are in acb_matchlist). |
| `away_wins` | Int64 | Not sent by this route (series records are in acb_matchlist). |
| `away_losses` | Int64 | Not sent by this route (series records are in acb_matchlist). |
| `home_team_id` | Int64 | Season-team id: one team's entry in one competition edition (4467 = Barça in Liga 2026-27, 4388 = Real Madrid in Liga 2025-26). Changes every season and competition. Joins every `homeTeamId` / `awayTeamId` / `teamId` / `teamEditionId`. Not the club id. |
| `home_club_id` | Int64 | Club id, stable across seasons and sponsor names (2 = Barça, 9 = Real Madrid, 13 = Valencia Basket): the `clubId` request param and every `homeClubId` / `awayClubId`. |
| `home_team` | String | Team name that season (sponsor names change) |
| `home_team_abbr` | String | Three-letter abbreviation (BAR, RMB) |
| `away_team_id` | Int64 | Season-team id: one team's entry in one competition edition (4467 = Barça in Liga 2026-27, 4388 = Real Madrid in Liga 2025-26). Changes every season and competition. Joins every `homeTeamId` / `awayTeamId` / `teamId` / `teamEditionId`. Not the club id. |
| `away_club_id` | Int64 | Club id, stable across seasons and sponsor names (2 = Barça, 9 = Real Madrid, 13 = Valencia Basket): the `clubId` request param and every `homeClubId` / `awayClubId`. |
| `away_team` | String | Team name that season (sponsor names change) |
| `away_team_abbr` | String | Three-letter abbreviation (BAR, RMB) |
| `competition_id` | Int64 | Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa (x-competitions) |
| `edition_id` | Int64 | Season id (`editionId`) = season START year - 1935 (91 = 2026-27, 81 = 2016-17, 70 = 2005-06, 48 = 1983-84, the first). The sdv season (END year) is editionId + 1936. Season lists also carry `seasonStartYear` / `seasonEndYear` |
| `season` | Int64 | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `is_selected_local` | Boolean | True when the requested club played at home. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_team_matches-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The ACB has nothing for the request (an unknown id, play-by-play or shots before 2016-17, a club with no games that season); a game outside a playoff series is zero rows. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_team_matches-example}

```python
acb_team_matches(club_id=2, season=2027)
```

## acb_team_leaders

A club's statistical leaders for an edition and all-time.

**Endpoint URL:** `GET https://api2.acb.com/api/seasondata/TeamProfile/team-leaders`

**Valid URL:** [https://api2.acb.com/api/seasondata/TeamProfile/team-leaders?clubId=2&numberOfPlayers=3&season=2027](https://api2.acb.com/api/seasondata/TeamProfile/team-leaders?clubId=2&numberOfPlayers=3&season=2027)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `clubId` | `club_id` |  | `Y` |  | Required. ACB club id (integer), stable across seasons and sponsor names (2 = Barca, 9 = Real Madrid, 13 = Valencia Basket). Not the season-team ``team_id``. |
| `numberOfPlayers` | `number_of_players` |  | `Y` |  | Required. Leaders per statistic; 0 is refused (HTTP 400). |
| `competitionId` | `competition_id` |  |  | `Y` | Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa (youth). Omitted = the API's default competition. |
| `season` | `season` |  |  | `Y` | Season END year (2027 = 2026-27; the first ACB season, 1983-84, is 1984). ``acb_runtime`` sends the API's ``editionId`` = season - 1936. Omitted = the current season. |

### Returns {#acb_team_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `club_id` | Int64 | Stable club id asked for. |
| `competition_id` | Int64 | Competition of the selected edition. |
| `edition_id` | Int64 | Season id (editionId) = season START year - 1935 (91 = 2026-27). The wrappers take the END year, `season`. |
| `season` | Int64 | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `scope` | String | all_time = club record holders (the API's leaders); edition = the selected season (seasonLeaders). The assignment is inferred from the values (689 games for J.C. Navarro is a career figure). |
| `metric` | String | Category in snake_case; avg_* are per-game averages, the rest totals. |
| `rank` | Int64 | Place in the category list, 1 = the leader, in the order the feed lists them. |
| `value` | Float64 | The player's value in the category (a total or an average depending on the metric). |
| `player_id` | Int64 | ACB licence number: the person id everywhere. A player's box-score `player.id`, his play-by-play and shot `playerLicenseId` and the `playerLicenceId` / `playerLicense` request params hold the same number (all 24 players of match 105401 match). 20209407 = Edy Tavares. Older licences start 2020..., recent ones 3000.... |
| `player_name` | String | Display (sporting) name ('Edy Tavares') |
| `first_name` | String | Legal first name(s) |
| `last_name` | String | Legal last name(s) |
| `shirt_number` | String | Shirt number, a string ('' or null when unknown) |
| `game_role` | String | Position or staff role, in Spanish: Base (point guard), Escolta (shooting guard), Alero (small forward), Ala-pivot (power forward), Pivot (centre); Entrenador (head coach), Segundo Entrenador / Entrenador Ayudante (assistant coach). Null or '' in many rows |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_team_leaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The ACB has nothing for the request (an unknown id, play-by-play or shots before 2016-17, a club with no games that season); a game outside a playoff series is zero rows. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_team_leaders-example}

```python
acb_team_leaders(club_id=2, number_of_players=3, season=2027)
```

## acb_team_stats

A club's player season statistics for an edition: totals, averages and per-40 side by side.

**Endpoint URL:** `GET https://api2.acb.com/api/seasondata/TeamProfile/team-stats`

**Valid URL:** [https://api2.acb.com/api/seasondata/TeamProfile/team-stats?clubId=2&season=2027](https://api2.acb.com/api/seasondata/TeamProfile/team-stats?clubId=2&season=2027)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `clubId` | `club_id` |  | `Y` |  | Required. ACB club id (integer), stable across seasons and sponsor names (2 = Barca, 9 = Real Madrid, 13 = Valencia Basket). Not the season-team ``team_id``. |
| `competitionId` | `competition_id` |  |  | `Y` | Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa (youth). Omitted = the API's default competition. |
| `season` | `season` |  |  | `Y` | Season END year (2027 = 2026-27; the first ACB season, 1983-84, is 1984). ``acb_runtime`` sends the API's ``editionId`` = season - 1936. Omitted = the current season. |

### Returns {#acb_team_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `club_id` | Int64 | Stable club id of the club asked for (2 = Barca). |
| `team_id` | Int64 | Season-team entry id of the selected edition. |
| `edition_id` | Int64 | Season id (editionId) = season START year - 1935 (91 = 2026-27). The wrappers take the END year, `season`. |
| `season` | Int64 | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `player_id` | Int64 | ACB licence number: the person id everywhere. A player's box-score `player.id`, his play-by-play and shot `playerLicenseId` and the `playerLicenceId` / `playerLicense` request params hold the same number (all 24 players of match 105401 match). 20209407 = Edy Tavares. Older licences start 2020..., recent ones 3000.... |
| `player_name` | String | Display (sporting) name ('Edy Tavares') |
| `first_name` | String | Legal first name(s) |
| `last_name` | String | Legal last name(s) |
| `shirt_number` | String | Shirt number, a string ('' or null when unknown) |
| `game_role` | String | Position or staff role, in Spanish: Base (point guard), Escolta (shooting guard), Alero (small forward), Ala-pivot (power forward), Pivot (centre); Entrenador (head coach), Segundo Entrenador / Entrenador Ayudante (assistant coach). Null or '' in many rows |
| `total_num_games` | Int64 | Season total of games played. |
| `total_time_played` | Int64 | Season total of time on court in seconds. |
| `total_starter` | Int64 | Season total of games started. |
| `total_points` | Int64 | Season total of points. |
| `total_max_points` | Int64 | Season total of most points in a single game. |
| `total_three_pointers_made` | Int64 | Season total of three-point field goals made. |
| `total_three_pointers_attempted` | Int64 | Season total of three-point field goals attempted. |
| `total_three_pointers_percentage` | Float64 | Season total of three-point percentage (0-100). |
| `total_two_pointers_made` | Int64 | Season total of two-point field goals made. |
| `total_two_pointers_attempted` | Int64 | Season total of two-point field goals attempted. |
| `total_two_pointers_percentage` | Float64 | Season total of two-point percentage (0-100). |
| `total_free_throws_made` | Int64 | Season total of free throws made. |
| `total_free_throws_attempted` | Int64 | Season total of free throws attempted. |
| `total_free_throws_percentage` | Float64 | Season total of free-throw percentage (0-100). |
| `total_rebounds` | Int64 | Season total of total rebounds. |
| `total_offensive_rebounds` | Int64 | Season total of offensive rebounds. |
| `total_defensive_rebounds` | Int64 | Season total of defensive rebounds. |
| `total_assists` | Int64 | Season total of assists. |
| `total_steals` | Int64 | Season total of steals. |
| `total_turnovers` | Int64 | Season total of turnovers. |
| `total_blocks` | Int64 | Season total of blocks made. |
| `total_shots_rejected` | Int64 | Season total of own shots blocked. |
| `total_dunks` | Int64 | Season total of dunks. |
| `total_personal_fouls` | Int64 | Season total of personal fouls committed. |
| `total_fouls_received` | Int64 | Season total of fouls drawn. |
| `total_rating` | Int64 | Season total of ACB rating (valoracion). |
| `total_plus_minus` | Int64 | Season total of plus/minus. |
| `total_wins` | Int64 | Season total of team wins in the counted games (meaning unverified). |
| `total_losses` | Int64 | Season total of team losses in the counted games (meaning unverified). |
| `avg_num_games` | Float64 | Per-game average of games played. |
| `avg_time_played` | Float64 | Per-game average of time on court in seconds. |
| `avg_starter` | Float64 | Per-game average of games started. |
| `avg_points` | Float64 | Per-game average of points. |
| `avg_max_points` | Float64 | Per-game average of most points in a single game. |
| `avg_three_pointers_made` | Float64 | Per-game average of three-point field goals made. |
| `avg_three_pointers_attempted` | Float64 | Per-game average of three-point field goals attempted. |
| `avg_three_pointers_percentage` | Float64 | Per-game average of three-point percentage (0-100). |
| `avg_two_pointers_made` | Float64 | Per-game average of two-point field goals made. |
| `avg_two_pointers_attempted` | Float64 | Per-game average of two-point field goals attempted. |
| `avg_two_pointers_percentage` | Float64 | Per-game average of two-point percentage (0-100). |
| `avg_free_throws_made` | Float64 | Per-game average of free throws made. |
| `avg_free_throws_attempted` | Float64 | Per-game average of free throws attempted. |
| `avg_free_throws_percentage` | Float64 | Per-game average of free-throw percentage (0-100). |
| `avg_rebounds` | Float64 | Per-game average of total rebounds. |
| `avg_offensive_rebounds` | Float64 | Per-game average of offensive rebounds. |
| `avg_defensive_rebounds` | Float64 | Per-game average of defensive rebounds. |
| `avg_assists` | Float64 | Per-game average of assists. |
| `avg_steals` | Float64 | Per-game average of steals. |
| `avg_turnovers` | Float64 | Per-game average of turnovers. |
| `avg_blocks` | Float64 | Per-game average of blocks made. |
| `avg_shots_rejected` | Float64 | Per-game average of own shots blocked. |
| `avg_dunks` | Float64 | Per-game average of dunks. |
| `avg_personal_fouls` | Float64 | Per-game average of personal fouls committed. |
| `avg_fouls_received` | Float64 | Per-game average of fouls drawn. |
| `avg_rating` | Float64 | Per-game average of ACB rating (valoracion). |
| `avg_plus_minus` | Float64 | Per-game average of plus/minus. |
| `avg_wins` | Float64 | Per-game average of team wins in the counted games (meaning unverified). |
| `avg_losses` | Float64 | Per-game average of team losses in the counted games (meaning unverified). |
| `per40_num_games` | Float64 | Per-40-minute rate of games played. |
| `per40_time_played` | Float64 | Always null: the per-40 block carries no time played. |
| `per40_starter` | Float64 | Per-40-minute rate of games started. |
| `per40_points` | Float64 | Per-40-minute rate of points. |
| `per40_max_points` | Float64 | Per-40-minute rate of most points in a single game. |
| `per40_three_pointers_made` | Float64 | Per-40-minute rate of three-point field goals made. |
| `per40_three_pointers_attempted` | Float64 | Per-40-minute rate of three-point field goals attempted. |
| `per40_three_pointers_percentage` | Float64 | Per-40-minute rate of three-point percentage (0-100). |
| `per40_two_pointers_made` | Float64 | Per-40-minute rate of two-point field goals made. |
| `per40_two_pointers_attempted` | Float64 | Per-40-minute rate of two-point field goals attempted. |
| `per40_two_pointers_percentage` | Float64 | Per-40-minute rate of two-point percentage (0-100). |
| `per40_free_throws_made` | Float64 | Per-40-minute rate of free throws made. |
| `per40_free_throws_attempted` | Float64 | Per-40-minute rate of free throws attempted. |
| `per40_free_throws_percentage` | Float64 | Per-40-minute rate of free-throw percentage (0-100). |
| `per40_rebounds` | Float64 | Per-40-minute rate of total rebounds. |
| `per40_offensive_rebounds` | Float64 | Per-40-minute rate of offensive rebounds. |
| `per40_defensive_rebounds` | Float64 | Per-40-minute rate of defensive rebounds. |
| `per40_assists` | Float64 | Per-40-minute rate of assists. |
| `per40_steals` | Float64 | Per-40-minute rate of steals. |
| `per40_turnovers` | Float64 | Per-40-minute rate of turnovers. |
| `per40_blocks` | Float64 | Per-40-minute rate of blocks made. |
| `per40_shots_rejected` | Float64 | Per-40-minute rate of own shots blocked. |
| `per40_dunks` | Float64 | Per-40-minute rate of dunks. |
| `per40_personal_fouls` | Float64 | Per-40-minute rate of personal fouls committed. |
| `per40_fouls_received` | Float64 | Per-40-minute rate of fouls drawn. |
| `per40_rating` | Float64 | Per-40-minute rate of ACB rating (valoracion). |
| `per40_plus_minus` | Float64 | Per-40-minute rate of plus/minus. |
| `per40_wins` | Float64 | Per-40-minute rate of team wins in the counted games (meaning unverified). |
| `per40_losses` | Float64 | Per-40-minute rate of team losses in the counted games (meaning unverified). |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_team_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The ACB has nothing for the request (an unknown id, play-by-play or shots before 2016-17, a club with no games that season); a game outside a playoff series is zero rows. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_team_stats-example}

```python
acb_team_stats(club_id=2, season=2027)
```

## acb_team_totals

A club's team season totals, averages and per-40 values for an edition.

**Endpoint URL:** `GET https://api2.acb.com/api/seasondata/TeamProfile/team-stats`

**Valid URL:** [https://api2.acb.com/api/seasondata/TeamProfile/team-stats?clubId=2&season=2027](https://api2.acb.com/api/seasondata/TeamProfile/team-stats?clubId=2&season=2027)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `clubId` | `club_id` |  | `Y` |  | Required. ACB club id (integer), stable across seasons and sponsor names (2 = Barca, 9 = Real Madrid, 13 = Valencia Basket). Not the season-team ``team_id``. |
| `competitionId` | `competition_id` |  |  | `Y` | Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa (youth). Omitted = the API's default competition. |
| `season` | `season` |  |  | `Y` | Season END year (2027 = 2026-27; the first ACB season, 1983-84, is 1984). ``acb_runtime`` sends the API's ``editionId`` = season - 1936. Omitted = the current season. |

### Returns {#acb_team_totals-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `club_id` | Int64 | Stable club id of the club asked for (2 = Barca). |
| `team_id` | Int64 | Season-team entry id (the API's teamEditionId). |
| `edition_id` | Int64 | Season id (editionId) = season START year - 1935 (91 = 2026-27). The wrappers take the END year, `season`. |
| `season` | Int64 | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `total_num_games` | Int64 | Season total of games played. |
| `total_time_played` | Int64 | Season total of time on court in seconds. |
| `total_starter` | Int64 | Season total of games started. |
| `total_points` | Int64 | Season total of points. |
| `total_max_points` | Int64 | Season total of most points in a single game. |
| `total_three_pointers_made` | Int64 | Season total of three-point field goals made. |
| `total_three_pointers_attempted` | Int64 | Season total of three-point field goals attempted. |
| `total_three_pointers_percentage` | Float64 | Season total of three-point percentage (0-100). |
| `total_two_pointers_made` | Int64 | Season total of two-point field goals made. |
| `total_two_pointers_attempted` | Int64 | Season total of two-point field goals attempted. |
| `total_two_pointers_percentage` | Float64 | Season total of two-point percentage (0-100). |
| `total_free_throws_made` | Int64 | Season total of free throws made. |
| `total_free_throws_attempted` | Int64 | Season total of free throws attempted. |
| `total_free_throws_percentage` | Float64 | Season total of free-throw percentage (0-100). |
| `total_rebounds` | Int64 | Season total of total rebounds. |
| `total_offensive_rebounds` | Int64 | Season total of offensive rebounds. |
| `total_defensive_rebounds` | Int64 | Season total of defensive rebounds. |
| `total_assists` | Int64 | Season total of assists. |
| `total_steals` | Int64 | Season total of steals. |
| `total_turnovers` | Int64 | Season total of turnovers. |
| `total_blocks` | Int64 | Season total of blocks made. |
| `total_shots_rejected` | Int64 | Season total of own shots blocked. |
| `total_dunks` | Int64 | Season total of dunks. |
| `total_personal_fouls` | Int64 | Season total of personal fouls committed. |
| `total_fouls_received` | Int64 | Season total of fouls drawn. |
| `total_rating` | Int64 | Season total of ACB rating (valoracion). |
| `total_plus_minus` | Int64 | Season total of plus/minus. |
| `total_wins` | Int64 | Season total of team wins in the counted games (meaning unverified). |
| `total_losses` | Int64 | Season total of team losses in the counted games (meaning unverified). |
| `avg_num_games` | Float64 | Per-game average of games played. |
| `avg_time_played` | Float64 | Per-game average of time on court in seconds. |
| `avg_starter` | Float64 | Per-game average of games started. |
| `avg_points` | Float64 | Per-game average of points. |
| `avg_max_points` | Float64 | Per-game average of most points in a single game. |
| `avg_three_pointers_made` | Float64 | Per-game average of three-point field goals made. |
| `avg_three_pointers_attempted` | Float64 | Per-game average of three-point field goals attempted. |
| `avg_three_pointers_percentage` | Float64 | Per-game average of three-point percentage (0-100). |
| `avg_two_pointers_made` | Float64 | Per-game average of two-point field goals made. |
| `avg_two_pointers_attempted` | Float64 | Per-game average of two-point field goals attempted. |
| `avg_two_pointers_percentage` | Float64 | Per-game average of two-point percentage (0-100). |
| `avg_free_throws_made` | Float64 | Per-game average of free throws made. |
| `avg_free_throws_attempted` | Float64 | Per-game average of free throws attempted. |
| `avg_free_throws_percentage` | Float64 | Per-game average of free-throw percentage (0-100). |
| `avg_rebounds` | Float64 | Per-game average of total rebounds. |
| `avg_offensive_rebounds` | Float64 | Per-game average of offensive rebounds. |
| `avg_defensive_rebounds` | Float64 | Per-game average of defensive rebounds. |
| `avg_assists` | Float64 | Per-game average of assists. |
| `avg_steals` | Float64 | Per-game average of steals. |
| `avg_turnovers` | Float64 | Per-game average of turnovers. |
| `avg_blocks` | Float64 | Per-game average of blocks made. |
| `avg_shots_rejected` | Float64 | Per-game average of own shots blocked. |
| `avg_dunks` | Float64 | Per-game average of dunks. |
| `avg_personal_fouls` | Float64 | Per-game average of personal fouls committed. |
| `avg_fouls_received` | Float64 | Per-game average of fouls drawn. |
| `avg_rating` | Float64 | Per-game average of ACB rating (valoracion). |
| `avg_plus_minus` | Float64 | Per-game average of plus/minus. |
| `avg_wins` | Float64 | Per-game average of team wins in the counted games (meaning unverified). |
| `avg_losses` | Float64 | Per-game average of team losses in the counted games (meaning unverified). |
| `per40_num_games` | Float64 | Per-40-minute rate of games played. |
| `per40_time_played` | Float64 | Always null: the per-40 block carries no time played. |
| `per40_starter` | Float64 | Per-40-minute rate of games started. |
| `per40_points` | Float64 | Per-40-minute rate of points. |
| `per40_max_points` | Float64 | Per-40-minute rate of most points in a single game. |
| `per40_three_pointers_made` | Float64 | Per-40-minute rate of three-point field goals made. |
| `per40_three_pointers_attempted` | Float64 | Per-40-minute rate of three-point field goals attempted. |
| `per40_three_pointers_percentage` | Float64 | Per-40-minute rate of three-point percentage (0-100). |
| `per40_two_pointers_made` | Float64 | Per-40-minute rate of two-point field goals made. |
| `per40_two_pointers_attempted` | Float64 | Per-40-minute rate of two-point field goals attempted. |
| `per40_two_pointers_percentage` | Float64 | Per-40-minute rate of two-point percentage (0-100). |
| `per40_free_throws_made` | Float64 | Per-40-minute rate of free throws made. |
| `per40_free_throws_attempted` | Float64 | Per-40-minute rate of free throws attempted. |
| `per40_free_throws_percentage` | Float64 | Per-40-minute rate of free-throw percentage (0-100). |
| `per40_rebounds` | Float64 | Per-40-minute rate of total rebounds. |
| `per40_offensive_rebounds` | Float64 | Per-40-minute rate of offensive rebounds. |
| `per40_defensive_rebounds` | Float64 | Per-40-minute rate of defensive rebounds. |
| `per40_assists` | Float64 | Per-40-minute rate of assists. |
| `per40_steals` | Float64 | Per-40-minute rate of steals. |
| `per40_turnovers` | Float64 | Per-40-minute rate of turnovers. |
| `per40_blocks` | Float64 | Per-40-minute rate of blocks made. |
| `per40_shots_rejected` | Float64 | Per-40-minute rate of own shots blocked. |
| `per40_dunks` | Float64 | Per-40-minute rate of dunks. |
| `per40_personal_fouls` | Float64 | Per-40-minute rate of personal fouls committed. |
| `per40_fouls_received` | Float64 | Per-40-minute rate of fouls drawn. |
| `per40_rating` | Float64 | Per-40-minute rate of ACB rating (valoracion). |
| `per40_plus_minus` | Float64 | Per-40-minute rate of plus/minus. |
| `per40_wins` | Float64 | Per-40-minute rate of team wins in the counted games (meaning unverified). |
| `per40_losses` | Float64 | Per-40-minute rate of team losses in the counted games (meaning unverified). |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_team_totals-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The ACB has nothing for the request (an unknown id, play-by-play or shots before 2016-17, a club with no games that season); a game outside a playoff series is zero rows. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_team_totals-example}

```python
acb_team_totals(club_id=2, season=2027)
```

## acb_team_records

A club's single-game records (best performance per statistic).

**Endpoint URL:** `GET https://api2.acb.com/api/seasondata/TeamProfile/team-records`

**Valid URL:** [https://api2.acb.com/api/seasondata/TeamProfile/team-records?clubId=2](https://api2.acb.com/api/seasondata/TeamProfile/team-records?clubId=2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `clubId` | `club_id` |  | `Y` |  | Required. ACB club id (integer), stable across seasons and sponsor names (2 = Barca, 9 = Real Madrid, 13 = Valencia Basket). Not the season-team ``team_id``. |

### Returns {#acb_team_records-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `club_id` | Int64 | Stable club id asked for. |
| `stat` | String | Record category in snake_case (rating, points, rebounds, assists, steals, ...). |
| `value` | Float64 | The record value (a count; Float64 so every category fits). |
| `game_id` | Int64 | Match id of the record game (legacy sequence for old games). |
| `game_date` | String | Date of the record game (UTC midnight). |
| `player_id` | Int64 | ACB licence number: the person id everywhere. A player's box-score `player.id`, his play-by-play and shot `playerLicenseId` and the `playerLicenceId` / `playerLicense` request params hold the same number (all 24 players of match 105401 match). 20209407 = Edy Tavares. Older licences start 2020..., recent ones 3000.... |
| `player_name` | String | Display (sporting) name ('Edy Tavares') |
| `first_name` | String | Legal first name(s) |
| `last_name` | String | Legal last name(s) |
| `shirt_number` | String | Shirt number, a string ('' or null when unknown) |
| `game_role` | String | Position or staff role, in Spanish: Base (point guard), Escolta (shooting guard), Alero (small forward), Ala-pivot (power forward), Pivot (centre); Entrenador (head coach), Segundo Entrenador / Entrenador Ayudante (assistant coach). Null or '' in many rows |
| `local_team_id` | Int64 | Home season-team entry of the record game. |
| `local_club_id` | Int64 | Home stable club id of the record game. |
| `local_team` | String | Home team name in the record game. |
| `local_team_abbr` | String | Home team abbreviation in the record game. |
| `visitor_team_id` | Int64 | Away season-team entry of the record game. |
| `visitor_club_id` | Int64 | Away stable club id of the record game. |
| `visitor_team` | String | Away team name in the record game. |
| `visitor_team_abbr` | String | Away team abbreviation in the record game. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_team_records-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The ACB has nothing for the request (an unknown id, play-by-play or shots before 2016-17, a club with no games that season); a game outside a playoff series is zero rows. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_team_records-example}

```python
acb_team_records(club_id=2)
```
