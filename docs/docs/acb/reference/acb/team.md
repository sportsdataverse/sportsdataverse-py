---
title: "ACB — ACB API (api2.acb.com, Spain) — Team"
sidebar_label: "Team"
sidebar_position: 3
description: "ACB — ACB API (api2.acb.com, Spain) — Team — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# ACB — ACB API (api2.acb.com, Spain) — Team

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
| `match_id` | integer | ACB match id (one global sequence for every competition; legacy ids are below about 20000 up to 2019-20, ids from 2020-21 start around 100000). Taken from the request when the body does not name the game. |
| `edition_id` | integer | Season id (editionId) = season START year - 1935 (91 = 2026-27). The wrappers take the END year, `season`. |
| `season` | integer | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `team_side` | character | Side of the game the team played: home or away (the feed lists the home team first). |
| `team_id` | integer | Season-team entry id: one club in one competition edition, NEW every season (Barca = 4467 in Liga 2026-27). Not the club: use club_id to follow a club across seasons. |
| `club_id` | integer | Stable ACB club id (2 = Barca, 9 = Real Madrid, 13 = Valencia Basket): the same across seasons and sponsor names, and the club_id request argument. |
| `team` | character | Team name as registered for that season (sponsor names change; the club id does not). |
| `team_abbr` | character | Three-letter team abbreviation (BAR, RMB, VBC). |
| `quarter` | integer | Period: 0 = the whole game, 1-4 the quarters, 5+ overtimes (games before about 2010 carry only 0). |
| `scope` | character | total = everything the team did (its players plus the team's own events); team = the part credited to the team itself rather than a player (team rebounds, team turnovers). |
| `points` | integer | Points, team total (players + team) |
| `free_throws_made` | integer | Free throws made, team total (players + team) |
| `free_throws_attempted` | integer | Free throws attempted, team total (players + team) |
| `two_pointers_made` | integer | Two-point field goals made, team total (players + team) |
| `two_pointers_attempted` | integer | Two-point field goals attempted, team total (players + team) |
| `three_pointers_made` | integer | Three-point field goals made, team total (players + team) |
| `three_pointers_attempted` | integer | Three-point field goals attempted, team total (players + team) |
| `assists` | integer | Assists, team total (players + team) |
| `off_rebounds` | integer | Offensive rebounds, team total (players + team) |
| `def_rebounds` | integer | Defensive rebounds, team total (players + team) |
| `total_rebounds` | integer | Total rebounds, team total (players + team) |
| `steals` | integer | Steals, team total (players + team) |
| `turnovers` | integer | Turnovers, team total (players + team) |
| `blocks` | integer | Shots blocked (by this player or team), team total (players + team) |
| `received_blocks` | integer | Own shots blocked by an opponent, team total (players + team) |
| `dunks` | integer | Dunks, team total (players + team) |
| `personal_fouls` | integer | Fouls committed, team total (players + team) |
| `fouls_drawn` | integer | Fouls received, team total (players + team) |
| `plus_minus` | integer | Point differential while on court (+/-), team total (players + team) |
| `rating` | integer | Valoracion: ACB's efficiency rating (PIR) = points + rebounds + assists + steals + blocks + fouls received - missed field goals - missed free throws - turnovers - shots blocked by an opponent - fouls committed, team total (players + team) |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Example {#acb_team_box-example}

```python
acb_team_box(match_id=105401)
```

_Last validated n/a._

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
| `match_id` | integer | ACB match id (one global sequence for every competition; legacy ids are below about 20000 up to 2019-20, ids from 2020-21 start around 100000). Taken from the request when the body does not name the game. |
| `quarter` | integer | Period of the bar: 0 = the whole game, 1-4 quarters. |
| `stat` | character | Compared statistic (free_throws, two_pointers, three_pointers, field_goals, rebounds, assists, steals, turnovers, personal_fouls, time_outs, points_off_turnover, paint_points, second_chance_points, fast_break_points, bench_points ...). |
| `home_value` | numeric | Home value of the bar: a percentage 0-100 for the shooting statistics (the API's '79,17%' decimal comma parsed), a count otherwise. |
| `away_value` | numeric | Away value of the bar, same units as home_value. |
| `home_made` | integer | Home shots made, from the 'made/attempted' subtitle; null for the non-shooting statistics. |
| `home_attempted` | integer | Home shots attempted, from the subtitle; null for the non-shooting statistics. |
| `away_made` | integer | Away shots made, from the subtitle; null for the non-shooting statistics. |
| `away_attempted` | integer | Away shots attempted, from the subtitle; null for the non-shooting statistics. |
| `home_is_highlighted` | logical | True when the site highlights the home bar as the better value. |
| `away_is_highlighted` | logical | True when the site highlights the away bar as the better value. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Example {#acb_team_comparison-example}

```python
acb_team_comparison(match_id=105401)
```

_Last validated n/a._

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
| `team_id` | integer | Season-team entry of the selected edition (new every season). |
| `club_id` | integer | Club id, stable across seasons and sponsor names (2 = Barça, 9 = Real Madrid, 13 = Valencia Basket): the `clubId` request param and every `homeClubId` / `awayClubId`. |
| `team` | character | Team name that season (sponsor names change) |
| `team_abbr` | character | Three-letter abbreviation (BAR, RMB) |
| `competition_id` | integer | Competition of the selected entry. |
| `edition_id` | integer | Season id of the selected entry (START year - 1935). |
| `season` | integer | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `latest_team_id` | integer | The club's most recent season-team entry. |
| `latest_team` | character | The club's most recent team name. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Example {#acb_teams-example}

```python
acb_teams(season=2027)
```

_Last validated n/a._

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
| `match_id` | integer | Match id. Legacy ids (seasons up to 2019-20) sit below about 20000 (12559 = 2005-06 final, 17718 = 2016-17 final); from 2020-21 they start at about 100000 (102920 in 2021-22, 105401 in 2026-27). One key space across competitions: the matchId param of every matchdata route. |
| `round_type` | character | Phase code: LR = Liga Regular (regular season), PO = playoffs (a bundle value, not in the samples); null on every row of /Menu/competition-data and /TeamProfile/next-matches |
| `round_id` | integer | Round id: global across seasons, not the round number (6017 = Liga 2026-27 round 3; 2600 = 2016-17 round 1) |
| `round_number` | integer | Round number within the phase |
| `group_id` | integer | Group id in group-stage competitions |
| `subphase_id` | integer | Sub-phase id of the round (playoff stages); null in the regular season. |
| `subphase_number` | integer | Sub-phase number of the round; null in the regular season. |
| `week_id` | integer | Week id: global across seasons (2989 = the week of 2026-10-10) |
| `status` | character | Match state (x-match-statuses) |
| `is_on_half_time` | logical | Not sent by this route (always null). |
| `current_quarter` | integer | Not sent by this route (always null). |
| `home_score` | integer | Home points (null before the match) |
| `away_score` | integer | Away points (null before the match) |
| `start` | character | Tip-off, ISO 8601 UTC |
| `home_wins` | integer | Not sent by this route (series records are in acb_matchlist). |
| `home_losses` | integer | Not sent by this route (series records are in acb_matchlist). |
| `away_wins` | integer | Not sent by this route (series records are in acb_matchlist). |
| `away_losses` | integer | Not sent by this route (series records are in acb_matchlist). |
| `home_team_id` | integer | Season-team id: one team's entry in one competition edition (4467 = Barça in Liga 2026-27, 4388 = Real Madrid in Liga 2025-26). Changes every season and competition. Joins every `homeTeamId` / `awayTeamId` / `teamId` / `teamEditionId`. Not the club id. |
| `home_club_id` | integer | Club id, stable across seasons and sponsor names (2 = Barça, 9 = Real Madrid, 13 = Valencia Basket): the `clubId` request param and every `homeClubId` / `awayClubId`. |
| `home_team` | character | Team name that season (sponsor names change) |
| `home_team_abbr` | character | Three-letter abbreviation (BAR, RMB) |
| `away_team_id` | integer | Season-team id: one team's entry in one competition edition (4467 = Barça in Liga 2026-27, 4388 = Real Madrid in Liga 2025-26). Changes every season and competition. Joins every `homeTeamId` / `awayTeamId` / `teamId` / `teamEditionId`. Not the club id. |
| `away_club_id` | integer | Club id, stable across seasons and sponsor names (2 = Barça, 9 = Real Madrid, 13 = Valencia Basket): the `clubId` request param and every `homeClubId` / `awayClubId`. |
| `away_team` | character | Team name that season (sponsor names change) |
| `away_team_abbr` | character | Three-letter abbreviation (BAR, RMB) |
| `competition_id` | integer | Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa (x-competitions) |
| `edition_id` | integer | Season id (`editionId`) = season START year - 1935 (91 = 2026-27, 81 = 2016-17, 70 = 2005-06, 48 = 1983-84, the first). The sdv season (END year) is editionId + 1936. Season lists also carry `seasonStartYear` / `seasonEndYear` |
| `season` | integer | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `is_selected_local` | logical | True when the requested club played at home. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Example {#acb_team_matches-example}

```python
acb_team_matches(club_id=2, season=2027)
```

_Last validated n/a._

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
| `club_id` | integer | Stable club id asked for. |
| `competition_id` | integer | Competition of the selected edition. |
| `edition_id` | integer | Season id (editionId) = season START year - 1935 (91 = 2026-27). The wrappers take the END year, `season`. |
| `season` | integer | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `scope` | character | all_time = club record holders (the API's leaders); edition = the selected season (seasonLeaders). The assignment is inferred from the values (689 games for J.C. Navarro is a career figure). |
| `metric` | character | Category in snake_case; avg_* are per-game averages, the rest totals. |
| `rank` | integer | Place in the category list, 1 = the leader, in the order the feed lists them. |
| `value` | numeric | The player's value in the category (a total or an average depending on the metric). |
| `player_id` | integer | ACB licence number: the person id everywhere. A player's box-score `player.id`, his play-by-play and shot `playerLicenseId` and the `playerLicenceId` / `playerLicense` request params hold the same number (all 24 players of match 105401 match). 20209407 = Edy Tavares. Older licences start 2020..., recent ones 3000.... |
| `player_name` | character | Display (sporting) name ('Edy Tavares') |
| `first_name` | character | Legal first name(s) |
| `last_name` | character | Legal last name(s) |
| `shirt_number` | character | Shirt number, a string ('' or null when unknown) |
| `game_role` | character | Position or staff role, in Spanish: Base (point guard), Escolta (shooting guard), Alero (small forward), Ala-pivot (power forward), Pivot (centre); Entrenador (head coach), Segundo Entrenador / Entrenador Ayudante (assistant coach). Null or '' in many rows |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Example {#acb_team_leaders-example}

```python
acb_team_leaders(club_id=2, number_of_players=3, season=2027)
```

_Last validated n/a._

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
| `club_id` | integer | Stable club id of the club asked for (2 = Barca). |
| `team_id` | integer | Season-team entry id of the selected edition. |
| `edition_id` | integer | Season id (editionId) = season START year - 1935 (91 = 2026-27). The wrappers take the END year, `season`. |
| `season` | integer | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `player_id` | integer | ACB licence number: the person id everywhere. A player's box-score `player.id`, his play-by-play and shot `playerLicenseId` and the `playerLicenceId` / `playerLicense` request params hold the same number (all 24 players of match 105401 match). 20209407 = Edy Tavares. Older licences start 2020..., recent ones 3000.... |
| `player_name` | character | Display (sporting) name ('Edy Tavares') |
| `first_name` | character | Legal first name(s) |
| `last_name` | character | Legal last name(s) |
| `shirt_number` | character | Shirt number, a string ('' or null when unknown) |
| `game_role` | character | Position or staff role, in Spanish: Base (point guard), Escolta (shooting guard), Alero (small forward), Ala-pivot (power forward), Pivot (centre); Entrenador (head coach), Segundo Entrenador / Entrenador Ayudante (assistant coach). Null or '' in many rows |
| `total_num_games` | integer | Season total of games played. |
| `total_time_played` | integer | Season total of time on court in seconds. |
| `total_starter` | integer | Season total of games started. |
| `total_points` | integer | Season total of points. |
| `total_max_points` | integer | Season total of most points in a single game. |
| `total_three_pointers_made` | integer | Season total of three-point field goals made. |
| `total_three_pointers_attempted` | integer | Season total of three-point field goals attempted. |
| `total_three_pointers_percentage` | numeric | Season total of three-point percentage (0-100). |
| `total_two_pointers_made` | integer | Season total of two-point field goals made. |
| `total_two_pointers_attempted` | integer | Season total of two-point field goals attempted. |
| `total_two_pointers_percentage` | numeric | Season total of two-point percentage (0-100). |
| `total_free_throws_made` | integer | Season total of free throws made. |
| `total_free_throws_attempted` | integer | Season total of free throws attempted. |
| `total_free_throws_percentage` | numeric | Season total of free-throw percentage (0-100). |
| `total_rebounds` | integer | Season total of total rebounds. |
| `total_offensive_rebounds` | integer | Season total of offensive rebounds. |
| `total_defensive_rebounds` | integer | Season total of defensive rebounds. |
| `total_assists` | integer | Season total of assists. |
| `total_steals` | integer | Season total of steals. |
| `total_turnovers` | integer | Season total of turnovers. |
| `total_blocks` | integer | Season total of blocks made. |
| `total_shots_rejected` | integer | Season total of own shots blocked. |
| `total_dunks` | integer | Season total of dunks. |
| `total_personal_fouls` | integer | Season total of personal fouls committed. |
| `total_fouls_received` | integer | Season total of fouls drawn. |
| `total_rating` | integer | Season total of ACB rating (valoracion). |
| `total_plus_minus` | integer | Season total of plus/minus. |
| `total_wins` | integer | Season total of team wins in the counted games (meaning unverified). |
| `total_losses` | integer | Season total of team losses in the counted games (meaning unverified). |
| `avg_num_games` | numeric | Per-game average of games played. |
| `avg_time_played` | numeric | Per-game average of time on court in seconds. |
| `avg_starter` | numeric | Per-game average of games started. |
| `avg_points` | numeric | Per-game average of points. |
| `avg_max_points` | numeric | Per-game average of most points in a single game. |
| `avg_three_pointers_made` | numeric | Per-game average of three-point field goals made. |
| `avg_three_pointers_attempted` | numeric | Per-game average of three-point field goals attempted. |
| `avg_three_pointers_percentage` | numeric | Per-game average of three-point percentage (0-100). |
| `avg_two_pointers_made` | numeric | Per-game average of two-point field goals made. |
| `avg_two_pointers_attempted` | numeric | Per-game average of two-point field goals attempted. |
| `avg_two_pointers_percentage` | numeric | Per-game average of two-point percentage (0-100). |
| `avg_free_throws_made` | numeric | Per-game average of free throws made. |
| `avg_free_throws_attempted` | numeric | Per-game average of free throws attempted. |
| `avg_free_throws_percentage` | numeric | Per-game average of free-throw percentage (0-100). |
| `avg_rebounds` | numeric | Per-game average of total rebounds. |
| `avg_offensive_rebounds` | numeric | Per-game average of offensive rebounds. |
| `avg_defensive_rebounds` | numeric | Per-game average of defensive rebounds. |
| `avg_assists` | numeric | Per-game average of assists. |
| `avg_steals` | numeric | Per-game average of steals. |
| `avg_turnovers` | numeric | Per-game average of turnovers. |
| `avg_blocks` | numeric | Per-game average of blocks made. |
| `avg_shots_rejected` | numeric | Per-game average of own shots blocked. |
| `avg_dunks` | numeric | Per-game average of dunks. |
| `avg_personal_fouls` | numeric | Per-game average of personal fouls committed. |
| `avg_fouls_received` | numeric | Per-game average of fouls drawn. |
| `avg_rating` | numeric | Per-game average of ACB rating (valoracion). |
| `avg_plus_minus` | numeric | Per-game average of plus/minus. |
| `avg_wins` | numeric | Per-game average of team wins in the counted games (meaning unverified). |
| `avg_losses` | numeric | Per-game average of team losses in the counted games (meaning unverified). |
| `per40_num_games` | numeric | Per-40-minute rate of games played. |
| `per40_time_played` | numeric | Always null: the per-40 block carries no time played. |
| `per40_starter` | numeric | Per-40-minute rate of games started. |
| `per40_points` | numeric | Per-40-minute rate of points. |
| `per40_max_points` | numeric | Per-40-minute rate of most points in a single game. |
| `per40_three_pointers_made` | numeric | Per-40-minute rate of three-point field goals made. |
| `per40_three_pointers_attempted` | numeric | Per-40-minute rate of three-point field goals attempted. |
| `per40_three_pointers_percentage` | numeric | Per-40-minute rate of three-point percentage (0-100). |
| `per40_two_pointers_made` | numeric | Per-40-minute rate of two-point field goals made. |
| `per40_two_pointers_attempted` | numeric | Per-40-minute rate of two-point field goals attempted. |
| `per40_two_pointers_percentage` | numeric | Per-40-minute rate of two-point percentage (0-100). |
| `per40_free_throws_made` | numeric | Per-40-minute rate of free throws made. |
| `per40_free_throws_attempted` | numeric | Per-40-minute rate of free throws attempted. |
| `per40_free_throws_percentage` | numeric | Per-40-minute rate of free-throw percentage (0-100). |
| `per40_rebounds` | numeric | Per-40-minute rate of total rebounds. |
| `per40_offensive_rebounds` | numeric | Per-40-minute rate of offensive rebounds. |
| `per40_defensive_rebounds` | numeric | Per-40-minute rate of defensive rebounds. |
| `per40_assists` | numeric | Per-40-minute rate of assists. |
| `per40_steals` | numeric | Per-40-minute rate of steals. |
| `per40_turnovers` | numeric | Per-40-minute rate of turnovers. |
| `per40_blocks` | numeric | Per-40-minute rate of blocks made. |
| `per40_shots_rejected` | numeric | Per-40-minute rate of own shots blocked. |
| `per40_dunks` | numeric | Per-40-minute rate of dunks. |
| `per40_personal_fouls` | numeric | Per-40-minute rate of personal fouls committed. |
| `per40_fouls_received` | numeric | Per-40-minute rate of fouls drawn. |
| `per40_rating` | numeric | Per-40-minute rate of ACB rating (valoracion). |
| `per40_plus_minus` | numeric | Per-40-minute rate of plus/minus. |
| `per40_wins` | numeric | Per-40-minute rate of team wins in the counted games (meaning unverified). |
| `per40_losses` | numeric | Per-40-minute rate of team losses in the counted games (meaning unverified). |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Example {#acb_team_stats-example}

```python
acb_team_stats(club_id=2, season=2027)
```

_Last validated n/a._

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
| `club_id` | integer | Stable club id of the club asked for (2 = Barca). |
| `team_id` | integer | Season-team entry id (the API's teamEditionId). |
| `edition_id` | integer | Season id (editionId) = season START year - 1935 (91 = 2026-27). The wrappers take the END year, `season`. |
| `season` | integer | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `total_num_games` | integer | Season total of games played. |
| `total_time_played` | integer | Season total of time on court in seconds. |
| `total_starter` | integer | Season total of games started. |
| `total_points` | integer | Season total of points. |
| `total_max_points` | integer | Season total of most points in a single game. |
| `total_three_pointers_made` | integer | Season total of three-point field goals made. |
| `total_three_pointers_attempted` | integer | Season total of three-point field goals attempted. |
| `total_three_pointers_percentage` | numeric | Season total of three-point percentage (0-100). |
| `total_two_pointers_made` | integer | Season total of two-point field goals made. |
| `total_two_pointers_attempted` | integer | Season total of two-point field goals attempted. |
| `total_two_pointers_percentage` | numeric | Season total of two-point percentage (0-100). |
| `total_free_throws_made` | integer | Season total of free throws made. |
| `total_free_throws_attempted` | integer | Season total of free throws attempted. |
| `total_free_throws_percentage` | numeric | Season total of free-throw percentage (0-100). |
| `total_rebounds` | integer | Season total of total rebounds. |
| `total_offensive_rebounds` | integer | Season total of offensive rebounds. |
| `total_defensive_rebounds` | integer | Season total of defensive rebounds. |
| `total_assists` | integer | Season total of assists. |
| `total_steals` | integer | Season total of steals. |
| `total_turnovers` | integer | Season total of turnovers. |
| `total_blocks` | integer | Season total of blocks made. |
| `total_shots_rejected` | integer | Season total of own shots blocked. |
| `total_dunks` | integer | Season total of dunks. |
| `total_personal_fouls` | integer | Season total of personal fouls committed. |
| `total_fouls_received` | integer | Season total of fouls drawn. |
| `total_rating` | integer | Season total of ACB rating (valoracion). |
| `total_plus_minus` | integer | Season total of plus/minus. |
| `total_wins` | integer | Season total of team wins in the counted games (meaning unverified). |
| `total_losses` | integer | Season total of team losses in the counted games (meaning unverified). |
| `avg_num_games` | numeric | Per-game average of games played. |
| `avg_time_played` | numeric | Per-game average of time on court in seconds. |
| `avg_starter` | numeric | Per-game average of games started. |
| `avg_points` | numeric | Per-game average of points. |
| `avg_max_points` | numeric | Per-game average of most points in a single game. |
| `avg_three_pointers_made` | numeric | Per-game average of three-point field goals made. |
| `avg_three_pointers_attempted` | numeric | Per-game average of three-point field goals attempted. |
| `avg_three_pointers_percentage` | numeric | Per-game average of three-point percentage (0-100). |
| `avg_two_pointers_made` | numeric | Per-game average of two-point field goals made. |
| `avg_two_pointers_attempted` | numeric | Per-game average of two-point field goals attempted. |
| `avg_two_pointers_percentage` | numeric | Per-game average of two-point percentage (0-100). |
| `avg_free_throws_made` | numeric | Per-game average of free throws made. |
| `avg_free_throws_attempted` | numeric | Per-game average of free throws attempted. |
| `avg_free_throws_percentage` | numeric | Per-game average of free-throw percentage (0-100). |
| `avg_rebounds` | numeric | Per-game average of total rebounds. |
| `avg_offensive_rebounds` | numeric | Per-game average of offensive rebounds. |
| `avg_defensive_rebounds` | numeric | Per-game average of defensive rebounds. |
| `avg_assists` | numeric | Per-game average of assists. |
| `avg_steals` | numeric | Per-game average of steals. |
| `avg_turnovers` | numeric | Per-game average of turnovers. |
| `avg_blocks` | numeric | Per-game average of blocks made. |
| `avg_shots_rejected` | numeric | Per-game average of own shots blocked. |
| `avg_dunks` | numeric | Per-game average of dunks. |
| `avg_personal_fouls` | numeric | Per-game average of personal fouls committed. |
| `avg_fouls_received` | numeric | Per-game average of fouls drawn. |
| `avg_rating` | numeric | Per-game average of ACB rating (valoracion). |
| `avg_plus_minus` | numeric | Per-game average of plus/minus. |
| `avg_wins` | numeric | Per-game average of team wins in the counted games (meaning unverified). |
| `avg_losses` | numeric | Per-game average of team losses in the counted games (meaning unverified). |
| `per40_num_games` | numeric | Per-40-minute rate of games played. |
| `per40_time_played` | numeric | Always null: the per-40 block carries no time played. |
| `per40_starter` | numeric | Per-40-minute rate of games started. |
| `per40_points` | numeric | Per-40-minute rate of points. |
| `per40_max_points` | numeric | Per-40-minute rate of most points in a single game. |
| `per40_three_pointers_made` | numeric | Per-40-minute rate of three-point field goals made. |
| `per40_three_pointers_attempted` | numeric | Per-40-minute rate of three-point field goals attempted. |
| `per40_three_pointers_percentage` | numeric | Per-40-minute rate of three-point percentage (0-100). |
| `per40_two_pointers_made` | numeric | Per-40-minute rate of two-point field goals made. |
| `per40_two_pointers_attempted` | numeric | Per-40-minute rate of two-point field goals attempted. |
| `per40_two_pointers_percentage` | numeric | Per-40-minute rate of two-point percentage (0-100). |
| `per40_free_throws_made` | numeric | Per-40-minute rate of free throws made. |
| `per40_free_throws_attempted` | numeric | Per-40-minute rate of free throws attempted. |
| `per40_free_throws_percentage` | numeric | Per-40-minute rate of free-throw percentage (0-100). |
| `per40_rebounds` | numeric | Per-40-minute rate of total rebounds. |
| `per40_offensive_rebounds` | numeric | Per-40-minute rate of offensive rebounds. |
| `per40_defensive_rebounds` | numeric | Per-40-minute rate of defensive rebounds. |
| `per40_assists` | numeric | Per-40-minute rate of assists. |
| `per40_steals` | numeric | Per-40-minute rate of steals. |
| `per40_turnovers` | numeric | Per-40-minute rate of turnovers. |
| `per40_blocks` | numeric | Per-40-minute rate of blocks made. |
| `per40_shots_rejected` | numeric | Per-40-minute rate of own shots blocked. |
| `per40_dunks` | numeric | Per-40-minute rate of dunks. |
| `per40_personal_fouls` | numeric | Per-40-minute rate of personal fouls committed. |
| `per40_fouls_received` | numeric | Per-40-minute rate of fouls drawn. |
| `per40_rating` | numeric | Per-40-minute rate of ACB rating (valoracion). |
| `per40_plus_minus` | numeric | Per-40-minute rate of plus/minus. |
| `per40_wins` | numeric | Per-40-minute rate of team wins in the counted games (meaning unverified). |
| `per40_losses` | numeric | Per-40-minute rate of team losses in the counted games (meaning unverified). |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Example {#acb_team_totals-example}

```python
acb_team_totals(club_id=2, season=2027)
```

_Last validated n/a._

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
| `club_id` | integer | Stable club id asked for. |
| `stat` | character | Record category in snake_case (rating, points, rebounds, assists, steals, ...). |
| `value` | numeric | The record value (a count; Float64 so every category fits). |
| `game_id` | integer | Match id of the record game (legacy sequence for old games). |
| `game_date` | character | Date of the record game (UTC midnight). |
| `player_id` | integer | ACB licence number: the person id everywhere. A player's box-score `player.id`, his play-by-play and shot `playerLicenseId` and the `playerLicenceId` / `playerLicense` request params hold the same number (all 24 players of match 105401 match). 20209407 = Edy Tavares. Older licences start 2020..., recent ones 3000.... |
| `player_name` | character | Display (sporting) name ('Edy Tavares') |
| `first_name` | character | Legal first name(s) |
| `last_name` | character | Legal last name(s) |
| `shirt_number` | character | Shirt number, a string ('' or null when unknown) |
| `game_role` | character | Position or staff role, in Spanish: Base (point guard), Escolta (shooting guard), Alero (small forward), Ala-pivot (power forward), Pivot (centre); Entrenador (head coach), Segundo Entrenador / Entrenador Ayudante (assistant coach). Null or '' in many rows |
| `local_team_id` | integer | Home season-team entry of the record game. |
| `local_club_id` | integer | Home stable club id of the record game. |
| `local_team` | character | Home team name in the record game. |
| `local_team_abbr` | character | Home team abbreviation in the record game. |
| `visitor_team_id` | integer | Away season-team entry of the record game. |
| `visitor_club_id` | integer | Away stable club id of the record game. |
| `visitor_team` | character | Away team name in the record game. |
| `visitor_team_abbr` | character | Away team abbreviation in the record game. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Example {#acb_team_records-example}

```python
acb_team_records(club_id=2)
```

_Last validated n/a._
