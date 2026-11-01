# FIBA (digital API and game pages) — FIBA digital API (GDAP, digital-api.fiba.basketball) — Team

> FIBA (digital API and game pages) — FIBA digital API (GDAP, digital-api.fiba.basketball) — Team — function reference in sdv-py, the SportsDataverse Python package.

## fiba_gdap_team_games

A team's games with each team's leaders.

**Endpoint URL:** `GET https://digital-api.fiba.basketball/hapi/getgdapgameswithleaderdetailsbyteamid`

**Valid URL:** [https://digital-api.fiba.basketball/hapi/getgdapgameswithleaderdetailsbyteamid?gdapTeamId=279569](https://digital-api.fiba.basketball/hapi/getgdapgameswithleaderdetailsbyteamid?gdapTeamId=279569)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gdapTeamId` | `team_id` |  | `Y` |  | Required. Competition team id: one per team per competition (teamA.teamId of a game row; Germany is 279569 at the 2023 World Cup and 281545 at Paris 2024). Not the federation organisationId |

### Returns {#fiba_gdap_team_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | FIBA game id, global across competitions (GDAP key space): gdapGameId, the <gameId> of a game-page URL. |
| `round_name` | String | Round label (Group Phase, Quarter-Finals, Final...). |
| `game_number` | String | Game number within the round or pairing (a string: '1', 'A'). |
| `status` | String | Game status label (Validated). |
| `status_code` | String | Game status code: VALID (validated result), CLOS (closed, pre-2000 rows). |
| `game_day` | Int64 | Game day number within the round. |
| `team_a_from` | String | How team A qualified for this slot; null in group play. |
| `team_b_from` | String | How team B qualified for this slot; null in group play. |
| `team_a_team_id` | Int64 | Competition team id: one per team per competition (Germany = 279569 at the 2023 World Cup, 281545 at Paris 2024). Joins getgdapcompetitionteamrosterbyteamid / getgdapcompetitionteamstatisticsbyteamid gdapTeamId; not an organisationId. |
| `team_a_organisation_id` | Int64 | Organisation id, stable across competitions and seasons: the national federation for a national team (64 = Germany), the club for a club team (396 = Filou Oostende). Same key space as nationalFederationId, clubId, play-by-play oId and the game page's T_<id>. |
| `team_a_code` | String | Team code (national: FIBA country code; club: a club abbreviation such as OOST). |
| `team_a_official_name` | String | Official name of team A: the country for a national team, the club for a club team. |
| `team_a_short_name` | String | Short name of team A: the country code for a national team, the club's short name for a club. |
| `team_b_team_id` | Int64 | Competition team id: one per team per competition (Germany = 279569 at the 2023 World Cup, 281545 at Paris 2024). Joins getgdapcompetitionteamrosterbyteamid / getgdapcompetitionteamstatisticsbyteamid gdapTeamId; not an organisationId. |
| `team_b_organisation_id` | Int64 | Organisation id, stable across competitions and seasons: the national federation for a national team (64 = Germany), the club for a club team (396 = Filou Oostende). Same key space as nationalFederationId, clubId, play-by-play oId and the game page's T_<id>. |
| `team_b_code` | String | Team code (national: FIBA country code; club: a club abbreviation such as NANT). |
| `team_b_official_name` | String | Official name of team B: the country for a national team, the club for a club team. |
| `team_b_short_name` | String | Short name of team B: the country code for a national team, the club's short name for a club. |
| `team_a_score` | Int64 | Team A points in the period. |
| `team_b_score` | Int64 | Team B points in the period. |
| `host_city` | String | City of the venue. |
| `host_country` | String | Country of the venue (name; a FIBA code in getgdapgameswithleaderdetailsbyteamid). |
| `host_country_code` | String | FIBA code of the venue's country; always null in the samples, values unverified. |
| `venue_id` | Int64 | Venue id (GDAP key space; 0 = unknown). |
| `venue_name` | String | Name of the arena the game is played in. |
| `game_date_time` | String | Tip-off in venue-local wall-clock time, ISO 8601 without offset (T00:00:00 when hasTimeGameDateTime is false). |
| `game_date_time_utc` | String | Tip-off in UTC, ISO 8601 without the Z. |
| `has_time_game_date_time` | Boolean | False when only the date is known (the time part is then 00:00:00). |
| `iana_time_zone` | String | IANA time zone of the venue. |
| `utc_offset` | Float64 | Venue UTC offset at tip-off, in hours (Float64: a half-hour zone such as India +5.5 or Adelaide +9.5 is a fraction; whole hours in every sample). |
| `buy_tickets_url` | String | Ticket link; always '' in the samples. |
| `watch_url` | String | Stream link; always '' in the samples. |
| `is_postponed` | Boolean | True when the game was postponed. |
| `is_played_behind_closed_doors` | Boolean | True when played without spectators. |
| `venue_capacity` | Int64 | Venue capacity (0 = unknown). |
| `spectators` | Int64 | Attendance at the game, as recorded by FIBA. |
| `is_live` | Boolean | True while the game is being played. |
| `leaders_team_a_points_leader_player_id` | Int64 | FIBA person id, global across competitions and roles (player, coach, official): roster personId = play-by-play pId = game-page P_<id> = gdapPlayerId. |
| `leaders_team_a_points_leader_first_name` | String | Given name(s) as registered with FIBA. |
| `leaders_team_a_points_leader_last_name` | String | Last name (officials: upper case). |
| `leaders_team_a_points_leader_statistics_value` | Int64 | Leader's value for the statistic. |
| `leaders_team_a_points_leader_nationality` | String | FIBA code of the main nationality. |
| `leaders_team_a_points_leader_rank` | Int64 | Rank (1 = first). |
| `leaders_team_a_rebounds_leader_player_id` | Int64 | FIBA person id, global across competitions and roles (player, coach, official): roster personId = play-by-play pId = game-page P_<id> = gdapPlayerId. |
| `leaders_team_a_rebounds_leader_first_name` | String | Given name(s) as registered with FIBA. |
| `leaders_team_a_rebounds_leader_last_name` | String | Last name (officials: upper case). |
| `leaders_team_a_rebounds_leader_statistics_value` | Int64 | Leader's value for the statistic. |
| `leaders_team_a_rebounds_leader_nationality` | String | FIBA code of the main nationality. |
| `leaders_team_a_rebounds_leader_rank` | Int64 | Rank (1 = first). |
| `leaders_team_a_assists_leader_player_id` | Int64 | FIBA person id, global across competitions and roles (player, coach, official): roster personId = play-by-play pId = game-page P_<id> = gdapPlayerId. |
| `leaders_team_a_assists_leader_first_name` | String | Given name(s) as registered with FIBA. |
| `leaders_team_a_assists_leader_last_name` | String | Last name (officials: upper case). |
| `leaders_team_a_assists_leader_statistics_value` | Int64 | Leader's value for the statistic. |
| `leaders_team_a_assists_leader_nationality` | String | FIBA code of the main nationality. |
| `leaders_team_a_assists_leader_rank` | Int64 | Rank (1 = first). |
| `leaders_team_b_points_leader_player_id` | Int64 | FIBA person id, global across competitions and roles (player, coach, official): roster personId = play-by-play pId = game-page P_<id> = gdapPlayerId. |
| `leaders_team_b_points_leader_first_name` | String | Given name(s) as registered with FIBA. |
| `leaders_team_b_points_leader_last_name` | String | Last name (officials: upper case). |
| `leaders_team_b_points_leader_statistics_value` | Int64 | Leader's value for the statistic. |
| `leaders_team_b_points_leader_nationality` | String | FIBA code of the main nationality. |
| `leaders_team_b_points_leader_rank` | Int64 | Rank (1 = first). |
| `leaders_team_b_rebounds_leader_player_id` | Int64 | FIBA person id, global across competitions and roles (player, coach, official): roster personId = play-by-play pId = game-page P_<id> = gdapPlayerId. |
| `leaders_team_b_rebounds_leader_first_name` | String | Given name(s) as registered with FIBA. |
| `leaders_team_b_rebounds_leader_last_name` | String | Last name (officials: upper case). |
| `leaders_team_b_rebounds_leader_statistics_value` | Int64 | Leader's value for the statistic. |
| `leaders_team_b_rebounds_leader_nationality` | String | FIBA code of the main nationality. |
| `leaders_team_b_rebounds_leader_rank` | Int64 | Rank (1 = first). |
| `leaders_team_b_assists_leader_player_id` | Int64 | FIBA person id, global across competitions and roles (player, coach, official): roster personId = play-by-play pId = game-page P_<id> = gdapPlayerId. |
| `leaders_team_b_assists_leader_first_name` | String | Given name(s) as registered with FIBA. |
| `leaders_team_b_assists_leader_last_name` | String | Last name (officials: upper case). |
| `leaders_team_b_assists_leader_statistics_value` | Int64 | Leader's value for the statistic. |
| `leaders_team_b_assists_leader_nationality` | String | FIBA code of the main nationality. |
| `leaders_team_b_assists_leader_rank` | Int64 | Rank (1 = first). |
| `periods` | String | Score by period. Stored as a JSON string. |
| `game_statistic_status_code` | String | Box-score validation status: VALID. |
| `game_play_by_play_status_code` | String | Play-by-play availability flag: VALID or EMPTY. UNRELIABLE: World Cup 2014 and EuroBasket 2017 say EMPTY but their game pages carry full play-by-play; always check the page. |
| `game_result_status_code` | String | Result validation status: VALID. |
| `game_result_additional_information` | String | Free-text result note (forfeit, overtime remark); null when none. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fiba_gdap_team_games-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FIBA has nothing for the request (HTTP 404, or GDAP answered {} or [] for an unknown id). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. HTTP 400 / 422, or a malformed id, season or date. |

### Example {#fiba_gdap_team_games-example}

```python
fiba_gdap_team_games(team_id=279569)
```

## fiba_gdap_team_player_stats

Per-game averages of every player of a team at a competition.

**Endpoint URL:** `GET https://digital-api.fiba.basketball/hapi/getgdapcompetitionteamstatisticsbyteamid`

**Valid URL:** [https://digital-api.fiba.basketball/hapi/getgdapcompetitionteamstatisticsbyteamid?gdapTeamId=279569](https://digital-api.fiba.basketball/hapi/getgdapcompetitionteamstatisticsbyteamid?gdapTeamId=279569)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gdapTeamId` | `team_id` |  | `Y` |  | Required. Competition team id: one per team per competition (teamA.teamId of a game row; Germany is 279569 at the 2023 World Cup and 281545 at Paris 2024). Not the federation organisationId |

### Returns {#fiba_gdap_team_player_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `competition_id` | Int64 | FIBA competition id (Int64) the averages belong to. |
| `player_id` | Int64 | FIBA person id, global across competitions and roles (player, coach, official): roster personId = play-by-play pId = game-page P_<id> = gdapPlayerId. |
| `first_name` | String | Given name(s) as registered with FIBA. |
| `last_name` | String | Last name (officials: upper case). |
| `uniform_number` | String | Jersey number (a string). |
| `team_id` | Int64 | Competition team id: one per team per competition (Germany = 279569 at the 2023 World Cup, 281545 at Paris 2024). Joins getgdapcompetitionteamrosterbyteamid / getgdapcompetitionteamstatisticsbyteamid gdapTeamId; not an organisationId. |
| `points_per_game` | Float64 | The player's average points per game played. |
| `rebounds_per_game` | Float64 | The player's average rebounds per game played. |
| `rebounds_offensive_per_game` | Float64 | The player's average offensive rebounds per game played. |
| `rebounds_defensive_per_game` | Float64 | The player's average defensive rebounds per game played. |
| `assists_per_game` | Float64 | The player's average assists per game played. |
| `blocks_per_game` | Float64 | The player's average blocked shots per game played. |
| `turnovers_per_game` | Float64 | The player's average turnovers per game played. |
| `fouls_per_game` | Float64 | The player's average personal fouls per game played. |
| `plus_minus_per_game` | Float64 | The player's average plus/minus (point differential while on court) per game played. |
| `efficiency_per_game` | Float64 | The player's average efficiency (FIBA EFF) per game played. |
| `play_time_in_seconds_per_game` | Float64 | The player's average playing time in seconds per game played. |
| `field_goals_made_per_game` | Float64 | The player's average field goals made per game played. |
| `field_goals_attempted_per_game` | Float64 | The player's average field goals attempted per game played. |
| `two_points_made_per_game` | Float64 | The player's average two-point field goals made per game played. |
| `two_points_attempted_per_game` | Float64 | The player's average two-point field goals attempted per game played. |
| `three_points_made_per_game` | Float64 | The player's average three-point field goals made per game played. |
| `three_points_attempted_per_game` | Float64 | The player's average three-point field goals attempted per game played. |
| `free_throws_made_per_game` | Float64 | The player's average free throws made per game played. |
| `free_throws_attempted_per_game` | Float64 | The player's average free throws attempted per game played. |
| `points_from_turnovers_per_game` | Float64 | The player's average points from turnovers per game played. |
| `fast_break_points_per_game` | Float64 | The player's average fast-break points per game played. |
| `second_chance_points_per_game` | Float64 | The player's average second-chance points per game played. |
| `points_in_the_paint_per_game` | Float64 | The player's average points in the paint per game played. |
| `steals_per_game` | Float64 | The player's average steals per game played. |
| `total_games_played` | Int64 | The player's total games played over the games played. |
| `total_games_won` | Int64 | The player's total games won over the games played. |
| `total_games_lost` | Int64 | The player's total games lost over the games played. |
| `total_rebounds` | Int64 | The player's total rebounds over the games played. |
| `total_rebounds_offensive` | Int64 | The player's total offensive rebounds over the games played. |
| `total_rebounds_defensive` | Int64 | The player's total defensive rebounds over the games played. |
| `total_blocks` | Int64 | The player's total blocked shots over the games played. |
| `total_turnovers` | Int64 | The player's total turnovers over the games played. |
| `total_fouls` | Int64 | The player's total personal fouls over the games played. |
| `total_assists` | Int64 | The player's total assists over the games played. |
| `total_plus_minus` | Int64 | The player's total plus/minus (point differential while on court) over the games played. |
| `total_efficiency` | Int64 | The player's total efficiency (FIBA EFF) over the games played. |
| `total_play_time_in_seconds` | Int64 | The player's total playing time in seconds over the games played. |
| `total_points` | Int64 | The player's total points over the games played. |
| `total_field_goals_made` | Int64 | The player's total field goals made over the games played. |
| `total_field_goals_attempted` | Int64 | The player's total field goals attempted over the games played. |
| `field_goals_percentage` | Float64 | Field-goal percentage, 0-100 (over the competition; full float precision in averages). |
| `total_two_points_made` | Int64 | The player's total two-point field goals made over the games played. |
| `total_two_points_attempted` | Int64 | The player's total two-point field goals attempted over the games played. |
| `two_points_percentage` | Float64 | Two-point field-goal percentage, 0-100 (over the competition; full float precision in averages). |
| `total_three_points_made` | Int64 | The player's total three-point field goals made over the games played. |
| `total_three_points_attempted` | Int64 | The player's total three-point field goals attempted over the games played. |
| `three_points_percentage` | Float64 | Three-point field-goal percentage, 0-100 (over the competition; full float precision in averages). |
| `total_free_throws_made` | Int64 | The player's total free throws made over the games played. |
| `total_free_throws_attempted` | Int64 | The player's total free throws attempted over the games played. |
| `free_throws_percentage` | Float64 | Free-throw percentage, 0-100 (over the competition; full float precision in averages). |
| `total_double_doubles` | Int64 | The player's total double-doubles over the games played. |
| `total_triple_doubles` | Int64 | The player's total triple-doubles over the games played. |
| `total_quadruple_doubles` | Int64 | The player's total quadruple-doubles over the games played. |
| `total_quintuple_doubles` | Int64 | The player's total quintuple-doubles over the games played. |
| `total_points_from_turnovers` | Int64 | The player's total points from turnovers over the games played. |
| `total_fast_break_points` | Int64 | The player's total fast-break points over the games played. |
| `total_second_chance_points` | Int64 | The player's total second-chance points over the games played. |
| `total_points_in_the_paint` | Int64 | The player's total points in the paint over the games played. |
| `total_steals` | Int64 | The player's total steals over the games played. |
| `rank` | Int64 | Rank (1 = first). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fiba_gdap_team_player_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FIBA has nothing for the request (HTTP 404, or GDAP answered {} or [] for an unknown id). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. HTTP 400 / 422, or a malformed id, season or date. |

### Example {#fiba_gdap_team_player_stats-example}

```python
fiba_gdap_team_player_stats(team_id=279569)
```

## fiba_gdap_team_staff

Coaches of every team of a competition.

**Endpoint URL:** `GET https://digital-api.fiba.basketball/hapi/getgdapteamstaffbycompetitionid`

**Valid URL:** [https://digital-api.fiba.basketball/hapi/getgdapteamstaffbycompetitionid?gdapCompetitionId=208182](https://digital-api.fiba.basketball/hapi/getgdapteamstaffbycompetitionid?gdapCompetitionId=208182)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gdapCompetitionId` | `comp` |  | `Y` |  | Required. FIBA competition id (GDAP key space), one per edition: competition.competitionId of any game row (208182 = World Cup 2023, 208722 = Paris 2024 men, 208850 = BCL 2024-25). Find it with fiba_gdap_editions or fiba_gdap_games_between. |

### Returns {#fiba_gdap_team_staff-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | FIBA team id (Int64), one per team per competition; the team the coach is registered with. |
| `team_name` | String | Team name the coach is registered with. |
| `team_code` | String | Team code (GER, USA...) the coach is registered with. |
| `person_id` | Int64 | FIBA person id, global across competitions and roles (player, coach, official): roster personId = play-by-play pId = game-page P_<id> = gdapPlayerId. |
| `first_name` | String | Given name(s) as registered with FIBA. |
| `last_name` | String | Last name (officials: upper case). |
| `gender` | String | Sex of the person: M or F (Male/Female on getgdapcompetitionnominationsbyid). |
| `function` | String | Role: Head Coach, Coach, Assistant Coach. |
| `is_head_coach` | Int64 | 1 for the head coach, 0 otherwise (an integer flag, not a boolean). |
| `is_assistant_coach` | Int64 | 1 for an assistant coach, else null (integer flag). |
| `is_accompanying_coach` | Boolean | Accompanying-coach flag; always null in the samples, values unverified. |
| `coach_display_order` | Int64 | Display order; null when unset. |
| `is_on_final_roster` | Boolean | True when on the final (12-player) roster. |
| `nationalities` | String | Nationalities (FIBA codes on player info; country objects on rosters and officials). Stored as a JSON string. |
| `coach_license_organisation_name` | String | Federation that issued the coaching licence. |
| `coach_license_country_code` | String | FIBA code of the licensing federation's country. |
| `final_roster_member_status_code` | String | Roster entry status; always null in the samples, values unverified. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fiba_gdap_team_staff-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FIBA has nothing for the request (HTTP 404, or GDAP answered {} or [] for an unknown id). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. HTTP 400 / 422, or a malformed id, season or date. |

### Example {#fiba_gdap_team_staff-example}

```python
fiba_gdap_team_staff(comp=208182)
```

## fiba_gdap_team_stats

A team's per-game averages and totals at a competition, with the bench totals.

**Endpoint URL:** `GET https://digital-api.fiba.basketball/hapi/getgdapcompetitionteamstatisticsbyteamid`

**Valid URL:** [https://digital-api.fiba.basketball/hapi/getgdapcompetitionteamstatisticsbyteamid?gdapTeamId=279569](https://digital-api.fiba.basketball/hapi/getgdapcompetitionteamstatisticsbyteamid?gdapTeamId=279569)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gdapTeamId` | `team_id` |  | `Y` |  | Required. Competition team id: one per team per competition (teamA.teamId of a game row; Germany is 279569 at the 2023 World Cup and 281545 at Paris 2024). Not the federation organisationId |

### Returns {#fiba_gdap_team_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | Competition team id: one per team per competition (Germany = 279569 at the 2023 World Cup, 281545 at Paris 2024). Joins getgdapcompetitionteamrosterbyteamid / getgdapcompetitionteamstatisticsbyteamid gdapTeamId; not an organisationId. |
| `competition_id` | Int64 | FIBA competition id (GDAP key space): one per edition (2023 World Cup = 208182, BCL 2024-25 = 208850); gdapCompetitionId. |
| `points_per_game` | Float64 | The team's average points per game played. |
| `rebounds_per_game` | Float64 | The bench players' average rebounds per game played. |
| `rebounds_offensive_per_game` | Float64 | The bench players' average offensive rebounds per game played. |
| `rebounds_defensive_per_game` | Float64 | The bench players' average defensive rebounds per game played. |
| `assists_per_game` | Float64 | The team's average assists per game played. |
| `blocks_per_game` | Float64 | The team's average blocked shots per game played. |
| `turnovers_per_game` | Float64 | The bench players' average turnovers per game played. |
| `fouls_per_game` | Float64 | The bench players' average personal fouls per game played. |
| `plus_minus_per_game` | Float64 | The team's average plus/minus (point differential while on court) per game played. |
| `efficiency_per_game` | Float64 | The team's average efficiency (FIBA EFF) per game played. |
| `play_time_in_seconds_per_game` | Float64 | The team's average playing time in seconds per game played. |
| `field_goals_made_per_game` | Float64 | The team's average field goals made per game played. |
| `field_goals_attempted_per_game` | Float64 | The team's average field goals attempted per game played. |
| `two_points_made_per_game` | Float64 | The team's average two-point field goals made per game played. |
| `two_points_attempted_per_game` | Float64 | The team's average two-point field goals attempted per game played. |
| `three_points_made_per_game` | Float64 | The team's average three-point field goals made per game played. |
| `three_points_attempted_per_game` | Float64 | The team's average three-point field goals attempted per game played. |
| `free_throws_made_per_game` | Float64 | The team's average free throws made per game played. |
| `free_throws_attempted_per_game` | Float64 | The team's average free throws attempted per game played. |
| `points_from_turnovers_per_game` | Float64 | The team's average points from turnovers per game played. |
| `fast_break_points_per_game` | Float64 | The team's average fast-break points per game played. |
| `second_chance_points_per_game` | Float64 | The team's average second-chance points per game played. |
| `points_in_the_paint_per_game` | Float64 | The team's average points in the paint per game played. |
| `from_bench_points_per_game` | Float64 | The team's average points by bench players per game played. |
| `steals_per_game` | Float64 | The team's average steals per game played. |
| `total_games_played` | Int64 | The bench players' total games played over the games played. |
| `total_games_won` | Int64 | The team's total games won over the games played. |
| `total_games_lost` | Int64 | The team's total games lost over the games played. |
| `total_rebounds` | Int64 | The bench players' total rebounds over the games played. |
| `total_rebounds_offensive` | Int64 | The bench players' total offensive rebounds over the games played. |
| `total_rebounds_defensive` | Int64 | The bench players' total defensive rebounds over the games played. |
| `total_blocks` | Int64 | The team's total blocked shots over the games played. |
| `total_turnovers` | Int64 | The bench players' total turnovers over the games played. |
| `total_fouls` | Int64 | The bench players' total personal fouls over the games played. |
| `total_assists` | Int64 | The team's total assists over the games played. |
| `total_plus_minus` | Int64 | The team's total plus/minus (point differential while on court) over the games played. |
| `total_efficiency` | Int64 | The team's total efficiency (FIBA EFF) over the games played. |
| `total_play_time_in_seconds` | Int64 | The team's total playing time in seconds over the games played. |
| `total_points` | Int64 | The team's total points over the games played. |
| `total_field_goals_made` | Int64 | The team's total field goals made over the games played. |
| `total_field_goals_attempted` | Int64 | The team's total field goals attempted over the games played. |
| `field_goals_percentage` | Float64 | Field-goal percentage, 0-100 (over the competition; full float precision in averages). |
| `total_two_points_made` | Int64 | The team's total two-point field goals made over the games played. |
| `total_two_points_attempted` | Int64 | The team's total two-point field goals attempted over the games played. |
| `two_points_percentage` | Float64 | Two-point field-goal percentage, 0-100 (over the competition; full float precision in averages). |
| `total_three_points_made` | Int64 | The team's total three-point field goals made over the games played. |
| `total_three_points_attempted` | Int64 | The team's total three-point field goals attempted over the games played. |
| `three_points_percentage` | Float64 | Three-point field-goal percentage, 0-100 (over the competition; full float precision in averages). |
| `total_free_throws_made` | Int64 | The team's total free throws made over the games played. |
| `total_free_throws_attempted` | Int64 | The team's total free throws attempted over the games played. |
| `free_throws_percentage` | Float64 | Free-throw percentage, 0-100 (over the competition; full float precision in averages). |
| `total_double_doubles` | Int64 | The team's total double-doubles over the games played. |
| `total_triple_doubles` | Int64 | The team's total triple-doubles over the games played. |
| `total_quadruple_doubles` | Int64 | The team's total quadruple-doubles over the games played. |
| `total_quintuple_doubles` | Int64 | The team's total quintuple-doubles over the games played. |
| `total_points_from_turnovers` | Int64 | The team's total points from turnovers over the games played. |
| `total_fast_break_points` | Int64 | The team's total fast-break points over the games played. |
| `total_second_chance_points` | Int64 | The team's total second-chance points over the games played. |
| `total_points_in_the_paint` | Int64 | The team's total points in the paint over the games played. |
| `total_from_bench_points` | Int64 | The team's total points by bench players over the games played. |
| `total_steals` | Int64 | The team's total steals over the games played. |
| `biggest_lead` | Int64 | Team's biggest lead in any game of the competition, points. |
| `bench_total_games_played` | Int64 | The bench players' total games played over the games played. |
| `bench_rebounds_per_game` | Float64 | The bench players' average rebounds per game played. |
| `bench_rebounds_offensive_per_game` | Float64 | The bench players' average offensive rebounds per game played. |
| `bench_rebounds_defensive_per_game` | Float64 | The bench players' average defensive rebounds per game played. |
| `bench_turnovers_per_game` | Float64 | The bench players' average turnovers per game played. |
| `bench_fouls_per_game` | Float64 | The bench players' average personal fouls per game played. |
| `bench_total_rebounds` | Int64 | The bench players' total rebounds over the games played. |
| `bench_total_rebounds_offensive` | Int64 | The bench players' total offensive rebounds over the games played. |
| `bench_total_rebounds_defensive` | Int64 | The bench players' total defensive rebounds over the games played. |
| `bench_total_turnovers` | Int64 | The bench players' total turnovers over the games played. |
| `bench_total_fouls` | Int64 | The bench players' total personal fouls over the games played. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fiba_gdap_team_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FIBA has nothing for the request (HTTP 404, or GDAP answered {} or [] for an unknown id). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. HTTP 400 / 422, or a malformed id, season or date. |

### Example {#fiba_gdap_team_stats-example}

```python
fiba_gdap_team_stats(team_id=279569)
```

## fiba_gdap_teams

Teams of a competition with their profile (final ranking, per-game averages).

**Endpoint URL:** `GET https://digital-api.fiba.basketball/hapi/getgdapcompetitionteamsbycompetitionid`

**Valid URL:** [https://digital-api.fiba.basketball/hapi/getgdapcompetitionteamsbycompetitionid?profile=true&gdapCompetitionId=208182](https://digital-api.fiba.basketball/hapi/getgdapcompetitionteamsbycompetitionid?profile=true&gdapCompetitionId=208182)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gdapCompetitionId` | `comp` |  | `Y` |  | Required. FIBA competition id (GDAP key space), one per edition: competition.competitionId of any game row (208182 = World Cup 2023, 208722 = Paris 2024 men, 208850 = BCL 2024-25). Find it with fiba_gdap_editions or fiba_gdap_games_between. |

### Returns {#fiba_gdap_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | Competition team id: one per team per competition (Germany = 279569 at the 2023 World Cup, 281545 at Paris 2024). Joins getgdapcompetitionteamrosterbyteamid / getgdapcompetitionteamstatisticsbyteamid gdapTeamId; not an organisationId. |
| `team_code` | String | Team abbreviation (FIBA country code or club abbreviation). |
| `short_name` | String | Short team name: the country code for a national team, the club's short name for a club. |
| `profile_team_id` | Int64 | Competition team id: one per team per competition (Germany = 279569 at the 2023 World Cup, 281545 at Paris 2024). Joins getgdapcompetitionteamrosterbyteamid / getgdapcompetitionteamstatisticsbyteamid gdapTeamId; not an organisationId. |
| `profile_organisation_id` | Int64 | Organisation id, stable across competitions and seasons: the national federation for a national team (64 = Germany), the club for a club team (396 = Filou Oostende). Same key space as nationalFederationId, clubId, play-by-play oId and the game page's T_<id>. |
| `profile_name` | String | Team name as registered for the competition. |
| `profile_code` | String | Team code (FIBA country code or club abbreviation). |
| `profile_country_name` | String | Country of the team, in English. |
| `profile_country_code` | String | Country FIBA code. |
| `profile_gender` | String | Men or Women: the gender of the team's competition. |
| `profile_facebook` | String | Facebook handle or URL; always null in the samples, values unverified. |
| `profile_twitter` | String | X/Twitter handle; always null in the samples, values unverified. |
| `profile_instagram` | String | Instagram handle; always null in the samples, values unverified. |
| `profile_website` | String | The team's website address. |
| `profile_you_tube` | String | YouTube handle; always null in the samples, values unverified. |
| `profile_final_ranking` | Int64 | Final rank in the competition. |
| `profile_points_per_game` | Float64 | The team's average points per game played. |
| `profile_rebounds_per_game` | Float64 | The team's average rebounds per game played. |
| `profile_assists_per_game` | Float64 | The team's average assists per game played. |
| `profile_rank_points_per_game` | Float64 | Rank in the competition by points per game (1 = highest). |
| `profile_rank_rebounds_per_game` | Float64 | Rank in the competition by rebounds per game (1 = highest). |
| `profile_rank_assists_per_game` | Float64 | Rank in the competition by assists per game (1 = highest). |
| `profile_total_games_won` | Int64 | The team's total games won over the games played. |
| `profile_total_games_lost` | Int64 | The team's total games lost over the games played. |
| `profile_social_networks` | String | Social-network handles. Stored as a JSON string. |
| `profile_competition_id` | Int64 | FIBA competition id (GDAP key space): one per edition (2023 World Cup = 208182, BCL 2024-25 = 208850); gdapCompetitionId. |
| `status_code` | String | Team entry status: APPR approved. |
| `status_name` | String | Team entry status label (Approved). |
| `organisation_id` | Int64 | Organisation id, stable across competitions and seasons: the national federation for a national team (64 = Germany), the club for a club team (396 = Filou Oostende). Same key space as nationalFederationId, clubId, play-by-play oId and the game page's T_<id>. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fiba_gdap_teams-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FIBA has nothing for the request (HTTP 404, or GDAP answered {} or [] for an unknown id). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. HTTP 400 / 422, or a malformed id, season or date. |

### Example {#fiba_gdap_teams-example}

```python
fiba_gdap_teams(comp=208182)
```
