---
title: "FIBA (digital API and game pages) — FIBA digital API (GDAP, digital-api.fiba.basketball) — Player"
sidebar_label: "Player"
sidebar_position: 3
description: "FIBA (digital API and game pages) — FIBA digital API (GDAP, digital-api.fiba.basketball) — Player — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# FIBA (digital API and game pages) — FIBA digital API (GDAP, digital-api.fiba.basketball) — Player

## fiba_gdap_player_game_stats

A player's game log (full box lines) at a competition.

**Endpoint URL:** `GET https://digital-api.fiba.basketball/hapi/getgdapplayergamestatisticsbyplayerid`

**Valid URL:** [https://digital-api.fiba.basketball/hapi/getgdapplayergamestatisticsbyplayerid?gdapPlayerId=211501&gdapCompetitionId=208182](https://digital-api.fiba.basketball/hapi/getgdapplayergamestatisticsbyplayerid?gdapPlayerId=211501&gdapCompetitionId=208182)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gdapPlayerId` | `player_id` |  | `Y` |  | Required. FIBA person id (global): personId of a roster row, pId in play-by-play, P_<id> in the game-page box |
| `gdapCompetitionId` | `comp` |  | `Y` |  | Required. FIBA competition id (GDAP key space), one per edition: competition.competitionId of any game row (208182 = World Cup 2023, 208722 = Paris 2024 men, 208850 = BCL 2024-25). Find it with fiba_gdap_editions or fiba_gdap_games_between. |

### Returns {#fiba_gdap_player_game_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | integer | FIBA person id (Int64), global; the player_id argument. |
| `competition_id` | integer | FIBA competition id (Int64) the game log belongs to; the comp argument. |
| `game_id` | integer | FIBA game id, global across competitions (GDAP key space): gdapGameId, the <gameId> of a game-page URL. |
| `game_day` | integer | Game day number within the round. |
| `round_id` | integer | Round id (GDAP key space, unique across competitions); the first segment of gameName. |
| `round_code` | character | Round code (GP group phase, QF, SF, F final, RS regular season, CL17-32 classification...). |
| `round_name` | character | Round label (Group Phase, Quarter-Finals, Final...). |
| `team_id` | integer | Competition team id: one per team per competition (Germany = 279569 at the 2023 World Cup, 281545 at Paris 2024). Joins getgdapcompetitionteamrosterbyteamid / getgdapcompetitionteamstatisticsbyteamid gdapTeamId; not an organisationId. |
| `team_name` | character | Player's team name. |
| `team_code` | character | Player's team code. |
| `team_organisation_id` | integer | Organisation id, stable across competitions and seasons: the national federation for a national team (64 = Germany), the club for a club team (396 = Filou Oostende). Same key space as nationalFederationId, clubId, play-by-play oId and the game page's T_<id>. |
| `versus_team_id` | integer | Opponent's competition team id (see teamId). |
| `versus_team_name` | character | Opponent's name. |
| `versus_team_code` | character | Opponent's code. |
| `versus_team_organisation_id` | integer | Opponent's organisation id (see organisationId). |
| `play_duration_in_seconds` | integer | Playing time in seconds in the game. |
| `points` | integer | Points in the game. |
| `field_goals_made` | integer | Field goals made in the game. |
| `field_goals_attempted` | integer | Field goals attempted in the game. |
| `field_goals_percentage` | numeric | Field-goal percentage, 0-100 (in the game; full float precision in averages). |
| `two_points_made` | integer | Two-point field goals made in the game. |
| `two_points_attempted` | integer | Two-point field goals attempted in the game. |
| `two_points_percentage` | numeric | Two-point field-goal percentage, 0-100 (in the game; full float precision in averages). |
| `three_points_made` | integer | Three-point field goals made in the game. |
| `three_points_attempted` | integer | Three-point field goals attempted in the game. |
| `three_points_percentage` | numeric | Three-point field-goal percentage, 0-100 (in the game; full float precision in averages). |
| `free_throws_made` | integer | Free throws made in the game. |
| `free_throws_attempted` | integer | Free throws attempted in the game. |
| `free_throws_percentage` | numeric | Free-throw percentage, 0-100 (in the game; full float precision in averages). |
| `offensive_rebounds` | integer | Offensive rebounds in the game. |
| `defensive_rebounds` | integer | Defensive rebounds in the game. |
| `rebounds` | integer | Rebounds in the game. |
| `assists` | integer | Assists in the game. |
| `personal_fouls` | integer | Personal fouls in the game. |
| `turnovers` | integer | Turnovers in the game. |
| `steals` | integer | Steals in the game. |
| `blocked_shots` | integer | Blocked shots in the game. |
| `plus_minus` | integer | Plus/minus (point differential while on court) in the game. |
| `efficiency` | integer | Efficiency (FIBA EFF) in the game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fiba_gdap_player_game_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FIBA has nothing for the request (HTTP 404, or GDAP answered {} or [] for an unknown id). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. HTTP 400 / 422, or a malformed id, season or date. |

### Example {#fiba_gdap_player_game_stats-example}

```python
fiba_gdap_player_game_stats(comp=208182, player_id=211501)
```

_Last validated n/a._

## fiba_gdap_player_info

A player's bio and per-game averages with ranks at a competition.

**Endpoint URL:** `GET https://digital-api.fiba.basketball/hapi/getgdapplayerinfobycompetitionid`

**Valid URL:** [https://digital-api.fiba.basketball/hapi/getgdapplayerinfobycompetitionid?gdapPlayerId=211501&gdapCompetitionId=208182](https://digital-api.fiba.basketball/hapi/getgdapplayerinfobycompetitionid?gdapPlayerId=211501&gdapCompetitionId=208182)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gdapPlayerId` | `player_id` |  | `Y` |  | Required. FIBA person id (global): personId of a roster row, pId in play-by-play, P_<id> in the game-page box |
| `gdapCompetitionId` | `comp` |  | `Y` |  | Required. FIBA competition id (GDAP key space), one per edition: competition.competitionId of any game row (208182 = World Cup 2023, 208722 = Paris 2024 men, 208850 = BCL 2024-25). Find it with fiba_gdap_editions or fiba_gdap_games_between. |

### Returns {#fiba_gdap_player_info-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | integer | FIBA person id, global across competitions and roles (player, coach, official): roster personId = play-by-play pId = game-page P_<id> = gdapPlayerId. |
| `competition_id` | integer | FIBA competition id (GDAP key space): one per edition (2023 World Cup = 208182, BCL 2024-25 = 208850); gdapCompetitionId. |
| `competition_team_id` | integer | Competition team id: one per team per competition (Germany = 279569 at the 2023 World Cup, 281545 at Paris 2024). Joins getgdapcompetitionteamrosterbyteamid / getgdapcompetitionteamstatisticsbyteamid gdapTeamId; not an organisationId. |
| `organisation_id` | integer | Organisation id, stable across competitions and seasons: the national federation for a national team (64 = Germany), the club for a club team (396 = Filou Oostende). Same key space as nationalFederationId, clubId, play-by-play oId and the game page's T_<id>. |
| `first_name` | character | Given name(s) as registered with FIBA. |
| `last_name` | character | Last name (officials: upper case). |
| `height_cm` | integer | Height in centimetres. |
| `height_ft_in` | character | Height, feet and inches string. |
| `club_name` | character | Club at the time of the competition. |
| `club_code` | character | Club abbreviation; always null in the samples, values unverified. |
| `is_player_free_agent` | logical | True when the player had no club. |
| `points_per_game` | numeric | The player's average points per game played. |
| `rank_points_per_game` | numeric | Rank in the competition by points per game (1 = highest). |
| `rebounds_per_game` | numeric | The player's average rebounds per game played. |
| `rank_rebounds_per_game` | numeric | Rank in the competition by rebounds per game (1 = highest). |
| `assists_per_game` | numeric | The player's average assists per game played. |
| `rank_assists_per_game` | numeric | Rank in the competition by assists per game (1 = highest). |
| `points_per_game_per_competition_category` | numeric | Average points per game across the competition category, the comparison baseline (unverified). |
| `rebounds_per_game_per_competition_category` | numeric | Average rebounds per game across the competition category, the comparison baseline (unverified). |
| `assists_per_game_per_competition_category` | numeric | Average assists per game across the competition category, the comparison baseline (unverified). |
| `birth_date` | character | Date of birth, YYYY-MM-DD. |
| `nationalities` | character | Nationalities (FIBA codes on player info; country objects on rosters and officials). Stored as a JSON string. |
| `social_media_website` | character | Website URL; always null in the samples, values unverified. |
| `social_media_twitter` | character | X/Twitter handle; always null in the samples, values unverified. |
| `social_media_facebook` | character | Facebook handle or URL; always null in the samples, values unverified. |
| `social_media_instagram` | character | Instagram handle; always null in the samples, values unverified. |
| `social_media_youtube` | character | YouTube handle; always null in the samples, values unverified. |
| `social_networks` | character | Social-network handles. Stored as a JSON string. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fiba_gdap_player_info-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FIBA has nothing for the request (HTTP 404, or GDAP answered {} or [] for an unknown id). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. HTTP 400 / 422, or a malformed id, season or date. |

### Example {#fiba_gdap_player_info-example}

```python
fiba_gdap_player_info(comp=208182, player_id=211501)
```

_Last validated n/a._

## fiba_gdap_player_stats

A player's per-game averages, totals and highs at a competition.

**Endpoint URL:** `GET https://digital-api.fiba.basketball/hapi/getgdapplayercompetitionstatisticsbyplayerid`

**Valid URL:** [https://digital-api.fiba.basketball/hapi/getgdapplayercompetitionstatisticsbyplayerid?gdapPlayerId=211501&gdapCompetitionId=208182](https://digital-api.fiba.basketball/hapi/getgdapplayercompetitionstatisticsbyplayerid?gdapPlayerId=211501&gdapCompetitionId=208182)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gdapPlayerId` | `player_id` |  | `Y` |  | Required. FIBA person id (global): personId of a roster row, pId in play-by-play, P_<id> in the game-page box |
| `gdapCompetitionId` | `comp` |  | `Y` |  | Required. FIBA competition id (GDAP key space), one per edition: competition.competitionId of any game row (208182 = World Cup 2023, 208722 = Paris 2024 men, 208850 = BCL 2024-25). Find it with fiba_gdap_editions or fiba_gdap_games_between. |

### Returns {#fiba_gdap_player_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | integer | FIBA person id, global across competitions and roles (player, coach, official): roster personId = play-by-play pId = game-page P_<id> = gdapPlayerId. |
| `competition_id` | integer | FIBA competition id (GDAP key space): one per edition (2023 World Cup = 208182, BCL 2024-25 = 208850); gdapCompetitionId. |
| `points_per_game` | numeric | The player's average points per game played. |
| `rebounds_per_game` | numeric | The player's average rebounds per game played. |
| `rebounds_offensive_per_game` | numeric | The player's average offensive rebounds per game played. |
| `rebounds_defensive_per_game` | numeric | The player's average defensive rebounds per game played. |
| `assists_per_game` | numeric | The player's average assists per game played. |
| `blocks_per_game` | numeric | The player's average blocked shots per game played. |
| `turnovers_per_game` | numeric | The player's average turnovers per game played. |
| `fouls_per_game` | numeric | The player's average personal fouls per game played. |
| `plus_minus_per_game` | numeric | The player's average plus/minus (point differential while on court) per game played. |
| `efficiency_per_game` | numeric | The player's average efficiency (FIBA EFF) per game played. |
| `play_time_in_seconds_per_game` | numeric | The player's average playing time in seconds per game played. |
| `field_goals_made_per_game` | numeric | The player's average field goals made per game played. |
| `field_goals_attempted_per_game` | numeric | The player's average field goals attempted per game played. |
| `two_points_made_per_game` | numeric | The player's average two-point field goals made per game played. |
| `two_points_attempted_per_game` | numeric | The player's average two-point field goals attempted per game played. |
| `three_points_made_per_game` | numeric | The player's average three-point field goals made per game played. |
| `three_points_attempted_per_game` | numeric | The player's average three-point field goals attempted per game played. |
| `free_throws_made_per_game` | numeric | The player's average free throws made per game played. |
| `free_throws_attempted_per_game` | numeric | The player's average free throws attempted per game played. |
| `points_from_turnovers_per_game` | numeric | The player's average points from turnovers per game played. |
| `fast_break_points_per_game` | numeric | The player's average fast-break points per game played. |
| `second_chance_points_per_game` | numeric | The player's average second-chance points per game played. |
| `points_in_the_paint_per_game` | numeric | The player's average points in the paint per game played. |
| `steals_per_game` | numeric | The player's average steals per game played. |
| `total_games_played` | integer | The player's total games played over the games played. |
| `total_games_won` | integer | The player's total games won over the games played. |
| `total_games_lost` | integer | The player's total games lost over the games played. |
| `total_rebounds` | integer | The player's total rebounds over the games played. |
| `total_rebounds_offensive` | integer | The player's total offensive rebounds over the games played. |
| `total_rebounds_defensive` | integer | The player's total defensive rebounds over the games played. |
| `total_blocks` | integer | The player's total blocked shots over the games played. |
| `total_turnovers` | integer | The player's total turnovers over the games played. |
| `total_fouls` | integer | The player's total personal fouls over the games played. |
| `total_assists` | integer | The player's total assists over the games played. |
| `total_plus_minus` | integer | The player's total plus/minus (point differential while on court) over the games played. |
| `total_efficiency` | integer | The player's total efficiency (FIBA EFF) over the games played. |
| `total_play_time_in_seconds` | integer | The player's total playing time in seconds over the games played. |
| `total_points` | integer | The player's total points over the games played. |
| `total_field_goals_made` | integer | The player's total field goals made over the games played. |
| `total_field_goals_attempted` | integer | The player's total field goals attempted over the games played. |
| `field_goals_percentage` | numeric | Field-goal percentage, 0-100 (over the competition; full float precision in averages). |
| `total_two_points_made` | integer | The player's total two-point field goals made over the games played. |
| `total_two_points_attempted` | integer | The player's total two-point field goals attempted over the games played. |
| `two_points_percentage` | numeric | Two-point field-goal percentage, 0-100 (over the competition; full float precision in averages). |
| `total_three_points_made` | integer | The player's total three-point field goals made over the games played. |
| `total_three_points_attempted` | integer | The player's total three-point field goals attempted over the games played. |
| `three_points_percentage` | numeric | Three-point field-goal percentage, 0-100 (over the competition; full float precision in averages). |
| `total_free_throws_made` | integer | The player's total free throws made over the games played. |
| `total_free_throws_attempted` | integer | The player's total free throws attempted over the games played. |
| `free_throws_percentage` | numeric | Free-throw percentage, 0-100 (over the competition; full float precision in averages). |
| `total_double_doubles` | integer | The player's total double-doubles over the games played. |
| `total_triple_doubles` | integer | The player's total triple-doubles over the games played. |
| `total_quadruple_doubles` | integer | The player's total quadruple-doubles over the games played. |
| `total_quintuple_doubles` | integer | The player's total quintuple-doubles over the games played. |
| `total_points_from_turnovers` | integer | The player's total points from turnovers over the games played. |
| `total_fast_break_points` | integer | The player's total fast-break points over the games played. |
| `total_second_chance_points` | integer | The player's total second-chance points over the games played. |
| `total_points_in_the_paint` | integer | The player's total points in the paint over the games played. |
| `total_steals` | integer | The player's total steals over the games played. |
| `rank_points_per_game` | numeric | Rank in the competition by points per game (1 = highest). |
| `rank_rebounds_per_game` | numeric | Rank in the competition by rebounds per game (1 = highest). |
| `rank_assists_per_game` | numeric | Rank in the competition by assists per game (1 = highest). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fiba_gdap_player_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FIBA has nothing for the request (HTTP 404, or GDAP answered {} or [] for an unknown id). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. HTTP 400 / 422, or a malformed id, season or date. |

### Example {#fiba_gdap_player_stats-example}

```python
fiba_gdap_player_stats(comp=208182, player_id=211501)
```

_Last validated n/a._
