---
title: "KZS — KZS FMS public API (Basketball Federation of Slovenia) — Team"
sidebar_label: "Team"
sidebar_position: 2
description: "KZS — KZS FMS public API (Basketball Federation of Slovenia) — Team — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# KZS — KZS FMS public API (Basketball Federation of Slovenia) — Team

## kzs_team_leaderboard

Team leaderboard of a competition for one stat category (total, per-game average and single-game best).

**Endpoint URL:** `GET https://api.kzs.si/api/v1/public/competitions/{competition_id}/teamStats`

**Valid URL:** [https://api.kzs.si/api/v1/public/competitions/579/teamStats?limit=1000&statsCategory=PTS&orderType=AVERAGE&sortBy=avg&sort=DESC](https://api.kzs.si/api/v1/public/competitions/579/teamStats?limit=1000&statsCategory=PTS&orderType=AVERAGE&sortBy=avg&sort=DESC)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competition_id` | `competition_id` |  | `Y` |  | Competition id (competition_id in kzs_competitions): one per competition per season, e.g. 579 (men's first league 2025/26). |
| `statsCategory` | `stats_category` |  |  | `Y` | Stat category (the site's statsCategory): PTS points, M_2/A_2/PERC_2 two-pointers, M_3/A_3/PERC_3 three-pointers, M_FG/A_FG/PERC_FG field goals, M_FT/A_FT/PERC_FT free throws, R rebounds (R_D defensive, R_O offensive), ASS assists, ST steals, TOV turnovers, PF_CM/PF_RV fouls committed/received, ASS_TOV and ST_TOV ratios. Competitions without live stats offer only PTS, M_2, M_3, M_FT, A_FT, PERC_FT. |
| `orderType` | `order_type` |  |  | `Y` | Figure the leaderboard is built on: AVERAGE (per game), SUM (season total) or BEST_PERFORMANCE. |
| `sortBy` | `sort_by` |  |  | `Y` | Row column the board is ordered by: avg (average), sum or max. The runtime checks the order. |
| `sort` | `sort` |  |  | `Y` | Sort direction. |

### Returns {#kzs_team_leaderboard-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `competition_id` | Int64 | Competition id: one per competition per season (men's first league 2025/26 = 579, 2026/27 = 608). |
| `stats_category` | String | Stat category of the board (PTS points, R rebounds, ASS assists ...): the request's, the rows do not echo it. |
| `club_id` | Int64 | Club id: the club across seasons and categories (Ilirija = 16). No captured route is keyed by it. |
| `club_fullname` | String | Club's full legal name. |
| `club_logo_uuid` | String | Club logo: image file id, served at https://api.kzs.si/public/images/{uuid}. |
| `club_code` | String | Club 3-letter code. |
| `team_id` | Int64 | Team id: one club team in one season (Ilirija 2025-26 = 17551). Joins to /teams/{teamId}, standings `teamId`, box-score `teamId` and the `teamId` query params. |
| `team_name` | String | Team name for the season, sponsor included. |
| `games_played` | Int64 | Games the team played in the competition. |
| `sum` | Float64 | Season total of the requested statsCategory. |
| `average` | Float64 | Per-game average of the statsCategory. |
| `max` | Float64 | Single-game best of the statsCategory. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kzs_team_leaderboard-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, a competition without standings, a final game with no play-by-play or box score). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API answered HTTP 400 (its only error status: an unknown id or a bad parameter). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: a full list page, a game that is not final, an envelope whose status is not OK, or a body for another competition, match, team, player or season than the one asked for. |

### Example {#kzs_team_leaderboard-example}

```python
kzs_team_leaderboard(competition_id=579)
```

## kzs_team_box_score

Team totals of a final game's box score, one row per side.

**Endpoint URL:** `GET https://api.kzs.si/api/v1/public/matches/{match_id}/stats`

**Valid URL:** [https://api.kzs.si/api/v1/public/matches/101839/stats](https://api.kzs.si/api/v1/public/matches/101839/stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Match id (match_id in kzs_matches), e.g. 101839. Global across seasons. |

### Returns {#kzs_team_box_score-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | Int64 | Match id: the federation key of a game, global across seasons (joins kzs_matches and kzs_match). |
| `team_side` | String | Which side of the match the row belongs to: home (the organiser, firstTeam on the wire) or away. |
| `team_id` | Int64 | Team id: one club team in one season (joins kzs_teams); not the match-team id and not the club id. |
| `team_name` | String | Name of the team for the season, sponsor included. |
| `minutes` | Float64 | Team minutes: the sum of its players (200.0 = 5 x 40; 0.0 on 2015-16 team lines). |
| `two_pm` | Int64 | The team's two-point field goals made. |
| `two_pa` | Int64 | The team's two-point field goals attempted. |
| `two_p_perc` | Float64 | The team's two-point percentage, 0-100. |
| `three_pm` | Int64 | The team's three-point field goals made. |
| `three_pa` | Int64 | The team's three-point field goals attempted. |
| `three_p_perc` | Float64 | The team's three-point percentage, 0-100. |
| `f_tm` | Int64 | The team's free throws made. |
| `f_ta` | Int64 | The team's free throws attempted. |
| `f_t_perc` | Float64 | The team's free-throw percentage, 0-100. |
| `fg_m` | Int64 | The team's field goals made. |
| `fg_a` | Int64 | The team's field goals attempted. |
| `fg_perc` | Float64 | The team's field-goal percentage, 0-100. |
| `offensive_rebounds` | Int64 | The team's offensive rebounds. |
| `defensive_rebounds` | Int64 | The team's defensive rebounds. |
| `total_rebounds` | Int64 | The team's total rebounds. |
| `assists` | Int64 | The team's assists. |
| `steals` | Int64 | The team's steals. |
| `turnovers` | Int64 | The team's turnovers. |
| `blocks_in_favor` | Int64 | The team's shots blocked (blocks made). |
| `blocks_against` | Int64 | The team's own shots that were blocked. |
| `foul_commited` | Int64 | The team's fouls committed. |
| `foul_received` | Int64 | The team's fouls drawn. |
| `p_ir` | Int64 | The team's PIR, performance index rating ((points + rebounds + assists + steals + blocks + fouls drawn) - (missed field goals + missed free throws + turnovers + shots blocked + fouls committed)). |
| `plus_minus` | Int64 | The team's plus-minus: points scored minus allowed while on court (team line: the final margin). |
| `assists_turnover_ratio` | Float64 | The team's assist-to-turnover ratio (0 in every sample: not computed). |
| `defensive_rating` | Float64 | The team's defensive rating (0 in every sample: not computed). |
| `efficiency` | Int64 | Team efficiency (PIR); -1 on 2015-16 team lines (not computed). |
| `points` | Int64 | The team's points. |
| `efficiency_custom` | Float64 | The team's custom efficiency as the federation computes it (equal to `efficiency` on player rows; 0 on team lines). |
| `efficiency_game_score` | Float64 | The team's game score (0 in every sample: not computed). |
| `fast_break_points_attempted` | Int64 | The team's fast-break attempts (0 in every sample: not computed). |
| `fast_break_points_made` | Int64 | The team's fast-break makes (0 in every sample: not computed). |
| `usage_percentage` | Float64 | The team's usage percentage (0 in every sample: not computed). |
| `m_vp_votes` | Int64 | The team's MVP votes (0 in every sample). |
| `minus` | Int64 | The team's points allowed on court (0 in every sample: not computed). |
| `offensive_rating` | Float64 | The team's offensive rating (0 in every sample: not computed). |
| `p_er` | Float64 | The team's PER (0 in every sample: not computed). |
| `plus` | Int64 | The team's points scored on court (0 in every sample: not computed). |
| `points_fast_break` | Int64 | The team's fast-break points. |
| `points_in_the_paint` | Int64 | The team's points in the paint. |
| `points_in_the_paint_attempted` | Int64 | The team's paint attempts (0 in every sample: not computed). |
| `points_in_the_paint_made` | Int64 | The team's paint makes (0 in every sample: not computed). |
| `points_second_chance` | Int64 | The team's second-chance points. |
| `second_chance_points_attempted` | Int64 | The team's second-chance attempts (0 in every sample: not computed). |
| `second_chance_points_made` | Int64 | The team's second-chance makes (0 in every sample: not computed). |
| `true_shooting_attempts` | Int64 | The team's true-shooting attempts (0 in every sample: not computed). |
| `true_shooting_percentage` | Float64 | The team's true-shooting percentage (0 in every sample: not computed). |
| `bench_points` | Int64 | Points by players who did not start. |
| `biggest_lead` | Int64 | Biggest lead, points. |
| `biggest_scoring_run` | Int64 | Biggest unanswered scoring run, points. |
| `fouls_bench_technical` | Int64 | Technical fouls charged to the bench (not the coach). |
| `fouls_coach_disqualifying` | Int64 | Disqualifying fouls charged to the head coach. |
| `fouls_coach_technical` | Int64 | Technical fouls charged to the head coach. |
| `fouls_disqualifying` | Int64 | Disqualifying fouls by players (ejections). |
| `fouls_offensive` | Int64 | Offensive (charging) fouls by players. |
| `fouls_on` | Int64 | Fouls drawn by the team. |
| `fouls_personal` | Int64 | Personal fouls by players, all kinds that count toward fouling out. |
| `fouls_team` | Int64 | Team fouls (0 in every sample). |
| `fouls_technical` | Int64 | Technical fouls by players. |
| `fouls_technical_other` | Int64 | Other technical fouls (0 in every sample). |
| `fouls_throw_in` | Int64 | Throw-in fouls (0 in every sample). |
| `fouls_total` | Int64 | All fouls committed. |
| `fouls_unsportsmanlike` | Int64 | Unsportsmanlike (flagrant) fouls by players. |
| `fouled_out` | Int64 | Players fouled out (0 in every sample). |
| `lead_changes` | Int64 | Lead changes in the game. |
| `points_from_turnovers` | Int64 | Points off opponent turnovers. |
| `time_leading` | Float64 | Time the team led, decimal minutes. |
| `times_scores_level` | Int64 | Times the score was tied. |
| `rebounds_team` | Int64 | Team rebounds (not credited to a player). |
| `rebounds_team_defensive` | Int64 | Defensive rebounds credited to the team, not a player. |
| `rebounds_team_offensive` | Int64 | Offensive rebounds credited to the team, not a player. |
| `turnovers_team` | Int64 | Team turnovers (not credited to a player). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kzs_team_box_score-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, a competition without standings, a final game with no play-by-play or box score). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API answered HTTP 400 (its only error status: an unknown id or a bad parameter). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: a full list page, a game that is not final, an envelope whose status is not OK, or a body for another competition, match, team, player or season than the one asked for. |

### Example {#kzs_team_box_score-example}

```python
kzs_team_box_score(match_id=101839)
```

## kzs_teams

Teams entered in a competition.

**Endpoint URL:** `GET https://api.kzs.si/api/v1/public/teams/`

**Valid URL:** [https://api.kzs.si/api/v1/public/teams/?limit=1000&competitionId=579](https://api.kzs.si/api/v1/public/teams/?limit=1000&competitionId=579)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competitionId` | `competition_id` |  | `Y` |  | Competition id (competition_id in kzs_competitions): one per competition per season, e.g. 579 (men's first league 2025/26). |

### Returns {#kzs_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `competition_id` | Int64 | Competition id: one per competition per season (men's first league 2025/26 = 579, 2026/27 = 608). |
| `team_id` | Int64 | Team-application id: the team's registration (entry) for the season; play-by-play `teamApplicationId` is the same key space. |
| `team_application_id` | Int64 | Team-application id: the team's registration (entry) for the season; play-by-play `teamApplicationId` is the same key space. |
| `club_id` | Int64 | Club id: the club across seasons and categories (Ilirija = 16). No captured route is keyed by it. |
| `club_member` | Boolean | True when the club is a KZS member (false on nested copies that omit it). |
| `team_internal_ranking` | String | The club's internal team letter for the category (A = first team, B = second team). |
| `category` | String | Age category: ABSOLUTE (senior) or U8-U23 / ANY (code table in kzs-returns.md). |
| `rank` | String | Tier: FIRST-FOURTH or TOURNAMENT (code table). |
| `gender` | String | MALE or FEMALE. |
| `sport_hall_id` | Int64 | Sport hall (venue) id. /sportHalls/{id} is in the site bundle, not captured. |
| `sport_hall_name` | String | Name of the venue (sports hall) where the game is played or the team is based. |
| `name` | String | Team name for the season (sponsor name included). |
| `code` | String | Team 3-letter code. |
| `home_jersey_color` | String | Home jersey colour, hex '#RRGGBB'. |
| `guest_jersey_color` | String | Away jersey colour, hex. |
| `logo_uuid` | String | Team logo: image file id, served at https://api.kzs.si/public/images/{uuid}. |
| `home_team_color` | String | Home colour, hex. |
| `guest_team_color` | String | Away colour, hex. |
| `short_name` | String | Team short name ('' when none). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kzs_teams-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, a competition without standings, a final game with no play-by-play or box score). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API answered HTTP 400 (its only error status: an unknown id or a bad parameter). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: a full list page, a game that is not final, an envelope whose status is not OK, or a body for another competition, match, team, player or season than the one asked for. |

### Example {#kzs_teams-example}

```python
kzs_teams(competition_id=579)
```

## kzs_team

Team profile: club, home hall, colours and the competitions the team entered.

**Endpoint URL:** `GET https://api.kzs.si/api/v1/public/teams/{team_id}`

**Valid URL:** [https://api.kzs.si/api/v1/public/teams/17551](https://api.kzs.si/api/v1/public/teams/17551)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Team id (team_id in kzs_teams): one club team in one season, e.g. 17551. Not the match-team id nor the club id. |

### Returns {#kzs_team-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | Team-application id: the team's registration (entry) for the season; play-by-play `teamApplicationId` is the same key space. |
| `team_application_id` | Int64 | Team-application id: the team's registration (entry) for the season; play-by-play `teamApplicationId` is the same key space. |
| `club_id` | Int64 | Club id: the club across seasons and categories (Ilirija = 16). No captured route is keyed by it. |
| `club_member` | Boolean | True when the club is a KZS member (false on nested copies that omit it). |
| `category` | String | Age category: ABSOLUTE (senior) or U8-U23 / ANY (code table in kzs-returns.md). |
| `rank` | String | Tier: FIRST-FOURTH or TOURNAMENT (code table). |
| `gender` | String | MALE or FEMALE. |
| `sport_hall_id` | Int64 | Sport hall (venue) id. /sportHalls/{id} is in the site bundle, not captured. |
| `sport_hall_name` | String | Name of the venue (sports hall) where the game is played or the team is based. |
| `sport_hall_address_id` | Int64 | Id of the venue's address record. |
| `sport_hall_address_post_office_id` | Int64 | Post-office id (FMS internal). |
| `sport_hall_address_post_office_zip_code` | String | Postal code (REDACTED under a club `addresses[]` row; kept for venues). |
| `sport_hall_address_post_office_name` | String | Post-office town. |
| `sport_hall_address_post_office_municipality` | String | Municipality, upper case. |
| `sport_hall_address_post_office_country` | String | Country, ISO 3166-1 alpha-2 (SI). |
| `sport_hall_address_street` | String | Venue street (a public sports hall). |
| `sport_hall_address_house_number` | String | Venue house number, a string ('6 A'). |
| `name` | String | Team name for the season (sponsor name included). |
| `code` | String | Team 3-letter code. |
| `season_id` | Int64 | Season id: OPAQUE, not a year (1 = 2000/2001 ... 27 = 2026/2027, 31 = 1991/1992). The season `name` is 'START/END'; the sdv season is the END year (the second half of `name`). |
| `season_name` | String | Season, 'START/END' (2025/2026); sdv season = the END year. |
| `season_is_active` | Boolean | True for the current season only (2026/2027 on 2026-10-12). |
| `season_active` | Boolean | Same as `isActive`. |
| `short_name` | String | Team short name ('' when none). |
| `home_jersey_color` | String | Home jersey colour, hex '#RRGGBB'. |
| `guest_jersey_color` | String | Away jersey colour, hex. |
| `logo_uuid` | String | Logo: image file id, served at https://api.kzs.si/public/images/{uuid}. |
| `season` | Int64 | Season of the registration. |
| `competitions` | String | Competitions of the team this season (id, name, rule, phases). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kzs_team-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, a competition without standings, a final game with no play-by-play or box score). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API answered HTTP 400 (its only error status: an unknown id or a bad parameter). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: a full list page, a game that is not final, an envelope whose status is not OK, or a body for another competition, match, team, player or season than the one asked for. |

### Example {#kzs_team-example}

```python
kzs_team(team_id=17551)
```

## kzs_team_general

A team's record, form and best players in one competition (the body echoes neither id).

**Endpoint URL:** `GET https://api.kzs.si/api/v1/public/teams/{team_id}/general`

**Valid URL:** [https://api.kzs.si/api/v1/public/teams/17551/general?competitionId=579](https://api.kzs.si/api/v1/public/teams/17551/general?competitionId=579)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Team id (team_id in kzs_teams): one club team in one season, e.g. 17551. Not the match-team id nor the club id. |
| `competitionId` | `competition_id` |  | `Y` |  | Competition id (competition_id in kzs_competitions): one per competition per season, e.g. 579 (men's first league 2025/26). |

### Returns {#kzs_team_general-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | Team id: one club team in one season (joins kzs_teams); not the match-team id and not the club id. |
| `competition_id` | Int64 | Competition id: one per competition per season (men's first league 2025/26 = 579, 2026/27 = 608). |
| `won` | Int64 | Wins in the competition (all phases). |
| `lost` | Int64 | Losses in the competition. |
| `last_games` | String | Recent results, 'W' or 'L' per game (order unverified). |
| `best_points_player_id` | Int64 | Player id (KZS player registry). Joins to /players/{playerId}, box-score and play-by-play `playerId`. |
| `best_points_player_identification_number` | Int64 | Player's KZS registration (licence) number; a different key space from the player id, used by no captured route. |
| `best_points_player_person_id` | Int64 | Person id: the federation's person record behind a player, coach or official; a different key space from player, coach and official ids. |
| `best_points_player_person_first_name` | String | The player's given name. |
| `best_points_player_person_last_name` | String | The player's family name. |
| `best_points_player_person_year_of_birth` | Int64 | Year of birth (players: public bio). |
| `best_points_player_nationality_id` | Int64 | Country id (FMS internal; use the ISO codes). |
| `best_points_player_nationality_alpha2_code` | String | ISO 3166-1 alpha-2 country code. |
| `best_points_player_nationality_alpha3_code` | String | ISO 3166-1 alpha-3 country code. |
| `best_points_player_nationality_name` | String | Country name in Slovenian (e.g. 'Združene države' = United States); use `engName` or the ISO codes. |
| `best_points_player_nationality_eng_name` | String | Country name in English. |
| `best_points_player_homegrown` | Boolean | True when the player counts as homegrown (domači igralec) for roster limits. |
| `best_points_player_photo_uuid` | String | Player photo: image file id, served at https://api.kzs.si/public/images/{uuid}. |
| `best_points_player_positions` | String | Unobserved: null, or an empty array, in every sample, so its type and meaning are unconfirmed. |
| `best_average_points` | Float64 | That player's points per game. |
| `best_rebounds_player_id` | Int64 | Player id (KZS player registry). Joins to /players/{playerId}, box-score and play-by-play `playerId`. |
| `best_rebounds_player_identification_number` | Int64 | Player's KZS registration (licence) number; a different key space from the player id, used by no captured route. |
| `best_rebounds_player_person_id` | Int64 | Person id: the federation's person record behind a player, coach or official; a different key space from player, coach and official ids. |
| `best_rebounds_player_person_first_name` | String | The player's given name. |
| `best_rebounds_player_person_last_name` | String | The player's family name. |
| `best_rebounds_player_person_year_of_birth` | Int64 | Year of birth (players: public bio). |
| `best_rebounds_player_nationality_id` | Int64 | Country id (FMS internal; use the ISO codes). |
| `best_rebounds_player_nationality_alpha2_code` | String | ISO 3166-1 alpha-2 country code. |
| `best_rebounds_player_nationality_alpha3_code` | String | ISO 3166-1 alpha-3 country code. |
| `best_rebounds_player_nationality_name` | String | Country name in Slovenian (e.g. 'Združene države' = United States); use `engName` or the ISO codes. |
| `best_rebounds_player_nationality_eng_name` | String | Country name in English. |
| `best_rebounds_player_homegrown` | Boolean | True when the player counts as homegrown (domači igralec) for roster limits. |
| `best_rebounds_player_photo_uuid` | String | Player photo: image file id, served at https://api.kzs.si/public/images/{uuid}. |
| `best_rebounds_player_positions` | String | Unobserved: null, or an empty array, in every sample, so its type and meaning are unconfirmed. |
| `best_average_rebounds` | Float64 | That player's rebounds per game. |
| `best_assists_player_id` | Int64 | Player id (KZS player registry). Joins to /players/{playerId}, box-score and play-by-play `playerId`. |
| `best_assists_player_identification_number` | Int64 | Player's KZS registration (licence) number; a different key space from the player id, used by no captured route. |
| `best_assists_player_person_id` | Int64 | Person id: the federation's person record behind a player, coach or official; a different key space from player, coach and official ids. |
| `best_assists_player_person_first_name` | String | The player's given name. |
| `best_assists_player_person_last_name` | String | The player's family name. |
| `best_assists_player_person_year_of_birth` | Int64 | Year of birth (players: public bio). |
| `best_assists_player_nationality_id` | Int64 | Country id (FMS internal; use the ISO codes). |
| `best_assists_player_nationality_alpha2_code` | String | ISO 3166-1 alpha-2 country code. |
| `best_assists_player_nationality_alpha3_code` | String | ISO 3166-1 alpha-3 country code. |
| `best_assists_player_nationality_name` | String | Country name in Slovenian (e.g. 'Združene države' = United States); use `engName` or the ISO codes. |
| `best_assists_player_nationality_eng_name` | String | Country name in English. |
| `best_assists_player_homegrown` | Boolean | True when the player counts as homegrown (domači igralec) for roster limits. |
| `best_assists_player_photo_uuid` | String | Player photo: image file id, served at https://api.kzs.si/public/images/{uuid}. |
| `best_assists_player_positions` | String | Unobserved: null, or an empty array, in every sample, so its type and meaning are unconfirmed. |
| `best_average_assists` | Float64 | That player's assists per game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kzs_team_general-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, a competition without standings, a final game with no play-by-play or box score). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API answered HTTP 400 (its only error status: an unknown id or a bad parameter). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: a full list page, a game that is not final, an envelope whose status is not OK, or a body for another competition, match, team, player or season than the one asked for. |

### Example {#kzs_team_general-example}

```python
kzs_team_general(competition_id=579, team_id=17551)
```
