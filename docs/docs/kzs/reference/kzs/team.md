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
| `competition_id` | integer | Competition id: one per competition per season (men's first league 2025/26 = 579, 2026/27 = 608). |
| `stats_category` | character | Stat category of the board (PTS points, R rebounds, ASS assists ...): the request's, the rows do not echo it. |
| `club_id` | integer | Club id: the club across seasons and categories (Ilirija = 16). No captured route is keyed by it. |
| `club_fullname` | character | Club's full legal name. |
| `club_logo_uuid` | character | Club logo: image file id, served at https://api.kzs.si/public/images/{uuid}. |
| `club_code` | character | Club 3-letter code. |
| `team_id` | integer | Team id: one club team in one season (Ilirija 2025-26 = 17551). Joins to /teams/{teamId}, standings `teamId`, box-score `teamId` and the `teamId` query params. |
| `team_name` | character | Team name for the season, sponsor included. |
| `games_played` | integer | Games the team played in the competition. |
| `sum` | numeric | Season total of the requested statsCategory. |
| `average` | numeric | Per-game average of the statsCategory. |
| `max` | numeric | Single-game best of the statsCategory. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#kzs_team_leaderboard-example}

```python
kzs_team_leaderboard(competition_id=579)
```

_Last validated n/a._

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
| `match_id` | integer | Match id: the federation key of a game, global across seasons (joins kzs_matches and kzs_match). |
| `team_side` | character | Which side of the match the row belongs to: home (the organiser, firstTeam on the wire) or away. |
| `team_id` | integer | Team id: one club team in one season (joins kzs_teams); not the match-team id and not the club id. |
| `team_name` | character | Name of the team for the season, sponsor included. |
| `minutes` | numeric | Team minutes: the sum of its players (200.0 = 5 x 40; 0.0 on 2015-16 team lines). |
| `two_pm` | integer | The team's two-point field goals made. |
| `two_pa` | integer | The team's two-point field goals attempted. |
| `two_p_perc` | numeric | The team's two-point percentage, 0-100. |
| `three_pm` | integer | The team's three-point field goals made. |
| `three_pa` | integer | The team's three-point field goals attempted. |
| `three_p_perc` | numeric | The team's three-point percentage, 0-100. |
| `f_tm` | integer | The team's free throws made. |
| `f_ta` | integer | The team's free throws attempted. |
| `f_t_perc` | numeric | The team's free-throw percentage, 0-100. |
| `fg_m` | integer | The team's field goals made. |
| `fg_a` | integer | The team's field goals attempted. |
| `fg_perc` | numeric | The team's field-goal percentage, 0-100. |
| `offensive_rebounds` | integer | The team's offensive rebounds. |
| `defensive_rebounds` | integer | The team's defensive rebounds. |
| `total_rebounds` | integer | The team's total rebounds. |
| `assists` | integer | The team's assists. |
| `steals` | integer | The team's steals. |
| `turnovers` | integer | The team's turnovers. |
| `blocks_in_favor` | integer | The team's shots blocked (blocks made). |
| `blocks_against` | integer | The team's own shots that were blocked. |
| `foul_commited` | integer | The team's fouls committed. |
| `foul_received` | integer | The team's fouls drawn. |
| `p_ir` | integer | The team's PIR, performance index rating ((points + rebounds + assists + steals + blocks + fouls drawn) - (missed field goals + missed free throws + turnovers + shots blocked + fouls committed)). |
| `plus_minus` | integer | The team's plus-minus: points scored minus allowed while on court (team line: the final margin). |
| `assists_turnover_ratio` | numeric | The team's assist-to-turnover ratio (0 in every sample: not computed). |
| `defensive_rating` | numeric | The team's defensive rating (0 in every sample: not computed). |
| `efficiency` | integer | Team efficiency (PIR); -1 on 2015-16 team lines (not computed). |
| `points` | integer | The team's points. |
| `efficiency_custom` | numeric | The team's custom efficiency as the federation computes it (equal to `efficiency` on player rows; 0 on team lines). |
| `efficiency_game_score` | numeric | The team's game score (0 in every sample: not computed). |
| `fast_break_points_attempted` | integer | The team's fast-break attempts (0 in every sample: not computed). |
| `fast_break_points_made` | integer | The team's fast-break makes (0 in every sample: not computed). |
| `usage_percentage` | numeric | The team's usage percentage (0 in every sample: not computed). |
| `m_vp_votes` | integer | The team's MVP votes (0 in every sample). |
| `minus` | integer | The team's points allowed on court (0 in every sample: not computed). |
| `offensive_rating` | numeric | The team's offensive rating (0 in every sample: not computed). |
| `p_er` | numeric | The team's PER (0 in every sample: not computed). |
| `plus` | integer | The team's points scored on court (0 in every sample: not computed). |
| `points_fast_break` | integer | The team's fast-break points. |
| `points_in_the_paint` | integer | The team's points in the paint. |
| `points_in_the_paint_attempted` | integer | The team's paint attempts (0 in every sample: not computed). |
| `points_in_the_paint_made` | integer | The team's paint makes (0 in every sample: not computed). |
| `points_second_chance` | integer | The team's second-chance points. |
| `second_chance_points_attempted` | integer | The team's second-chance attempts (0 in every sample: not computed). |
| `second_chance_points_made` | integer | The team's second-chance makes (0 in every sample: not computed). |
| `true_shooting_attempts` | integer | The team's true-shooting attempts (0 in every sample: not computed). |
| `true_shooting_percentage` | numeric | The team's true-shooting percentage (0 in every sample: not computed). |
| `bench_points` | integer | Points by players who did not start. |
| `biggest_lead` | integer | Biggest lead, points. |
| `biggest_scoring_run` | integer | Biggest unanswered scoring run, points. |
| `fouls_bench_technical` | integer | Technical fouls charged to the bench (not the coach). |
| `fouls_coach_disqualifying` | integer | Disqualifying fouls charged to the head coach. |
| `fouls_coach_technical` | integer | Technical fouls charged to the head coach. |
| `fouls_disqualifying` | integer | Disqualifying fouls by players (ejections). |
| `fouls_offensive` | integer | Offensive (charging) fouls by players. |
| `fouls_on` | integer | Fouls drawn by the team. |
| `fouls_personal` | integer | Personal fouls by players, all kinds that count toward fouling out. |
| `fouls_team` | integer | Team fouls (0 in every sample). |
| `fouls_technical` | integer | Technical fouls by players. |
| `fouls_technical_other` | integer | Other technical fouls (0 in every sample). |
| `fouls_throw_in` | integer | Throw-in fouls (0 in every sample). |
| `fouls_total` | integer | All fouls committed. |
| `fouls_unsportsmanlike` | integer | Unsportsmanlike (flagrant) fouls by players. |
| `fouled_out` | integer | Players fouled out (0 in every sample). |
| `lead_changes` | integer | Lead changes in the game. |
| `points_from_turnovers` | integer | Points off opponent turnovers. |
| `time_leading` | numeric | Time the team led, decimal minutes. |
| `times_scores_level` | integer | Times the score was tied. |
| `rebounds_team` | integer | Team rebounds (not credited to a player). |
| `rebounds_team_defensive` | integer | Defensive rebounds credited to the team, not a player. |
| `rebounds_team_offensive` | integer | Offensive rebounds credited to the team, not a player. |
| `turnovers_team` | integer | Team turnovers (not credited to a player). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#kzs_team_box_score-example}

```python
kzs_team_box_score(match_id=101839)
```

_Last validated n/a._

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
| `competition_id` | integer | Competition id: one per competition per season (men's first league 2025/26 = 579, 2026/27 = 608). |
| `team_id` | integer | Team-application id: the team's registration (entry) for the season; play-by-play `teamApplicationId` is the same key space. |
| `team_application_id` | integer | Team-application id: the team's registration (entry) for the season; play-by-play `teamApplicationId` is the same key space. |
| `club_id` | integer | Club id: the club across seasons and categories (Ilirija = 16). No captured route is keyed by it. |
| `club_member` | logical | True when the club is a KZS member (false on nested copies that omit it). |
| `team_internal_ranking` | character | The club's internal team letter for the category (A = first team, B = second team). |
| `category` | character | Age category: ABSOLUTE (senior) or U8-U23 / ANY (code table in kzs-returns.md). |
| `rank` | character | Tier: FIRST-FOURTH or TOURNAMENT (code table). |
| `gender` | character | MALE or FEMALE. |
| `sport_hall_id` | integer | Sport hall (venue) id. /sportHalls/{id} is in the site bundle, not captured. |
| `sport_hall_name` | character | Name of the venue (sports hall) where the game is played or the team is based. |
| `name` | character | Team name for the season (sponsor name included). |
| `code` | character | Team 3-letter code. |
| `home_jersey_color` | character | Home jersey colour, hex '#RRGGBB'. |
| `guest_jersey_color` | character | Away jersey colour, hex. |
| `logo_uuid` | character | Team logo: image file id, served at https://api.kzs.si/public/images/{uuid}. |
| `home_team_color` | character | Home colour, hex. |
| `guest_team_color` | character | Away colour, hex. |
| `short_name` | character | Team short name ('' when none). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#kzs_teams-example}

```python
kzs_teams(competition_id=579)
```

_Last validated n/a._

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
| `team_id` | integer | Team-application id: the team's registration (entry) for the season; play-by-play `teamApplicationId` is the same key space. |
| `team_application_id` | integer | Team-application id: the team's registration (entry) for the season; play-by-play `teamApplicationId` is the same key space. |
| `club_id` | integer | Club id: the club across seasons and categories (Ilirija = 16). No captured route is keyed by it. |
| `club_member` | logical | True when the club is a KZS member (false on nested copies that omit it). |
| `category` | character | Age category: ABSOLUTE (senior) or U8-U23 / ANY (code table in kzs-returns.md). |
| `rank` | character | Tier: FIRST-FOURTH or TOURNAMENT (code table). |
| `gender` | character | MALE or FEMALE. |
| `sport_hall_id` | integer | Sport hall (venue) id. /sportHalls/{id} is in the site bundle, not captured. |
| `sport_hall_name` | character | Name of the venue (sports hall) where the game is played or the team is based. |
| `sport_hall_address_id` | integer | Id of the venue's address record. |
| `sport_hall_address_post_office_id` | integer | Post-office id (FMS internal). |
| `sport_hall_address_post_office_zip_code` | character | Postal code (REDACTED under a club `addresses[]` row; kept for venues). |
| `sport_hall_address_post_office_name` | character | Post-office town. |
| `sport_hall_address_post_office_municipality` | character | Municipality, upper case. |
| `sport_hall_address_post_office_country` | character | Country, ISO 3166-1 alpha-2 (SI). |
| `sport_hall_address_street` | character | Venue street (a public sports hall). |
| `sport_hall_address_house_number` | character | Venue house number, a string ('6 A'). |
| `name` | character | Team name for the season (sponsor name included). |
| `code` | character | Team 3-letter code. |
| `season_id` | integer | Season id: OPAQUE, not a year (1 = 2000/2001 ... 27 = 2026/2027, 31 = 1991/1992). The season `name` is 'START/END'; the sdv season is the END year (the second half of `name`). |
| `season_name` | character | Season, 'START/END' (2025/2026); sdv season = the END year. |
| `season_is_active` | logical | True for the current season only (2026/2027 on 2026-10-12). |
| `season_active` | logical | Same as `isActive`. |
| `short_name` | character | Team short name ('' when none). |
| `home_jersey_color` | character | Home jersey colour, hex '#RRGGBB'. |
| `guest_jersey_color` | character | Away jersey colour, hex. |
| `logo_uuid` | character | Logo: image file id, served at https://api.kzs.si/public/images/{uuid}. |
| `season` | integer | Season of the registration. |
| `competitions` | character | Competitions of the team this season (id, name, rule, phases). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#kzs_team-example}

```python
kzs_team(team_id=17551)
```

_Last validated n/a._

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
| `team_id` | integer | Team id: one club team in one season (joins kzs_teams); not the match-team id and not the club id. |
| `competition_id` | integer | Competition id: one per competition per season (men's first league 2025/26 = 579, 2026/27 = 608). |
| `won` | integer | Wins in the competition (all phases). |
| `lost` | integer | Losses in the competition. |
| `last_games` | character | Recent results, 'W' or 'L' per game (order unverified). |
| `best_points_player_id` | integer | Player id (KZS player registry). Joins to /players/{playerId}, box-score and play-by-play `playerId`. |
| `best_points_player_identification_number` | integer | Player's KZS registration (licence) number; a different key space from the player id, used by no captured route. |
| `best_points_player_person_id` | integer | Person id: the federation's person record behind a player, coach or official; a different key space from player, coach and official ids. |
| `best_points_player_person_first_name` | character | The player's given name. |
| `best_points_player_person_last_name` | character | The player's family name. |
| `best_points_player_person_year_of_birth` | integer | Year of birth (players: public bio). |
| `best_points_player_nationality_id` | integer | Country id (FMS internal; use the ISO codes). |
| `best_points_player_nationality_alpha2_code` | character | ISO 3166-1 alpha-2 country code. |
| `best_points_player_nationality_alpha3_code` | character | ISO 3166-1 alpha-3 country code. |
| `best_points_player_nationality_name` | character | Country name in Slovenian (e.g. 'Združene države' = United States); use `engName` or the ISO codes. |
| `best_points_player_nationality_eng_name` | character | Country name in English. |
| `best_points_player_homegrown` | logical | True when the player counts as homegrown (domači igralec) for roster limits. |
| `best_points_player_photo_uuid` | character | Player photo: image file id, served at https://api.kzs.si/public/images/{uuid}. |
| `best_points_player_positions` | character | Unobserved: null, or an empty array, in every sample, so its type and meaning are unconfirmed. |
| `best_average_points` | numeric | That player's points per game. |
| `best_rebounds_player_id` | integer | Player id (KZS player registry). Joins to /players/{playerId}, box-score and play-by-play `playerId`. |
| `best_rebounds_player_identification_number` | integer | Player's KZS registration (licence) number; a different key space from the player id, used by no captured route. |
| `best_rebounds_player_person_id` | integer | Person id: the federation's person record behind a player, coach or official; a different key space from player, coach and official ids. |
| `best_rebounds_player_person_first_name` | character | The player's given name. |
| `best_rebounds_player_person_last_name` | character | The player's family name. |
| `best_rebounds_player_person_year_of_birth` | integer | Year of birth (players: public bio). |
| `best_rebounds_player_nationality_id` | integer | Country id (FMS internal; use the ISO codes). |
| `best_rebounds_player_nationality_alpha2_code` | character | ISO 3166-1 alpha-2 country code. |
| `best_rebounds_player_nationality_alpha3_code` | character | ISO 3166-1 alpha-3 country code. |
| `best_rebounds_player_nationality_name` | character | Country name in Slovenian (e.g. 'Združene države' = United States); use `engName` or the ISO codes. |
| `best_rebounds_player_nationality_eng_name` | character | Country name in English. |
| `best_rebounds_player_homegrown` | logical | True when the player counts as homegrown (domači igralec) for roster limits. |
| `best_rebounds_player_photo_uuid` | character | Player photo: image file id, served at https://api.kzs.si/public/images/{uuid}. |
| `best_rebounds_player_positions` | character | Unobserved: null, or an empty array, in every sample, so its type and meaning are unconfirmed. |
| `best_average_rebounds` | numeric | That player's rebounds per game. |
| `best_assists_player_id` | integer | Player id (KZS player registry). Joins to /players/{playerId}, box-score and play-by-play `playerId`. |
| `best_assists_player_identification_number` | integer | Player's KZS registration (licence) number; a different key space from the player id, used by no captured route. |
| `best_assists_player_person_id` | integer | Person id: the federation's person record behind a player, coach or official; a different key space from player, coach and official ids. |
| `best_assists_player_person_first_name` | character | The player's given name. |
| `best_assists_player_person_last_name` | character | The player's family name. |
| `best_assists_player_person_year_of_birth` | integer | Year of birth (players: public bio). |
| `best_assists_player_nationality_id` | integer | Country id (FMS internal; use the ISO codes). |
| `best_assists_player_nationality_alpha2_code` | character | ISO 3166-1 alpha-2 country code. |
| `best_assists_player_nationality_alpha3_code` | character | ISO 3166-1 alpha-3 country code. |
| `best_assists_player_nationality_name` | character | Country name in Slovenian (e.g. 'Združene države' = United States); use `engName` or the ISO codes. |
| `best_assists_player_nationality_eng_name` | character | Country name in English. |
| `best_assists_player_homegrown` | logical | True when the player counts as homegrown (domači igralec) for roster limits. |
| `best_assists_player_photo_uuid` | character | Player photo: image file id, served at https://api.kzs.si/public/images/{uuid}. |
| `best_assists_player_positions` | character | Unobserved: null, or an empty array, in every sample, so its type and meaning are unconfirmed. |
| `best_average_assists` | numeric | That player's assists per game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#kzs_team_general-example}

```python
kzs_team_general(competition_id=579, team_id=17551)
```

_Last validated n/a._
