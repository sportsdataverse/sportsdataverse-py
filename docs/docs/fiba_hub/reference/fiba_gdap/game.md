---
title: "FIBA (digital API and game pages) — FIBA digital API (GDAP, digital-api.fiba.basketball) — Game"
sidebar_label: "Game"
sidebar_position: 2
description: "FIBA (digital API and game pages) — FIBA digital API (GDAP, digital-api.fiba.basketball) — Game — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# FIBA (digital API and game pages) — FIBA digital API (GDAP, digital-api.fiba.basketball) — Game

## fiba_gdap_game_officials

Officials nominated to one game (referees, table officials, commissioner).

**Endpoint URL:** `GET https://digital-api.fiba.basketball/hapi/getgdapgamebyid`

**Valid URL:** [https://digital-api.fiba.basketball/hapi/getgdapgamebyid?gdapGameId=100720](https://digital-api.fiba.basketball/hapi/getgdapgamebyid?gdapGameId=100720)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gdapGameId` | `game_id` |  | `Y` |  | Required. FIBA game id (global GDAP key space): gameId of a schedule row; the <gameId> of a game page URL |

### Returns {#fiba_gdap_game_officials-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | FIBA game id (Int64) of the game the official was nominated to. |
| `person_id` | integer | FIBA person id, global across competitions and roles (player, coach, official): roster personId = play-by-play pId = game-page P_<id> = gdapPlayerId. |
| `first_name` | character | Given name(s) as registered with FIBA. |
| `last_name` | character | Last name (officials: upper case). |
| `gender` | character | Sex of the person: M or F (Male/Female on getgdapcompetitionnominationsbyid). |
| `role_code` | character | Role code (REF, FIBATDEL...). |
| `role_name` | character | Role name on the game (Crew chief, Umpire, Commissioner, Assistant scorer...). |
| `role_order` | character | Order within the role; a numeric string or null. |
| `default_nationality` | character | FIBA code of the official's main nationality. |
| `license_nationality` | character | FIBA code of the federation that licenses the official. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fiba_gdap_game_officials-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FIBA has nothing for the request (HTTP 404, or GDAP answered {} or [] for an unknown id). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. HTTP 400 / 422, or a malformed id, season or date. |

### Example {#fiba_gdap_game_officials-example}

```python
fiba_gdap_game_officials(game_id=100720)
```

## fiba_gdap_game_periods

Period scores of one game.

**Endpoint URL:** `GET https://digital-api.fiba.basketball/hapi/getgdapgamebyid`

**Valid URL:** [https://digital-api.fiba.basketball/hapi/getgdapgamebyid?gdapGameId=100720](https://digital-api.fiba.basketball/hapi/getgdapgamebyid?gdapGameId=100720)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gdapGameId` | `game_id` |  | `Y` |  | Required. FIBA game id (global GDAP key space): gameId of a schedule row; the <gameId> of a game page URL |

### Returns {#fiba_gdap_game_periods-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | FIBA game id (Int64) of the game the period belongs to. |
| `name` | character | Period name ('Q1'...'OT1'). |
| `number` | integer | Period number (1-based). |
| `team_a_score` | integer | Team A points in the period. |
| `team_b_score` | integer | Team B points in the period. |
| `team_a_from` | character | How team A qualified for this slot ('Winner of Game 87', '1st of group C'); null in group play. |
| `team_b_from` | character | How team B qualified for this slot ('Winner of Game 88', '2nd of group A'); null in group play. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fiba_gdap_game_periods-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FIBA has nothing for the request (HTTP 404, or GDAP answered {} or [] for an unknown id). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. HTTP 400 / 422, or a malformed id, season or date. |

### Example {#fiba_gdap_game_periods-example}

```python
fiba_gdap_game_periods(game_id=100720)
```

## fiba_gdap_games_between

Games of EVERY FIBA competition in a local-time window (how competitions and games are discovered).

**Endpoint URL:** `GET https://digital-api.fiba.basketball/hapi/getgdapgamesbetweentwodates`

**Valid URL:** [https://digital-api.fiba.basketball/hapi/getgdapgamesbetweentwodates?dateFrom=2023-09-08T00%3A00%3A00Z&dateTo=2023-09-11T00%3A00%3A00Z](https://digital-api.fiba.basketball/hapi/getgdapgamesbetweentwodates?dateFrom=2023-09-08T00%3A00%3A00Z&dateTo=2023-09-11T00%3A00%3A00Z)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `dateFrom` | `date_from` |  | `Y` |  | Required. Window start as YYYY-MM-DD, compared with the venue-local gameDateTime (not UTC). |
| `dateTo` | `date_to` |  | `Y` |  | Required. Window end as YYYY-MM-DD, EXCLUSIVE (date_from = date_to returns nothing). |

### Returns {#fiba_gdap_games_between-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | FIBA game id, global across competitions (GDAP key space): gdapGameId, the <gameId> of a game-page URL. |
| `game_name` | character | Technical game name '<roundId>-<groupPairingCode>-<gameNumber>'. |
| `game_number` | character | Game number within the round or pairing (a string: '1', 'A'). |
| `status_code` | character | Game status code: VALID (validated result), CLOS (closed, pre-2000 rows). |
| `team_a_team_id` | integer | Competition team id: one per team per competition (Germany = 279569 at the 2023 World Cup, 281545 at Paris 2024). Joins getgdapcompetitionteamrosterbyteamid / getgdapcompetitionteamstatisticsbyteamid gdapTeamId; not an organisationId. |
| `team_a_organisation_id` | integer | Organisation id, stable across competitions and seasons: the national federation for a national team (64 = Germany), the club for a club team (396 = Filou Oostende). Same key space as nationalFederationId, clubId, play-by-play oId and the game page's T_<id>. |
| `team_a_code` | character | Team code (national: FIBA country code; club: a club abbreviation such as OOST). |
| `team_a_official_name` | character | Official name of team A: the country for a national team, the club for a club team. |
| `team_a_short_name` | character | Short name of team A: the country code for a national team, the club's short name for a club. |
| `team_b_team_id` | integer | Competition team id: one per team per competition (Germany = 279569 at the 2023 World Cup, 281545 at Paris 2024). Joins getgdapcompetitionteamrosterbyteamid / getgdapcompetitionteamstatisticsbyteamid gdapTeamId; not an organisationId. |
| `team_b_organisation_id` | integer | Organisation id, stable across competitions and seasons: the national federation for a national team (64 = Germany), the club for a club team (396 = Filou Oostende). Same key space as nationalFederationId, clubId, play-by-play oId and the game page's T_<id>. |
| `team_b_code` | character | Team code (national: FIBA country code; club: a club abbreviation such as NANT). |
| `team_b_official_name` | character | Official name of team B: the country for a national team, the club for a club team. |
| `team_b_short_name` | character | Short name of team B: the country code for a national team, the club's short name for a club. |
| `team_a_score` | integer | Final score of team A. |
| `team_b_score` | integer | Final score of team B. |
| `is_live` | logical | True while the game is being played. |
| `current_period` | character | Last period reached ('Q4', 'OT1'). |
| `chrono` | character | Game clock of the current period, 'mm:ss' ('00:00' when finished). |
| `live_game_status` | integer | Live state code: 999 on every finished game; 5 seen on a finished 2024 youth game; mapping unverified. |
| `current_period_status` | character | State of the current period: E = ended (the only value sampled). |
| `host_city` | character | City of the venue. |
| `host_country` | character | Country of the venue (name; a FIBA code in getgdapgameswithleaderdetailsbyteamid). |
| `host_country_code` | character | FIBA code of the venue's country. |
| `venue_id` | integer | Venue id (GDAP key space; 0 = unknown). |
| `venue_name` | character | Name of the arena the game is played in. |
| `game_date_time` | character | Tip-off in venue-local wall-clock time, ISO 8601 without offset (T00:00:00 when hasTimeGameDateTime is false). |
| `game_date_time_utc` | character | Tip-off in UTC, ISO 8601 without the Z. |
| `has_time_game_date_time` | logical | False when only the date is known (the time part is then 00:00:00). |
| `iana_time_zone` | character | IANA time zone of the venue. |
| `utc_offset` | numeric | Venue UTC offset at tip-off, in hours (Float64: a half-hour zone such as India +5.5 or Adelaide +9.5 is a fraction; whole hours in every sample). |
| `is_postponed` | logical | True when the game was postponed. |
| `is_played_behind_closed_doors` | logical | True when played without spectators. |
| `venue_capacity` | integer | Venue capacity (0 = unknown). |
| `spectators` | integer | Attendance at the game, as recorded by FIBA. |
| `statistic_system` | character | Live-stats system that scored the game: OVR (FIBA's Olympic/World Cup system), FLS (FIBA LiveStats), null before about 2006. |
| `group_id` | character | Group id (GDAP key space). |
| `group_pairing_code` | character | Group letter (group play) or pairing number (knockout bracket slot). |
| `round_round_id` | integer | Round id (GDAP key space, unique across competitions); the first segment of gameName. |
| `round_round_code` | character | Round code (GP group phase, QF, SF, F final, RS regular season, CL17-32 classification...). |
| `round_round_name` | character | Round label (Group Phase, Quarter-Finals, Final...). |
| `round_round_number` | integer | Round number within the competition (1-based). |
| `round_round_type` | character | Round type label: Group Round, Non Group Round, Super Round (getgdapgamesbycompetitionid uses the code G/NG here). |
| `round_round_status_code` | character | Round status: COMPL completed, PROGR in progress, CLOS closed. |
| `round_game_system` | character | Pairing system label (Single, Home and Away, Best of 3). |
| `round_game_system_code` | character | Pairing system code: S single game, HA home and away, BOF3 best of three. |
| `round_start_date` | character | Round start, local date-time. |
| `round_end_date` | character | Round end, local date-time (23:59:59.9999999 on the last day). |
| `round_round_kind` | character | Standard Round or Qualifiers. |
| `round_round_order` | integer | Display order of the round. |
| `competition_competition_id` | integer | FIBA competition id (GDAP key space): one per edition (2023 World Cup = 208182, BCL 2024-25 = 208850); gdapCompetitionId. |
| `competition_competition_code` | character | Competition code (WMM men's World Cup, WOLYM, BCL, ELW...). |
| `competition_season` | integer | Season key, three rules: the END year for club competitions (BCL 2024-25 = 2025) and the event year for national-team events (World Cup 2023 = 2023), both equal to the sdv season key; qualifiers carry the TARGET tournament year, not the year played (World Cup 2027 qualifiers played in 2025 = 2027, EuroBasket 2029 pre-qualifiers = 2029), whose sdv mapping is an open Phase B decision. |
| `competition_official_name` | character | Edition title (FIBA Basketball World Cup; Paris 2024 Men's Olympic Basketball Tournament). |
| `competition_start` | character | First day, YYYY-MM-DD. |
| `competition_end` | character | Last day, YYYY-MM-DD. |
| `competition_status` | character | Competition status: In Progress, Completed, Closed. |
| `competition_age_category` | character | Age category (Senior, Under 16...). |
| `competition_gender` | character | Men or Women: the gender of the competition. |
| `competition_fiba_zone` | character | FIBA zone the competition belongs to (World, Europe, Americas...). |
| `competition_zone_information_zone_id` | integer | FIBA zone id (1 = World, 23 = Europe; zone table key space). |
| `competition_zone_information_zone_official_name` | character | Official name of the competition's FIBA zone (FIBA Europe, FIBA Americas, World). |
| `competition_zone_information_zone_code` | character | Zone code (W, E, AF, AM, AS, OC). |
| `competition_zone_information_top_zone_id` | integer | Parent zone id; null for top zones. |
| `competition_zone_information_top_zone_official_name` | character | Parent zone name; always null in the samples, values unverified. |
| `competition_zone_information_top_zone_code` | character | Parent zone code; always null in the samples, values unverified. |
| `competition_competition_type` | character | National Teams or Clubs. |
| `competition_competition_category_is_league` | logical | True for league-format club competitions. |
| `competition_competition_category_age_type` | character | Age type code: S senior, U16. |
| `competition_competition_category_code` | character | Category code (WC World Cup, OLY Olympic Tournament, CCC-1 continental club tier 1). |
| `competition_competition_category_name` | character | Name of the competition's category (FIBA Basketball World Cup, Olympic Basketball Tournament...). |
| `competition_official_flag_item_code` | character | Official-status code: O = official. |
| `competition_official_flag_item_name` | character | Official-status label. |
| `competition_official` | character | Official-status label. |
| `competition_discipline` | character | Discipline (Basketball; 3x3 is a separate platform). |
| `competition_competition_marketing_name` | character | Competition family, shared by every edition; always null in the samples, values unverified. |
| `game_statistic_status_code` | character | Box-score validation status: VALID. |
| `game_play_by_play_status_code` | character | Play-by-play availability flag: VALID or EMPTY. UNRELIABLE: World Cup 2014 and EuroBasket 2017 say EMPTY but their game pages carry full play-by-play; always check the page. |
| `game_result_status_code` | character | Result validation status: VALID. |
| `game_result_additional_information` | character | Free-text result note (forfeit, overtime remark); null when none. |
| `window_code` | character | Qualifier window code; null otherwise. |
| `window_name` | character | Qualifier window name; null otherwise. |
| `team_a_from` | character | How team A qualified for this slot ('Winner of Game 87', '1st of group C'); null in group play. |
| `team_b_from` | character | How team B qualified for this slot ('Winner of Game 88', '2nd of group A'); null in group play. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fiba_gdap_games_between-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FIBA has nothing for the request (HTTP 404, or GDAP answered {} or [] for an unknown id). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. HTTP 400 / 422, or a malformed id, season or date. |

### Example {#fiba_gdap_games_between-example}

```python
fiba_gdap_games_between(date_from='2023-09-08', date_to='2023-09-11')
```
