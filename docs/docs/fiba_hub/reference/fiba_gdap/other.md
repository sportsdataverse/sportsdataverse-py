---
title: "FIBA (digital API and game pages) — FIBA digital API (GDAP, digital-api.fiba.basketball) — Other: awards–round_standings"
sidebar_label: "Other: awards–round_standings"
sidebar_position: 5
description: "FIBA (digital API and game pages) — FIBA digital API (GDAP, digital-api.fiba.basketball) — Other: awards–round_standings — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# FIBA (digital API and game pages) — FIBA digital API (GDAP, digital-api.fiba.basketball) — Other: awards–round_standings

## fiba_gdap_awards

Awards of a competition (MVP, All-Star Five...).

**Endpoint URL:** `GET https://digital-api.fiba.basketball/hapi/getgdapcompetitionawardsbyid`

**Valid URL:** [https://digital-api.fiba.basketball/hapi/getgdapcompetitionawardsbyid?gdapCompetitionId=208182](https://digital-api.fiba.basketball/hapi/getgdapcompetitionawardsbyid?gdapCompetitionId=208182)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gdapCompetitionId` | `comp` |  | `Y` |  | Required. FIBA competition id (GDAP key space), one per edition: competition.competitionId of any game row (208182 = World Cup 2023, 208722 = Paris 2024 men, 208850 = BCL 2024-25). Find it with fiba_gdap_editions or fiba_gdap_games_between. |
| `disciplineFormat` | `discipline_format` |  |  | `Y` | Optional; the site passes its discipline format. 5x5 answers the same as omitting it (verified on awards and rosters) |

### Returns {#fiba_gdap_awards-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `competition_id` | integer | FIBA competition id (GDAP key space): one per edition (2023 World Cup = 208182, BCL 2024-25 = 208850); gdapCompetitionId. |
| `competition_end_date` | character | Last day of the competition, YYYY-MM-DD. |
| `award_date` | character | Date awarded, YYYY-MM-DD. |
| `competition_season` | integer | Season key, three rules: the END year for club competitions (BCL 2024-25 = 2025) and the event year for national-team events (World Cup 2023 = 2023), both equal to the sdv season key; qualifiers carry the TARGET tournament year, not the year played (World Cup 2027 qualifiers played in 2025 = 2027, EuroBasket 2029 pre-qualifiers = 2029), whose sdv mapping is an open Phase B decision. |
| `competition_official_name` | character | Official name of the competition edition the award was given in. |
| `award_name` | character | Award name (All-Star 5, MVP...). |
| `award_code` | character | Award code (ALLSTAR, MVP...). |
| `award_level_id` | integer | Id of the object the award is attached to: the competitionId when awardLevel is Competition. |
| `award_level` | character | Level the award is given at: Competition. |
| `award_label` | character | Extra label; null in every sample. |
| `player_id` | integer | FIBA person id, global across competitions and roles (player, coach, official): roster personId = play-by-play pId = game-page P_<id> = gdapPlayerId. |
| `player_first_name` | character | Awarded player's first name. |
| `player_last_name` | character | Awarded player's last name. |
| `player_date_of_birth` | character | Awarded player's date of birth, YYYY-MM-DD. |
| `player_nationality` | character | Awarded player's nationality, FIBA code. |
| `teams` | character | Teams the awarded player played for in the competition. Stored as a JSON string. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fiba_gdap_awards-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FIBA has nothing for the request (HTTP 404, or GDAP answered {} or [] for an unknown id). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. HTTP 400 / 422, or a malformed id, season or date. |

### Example {#fiba_gdap_awards-example}

```python
fiba_gdap_awards(comp=208182)
```

## fiba_gdap_competition

One competition (edition): season, dates, hosts, zone, category and competition family.

**Endpoint URL:** `GET https://digital-api.fiba.basketball/hapi/getGdapCompetitionById`

**Valid URL:** [https://digital-api.fiba.basketball/hapi/getGdapCompetitionById?gdapCompetitionId=208182](https://digital-api.fiba.basketball/hapi/getGdapCompetitionById?gdapCompetitionId=208182)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gdapCompetitionId` | `comp` |  | `Y` |  | Required. FIBA competition id (GDAP key space), one per edition: competition.competitionId of any game row (208182 = World Cup 2023, 208722 = Paris 2024 men, 208850 = BCL 2024-25). Find it with fiba_gdap_editions or fiba_gdap_games_between. |

### Returns {#fiba_gdap_competition-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `hosts` | character | Host countries with their cities. Stored as a JSON string. |
| `competition_id` | integer | FIBA competition id (GDAP key space): one per edition (2023 World Cup = 208182, BCL 2024-25 = 208850); gdapCompetitionId. |
| `competition_code` | character | Competition code (WMM men's World Cup, WOLYM, BCL, ELW...). |
| `season` | integer | Season key, three rules: the END year for club competitions (BCL 2024-25 = 2025) and the event year for national-team events (World Cup 2023 = 2023), both equal to the sdv season key; qualifiers carry the TARGET tournament year, not the year played (World Cup 2027 qualifiers played in 2025 = 2027, EuroBasket 2029 pre-qualifiers = 2029), whose sdv mapping is an open Phase B decision. |
| `official_name` | character | Edition title (FIBA Basketball World Cup; Paris 2024 Men's Olympic Basketball Tournament). |
| `start` | character | First day, YYYY-MM-DD. |
| `end` | character | Last day, YYYY-MM-DD. |
| `status` | character | Competition status: In Progress, Completed, Closed. |
| `age_category` | character | Age category (Senior, Under 16...). |
| `gender` | character | Men or Women: the gender of the competition. |
| `fiba_zone` | character | FIBA zone the competition belongs to (World, Europe, Americas...). |
| `zone_information_zone_id` | integer | FIBA zone id (1 = World, 23 = Europe; zone table key space). |
| `zone_information_zone_official_name` | character | Official name of the competition's FIBA zone (FIBA Europe, FIBA Americas, World). |
| `zone_information_zone_code` | character | Zone code (W, E, AF, AM, AS, OC). |
| `zone_information_top_zone_id` | integer | Parent zone id; null for top zones. |
| `zone_information_top_zone_official_name` | character | Parent zone name; always null in the samples, values unverified. |
| `zone_information_top_zone_code` | character | Parent zone code; always null in the samples, values unverified. |
| `competition_type` | character | National Teams or Clubs. |
| `competition_category_is_league` | logical | True for league-format club competitions. |
| `competition_category_age_type` | character | Age type code: S senior, U16. |
| `competition_category_code` | character | Category code (WC World Cup, OLY Olympic Tournament, CCC-1 continental club tier 1). |
| `competition_category_name` | character | Name of the competition's category (FIBA Basketball World Cup, Olympic Basketball Tournament...). |
| `official_flag_item_code` | character | Official-status code: O = official. |
| `official_flag_item_name` | character | Official-status label. |
| `official` | character | Official-status label. |
| `discipline` | character | Discipline (Basketball; 3x3 is a separate platform). |
| `competition_marketing_name_competition_marketing_name_id` | integer | Competition family id (201 = World Cup, 112 = BCL tier 1, 211 = EuroLeague Women); the <histId> of /en/history/ pages. |
| `competition_marketing_name_marketing_name` | character | Competition family name; slugified it is the <histSlug> of /en/history/ pages. |
| `competition_marketing_name_code` | character | Family code; null in every sample. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fiba_gdap_competition-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FIBA has nothing for the request (HTTP 404, or GDAP answered {} or [] for an unknown id). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. HTTP 400 / 422, or a malformed id, season or date. |

### Example {#fiba_gdap_competition-example}

```python
fiba_gdap_competition(comp=208182)
```

## fiba_gdap_editions

Every edition of a competition family between two seasons.

**Endpoint URL:** `GET https://digital-api.fiba.basketball/hapi/getgdapcompetitionsbyseasonsandcompetitionmarketingname`

**Valid URL:** [https://digital-api.fiba.basketball/hapi/getgdapcompetitionsbyseasonsandcompetitionmarketingname?competitionMarketingNameId=201&fromSeason=2023](https://digital-api.fiba.basketball/hapi/getgdapcompetitionsbyseasonsandcompetitionmarketingname?competitionMarketingNameId=201&fromSeason=2023)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competitionMarketingNameId` | `family_id` |  | `Y` |  | Required. Competition family id (competitionMarketingName.competitionMarketingNameId): 201 = World Cup, 320 = men's Olympics, 306 = Women's World Cup, 112 = BCL tier 1, 211 = EuroLeague Women. |
| `fromSeason` | `season` |  |  | `Y` | Season key: the END year for club competitions (BCL 2024-25 = 2025), the event year for national-team events (World Cup 2023 = 2023); qualifiers carry the TARGET tournament year (World Cup 2027 qualifiers played in 2025 = 2027). Sent unchanged. First season, inclusive; omitted = no lower bound. |
| `toSeason` | `to_season` |  |  | `Y` | Last season, inclusive (same key as season). Omitted with season given = the same season. |

### Returns {#fiba_gdap_editions-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `hosts` | character | Host countries with their cities. Stored as a JSON string. |
| `competition_id` | integer | FIBA competition id (GDAP key space): one per edition (2023 World Cup = 208182, BCL 2024-25 = 208850); gdapCompetitionId. |
| `competition_code` | character | Competition code (WMM men's World Cup, WOLYM, BCL, ELW...). |
| `season` | integer | Season key, three rules: the END year for club competitions (BCL 2024-25 = 2025) and the event year for national-team events (World Cup 2023 = 2023), both equal to the sdv season key; qualifiers carry the TARGET tournament year, not the year played (World Cup 2027 qualifiers played in 2025 = 2027, EuroBasket 2029 pre-qualifiers = 2029), whose sdv mapping is an open Phase B decision. |
| `official_name` | character | Edition title (FIBA Basketball World Cup; Paris 2024 Men's Olympic Basketball Tournament). |
| `start` | character | First day, YYYY-MM-DD. |
| `end` | character | Last day, YYYY-MM-DD. |
| `status` | character | Competition status: In Progress, Completed, Closed. |
| `age_category` | character | Age category (Senior, Under 16...). |
| `gender` | character | Men or Women: the gender of the competition. |
| `fiba_zone` | character | FIBA zone the competition belongs to (World, Europe, Americas...). |
| `zone_information_zone_id` | integer | FIBA zone id (1 = World, 23 = Europe; zone table key space). |
| `zone_information_zone_official_name` | character | Official name of the competition's FIBA zone (FIBA Europe, FIBA Americas, World). |
| `zone_information_zone_code` | character | Zone code (W, E, AF, AM, AS, OC). |
| `zone_information_top_zone_id` | integer | Parent zone id; null for top zones. |
| `zone_information_top_zone_official_name` | character | Parent zone name; always null in the samples, values unverified. |
| `zone_information_top_zone_code` | character | Parent zone code; always null in the samples, values unverified. |
| `competition_type` | character | National Teams or Clubs. |
| `competition_category_is_league` | logical | True for league-format club competitions. |
| `competition_category_age_type` | character | Age type code: S senior, U16. |
| `competition_category_code` | character | Category code (WC World Cup, OLY Olympic Tournament, CCC-1 continental club tier 1). |
| `competition_category_name` | character | Name of the competition's category (FIBA Basketball World Cup, Olympic Basketball Tournament...). |
| `official_flag_item_code` | character | Official-status code: O = official. |
| `official_flag_item_name` | character | Official-status label. |
| `official` | character | Official-status label. |
| `discipline` | character | Discipline (Basketball; 3x3 is a separate platform). |
| `competition_marketing_name_competition_marketing_name_id` | integer | Competition family id (201 = World Cup, 112 = BCL tier 1, 211 = EuroLeague Women); the <histId> of /en/history/ pages. |
| `competition_marketing_name_marketing_name` | character | Competition family name; slugified it is the <histSlug> of /en/history/ pages. |
| `competition_marketing_name_code` | character | Family code; null in every sample. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fiba_gdap_editions-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FIBA has nothing for the request (HTTP 404, or GDAP answered {} or [] for an unknown id). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. HTTP 400 / 422, or a malformed id, season or date. |

### Example {#fiba_gdap_editions-example}

```python
fiba_gdap_editions(family_id=201, season=2023)
```

## fiba_gdap_final_standings

Final ranking of a competition.

**Endpoint URL:** `GET https://digital-api.fiba.basketball/hapi/getgdapcompetitionfinalstandingbycompetitionid`

**Valid URL:** [https://digital-api.fiba.basketball/hapi/getgdapcompetitionfinalstandingbycompetitionid?profile=true&gdapCompetitionId=208182](https://digital-api.fiba.basketball/hapi/getgdapcompetitionfinalstandingbycompetitionid?profile=true&gdapCompetitionId=208182)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gdapCompetitionId` | `comp` |  | `Y` |  | Required. FIBA competition id (GDAP key space), one per edition: competition.competitionId of any game row (208182 = World Cup 2023, 208722 = Paris 2024 men, 208850 = BCL 2024-25). Find it with fiba_gdap_editions or fiba_gdap_games_between. |

### Returns {#fiba_gdap_final_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `competition_id` | integer | FIBA competition id (Int64) the final ranking belongs to; the comp argument. |
| `final_rank` | integer | Final rank in the competition. |
| `team_team_id` | integer | Competition team id: one per team per competition (Germany = 279569 at the 2023 World Cup, 281545 at Paris 2024). Joins getgdapcompetitionteamrosterbyteamid / getgdapcompetitionteamstatisticsbyteamid gdapTeamId; not an organisationId. |
| `team_team_code` | character | Team abbreviation (FIBA country code or club abbreviation). |
| `team_short_name` | character | Short team name: the country code for a national team, the club's short name for a club. |
| `team_profile` | character | Team profile; null in this route. |
| `team_status_code` | character | Team entry status; always null in the samples, values unverified. |
| `team_status_name` | character | Team entry status label; always null in the samples, values unverified. |
| `team_organisation_id` | integer | Organisation id, stable across competitions and seasons: the national federation for a national team (64 = Germany), the club for a club team (396 = Filou Oostende). Same key space as nationalFederationId, clubId, play-by-play oId and the game page's T_<id>. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fiba_gdap_final_standings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FIBA has nothing for the request (HTTP 404, or GDAP answered {} or [] for an unknown id). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. HTTP 400 / 422, or a malformed id, season or date. |

### Example {#fiba_gdap_final_standings-example}

```python
fiba_gdap_final_standings(comp=208182)
```

## fiba_gdap_game

One game's header: teams, score, venue, attendance and status codes (no box score or play-by-play: see fiba_gamepage).

**Endpoint URL:** `GET https://digital-api.fiba.basketball/hapi/getgdapgamebyid`

**Valid URL:** [https://digital-api.fiba.basketball/hapi/getgdapgamebyid?gdapGameId=100720](https://digital-api.fiba.basketball/hapi/getgdapgamebyid?gdapGameId=100720)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gdapGameId` | `game_id` |  | `Y` |  | Required. FIBA game id (global GDAP key space): gameId of a schedule row; the <gameId> of a game page URL |

### Returns {#fiba_gdap_game-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | FIBA game id, global across competitions (GDAP key space): gdapGameId, the <gameId> of a game-page URL. |
| `round_name` | character | Round label (Group Phase, Quarter-Finals, Final...). |
| `technical_name` | character | Technical game name '<roundId>-<groupPairingCode>-<gameNumber>'. |
| `round_type` | character | Round type label: Group Round, Non Group Round, Super Round (getgdapgamesbycompetitionid uses the code G/NG here). |
| `group_pairing_code` | character | Group letter (group play) or pairing number (knockout bracket slot). |
| `game_number` | character | Game number within the round or pairing (a string: '1', 'A'). |
| `status_code` | character | Game status code: VALID (validated result), CLOS (closed, pre-2000 rows). |
| `game_day` | integer | Game day number within the round. |
| `team_a_from` | character | How team A qualified for this slot ('Winner of Game 87', '1st of group C'); null in group play. |
| `team_b_from` | character | How team B qualified for this slot ('Winner of Game 88', '2nd of group A'); null in group play. |
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
| `team_a_aggregated_score` | integer | Two-leg aggregate score of team A (0 when not a two-leg tie). |
| `team_b_aggregated_score` | integer | Two-leg aggregate score of team B (0 when not a two-leg tie). |
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
| `is_live` | logical | True while the game is being played. |
| `current_period` | character | Last period reached ('Q4', 'OT1'). |
| `chrono` | character | Game clock of the current period, 'mm:ss' ('00:00' when finished). |
| `live_game_status` | integer | Live state code: 999 on every finished game; 5 seen on a finished 2024 youth game; mapping unverified. |
| `current_period_status` | character | State of the current period: E = ended (the only value sampled). |
| `game_statistic_status_code` | character | Box-score validation status: VALID. |
| `game_play_by_play_status_code` | character | Play-by-play availability flag: VALID or EMPTY. UNRELIABLE: World Cup 2014 and EuroBasket 2017 say EMPTY but their game pages carry full play-by-play; always check the page. |
| `game_result_status_code` | character | Result validation status: VALID. |
| `game_result_additional_information` | character | Free-text result note (forfeit, overtime remark); null when none. |
| `window_id` | integer | Qualifier window id; null otherwise. |
| `window_code` | character | Qualifier window code; null otherwise. |
| `window_name` | character | Qualifier window name; null otherwise. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fiba_gdap_game-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FIBA has nothing for the request (HTTP 404, or GDAP answered {} or [] for an unknown id). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. HTTP 400 / 422, or a malformed id, season or date. |

### Example {#fiba_gdap_game-example}

```python
fiba_gdap_game(game_id=100720)
```

## fiba_gdap_gateway_competition_ids

Ids of the competitions that run in a date window.

**Endpoint URL:** `GET https://digital-api.fiba.basketball/hapi/getcustomgateway`

**Valid URL:** [https://digital-api.fiba.basketball/hapi/getcustomgateway?fibaFunction=competitionIdsBetweenTwoDates&startDate=2025-01-14T00%3A00%3A00Z&endDate=2025-01-16T00%3A00%3A00Z](https://digital-api.fiba.basketball/hapi/getcustomgateway?fibaFunction=competitionIdsBetweenTwoDates&startDate=2025-01-14T00%3A00%3A00Z&endDate=2025-01-16T00%3A00%3A00Z)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `startDate` | `start_date` |  | `Y` |  | Required. Window start as YYYY-MM-DD. |
| `endDate` | `end_date` |  | `Y` |  | Required. Window end as YYYY-MM-DD. |

### Returns {#fiba_gdap_gateway_competition_ids-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `competition_id` | integer | FIBA competition id (Int64, GDAP key space) of a competition that runs in the window. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fiba_gdap_gateway_competition_ids-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FIBA has nothing for the request (HTTP 404, or GDAP answered {} or [] for an unknown id). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. HTTP 400 / 422, or a malformed id, season or date. |

### Example {#fiba_gdap_gateway_competition_ids-example}

```python
fiba_gdap_gateway_competition_ids(end_date='2025-01-16', start_date='2025-01-14')
```

## fiba_gdap_gateway_federation_ranking

FIBA world ranking from the server-side gateway (same items as fiba_gdap_federation_ranking).

**Endpoint URL:** `GET https://digital-api.fiba.basketball/hapi/getcustomgateway`

**Valid URL:** [https://digital-api.fiba.basketball/hapi/getcustomgateway?fibaFunction=federationRanking&gender=men&zoneId=1&asOfDate=2025-09-15](https://digital-api.fiba.basketball/hapi/getcustomgateway?fibaFunction=federationRanking&gender=men&zoneId=1&asOfDate=2025-09-15)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gender` | `gender` |  |  | `Y` | federationRanking: men or women (= gdapCategory) |
| `zoneId` | `zone_id` |  |  | `Y` | federationRanking: ranking zone id (= zoneRankingId) |
| `asOfDate` | `as_of_date` |  |  | `Y` | Ranking as of this date, ISO 8601: the latest ranking published on or before it (2026-03-10 answers the 2026-03-03 ranking). Omitted = the latest ranking |

### Returns {#fiba_gdap_gateway_federation_ranking-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `as_of_date` | character | Date of the ranking returned: the latest ranking published on or before the requested date. |
| `zone_ranking_code` | character | Ranking zone code (WORLD, AFRICA...). |
| `zone_ranking_official_name` | character | Ranking zone official name. |
| `world_rank` | integer | Position in the FIBA world ranking (1 = best). |
| `country_name` | character | Country name in English. |
| `zone_rank` | integer | Rank within the zone. |
| `ioc_code` | character | IOC country code. |
| `fiba_code` | character | FIBA three-letter country code (GER, USA; historic URS, YUG). |
| `current_points` | numeric | Ranking points (one decimal). |
| `world_rank_variation` | integer | Change in world rank since the previous release (positive = up). |
| `comments` | character | Ranking note; null in every sample. |
| `organisation_id` | integer | Organisation id, stable across competitions and seasons: the national federation for a national team (64 = Germany), the club for a club team (396 = Filou Oostende). Same key space as nationalFederationId, clubId, play-by-play oId and the game page's T_<id>. |
| `organisation_status_code` | character | Organisation status: ACT active. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fiba_gdap_gateway_federation_ranking-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FIBA has nothing for the request (HTTP 404, or GDAP answered {} or [] for an unknown id). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. HTTP 400 / 422, or a malformed id, season or date. |

### Example {#fiba_gdap_gateway_federation_ranking-example}

```python
fiba_gdap_gateway_federation_ranking(as_of_date='2025-09-15', gender='men', zone_id=1)
```

## fiba_gdap_medal_count

All-time medal table of a competition family.

**Endpoint URL:** `GET https://digital-api.fiba.basketball/hapi/getgdapmedalcount`

**Valid URL:** [https://digital-api.fiba.basketball/hapi/getgdapmedalcount?competitionMarketingNameId=201](https://digital-api.fiba.basketball/hapi/getgdapmedalcount?competitionMarketingNameId=201)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competitionMarketingNameId` | `family_id` |  | `Y` |  | Required. Competition family id (competitionMarketingName.competitionMarketingNameId): 201 = World Cup, 320 = men's Olympics, 306 = Women's World Cup, 112 = BCL tier 1, 211 = EuroLeague Women. |

### Returns {#fiba_gdap_medal_count-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_name` | character | Team name (country or club). |
| `organisation_id` | integer | Organisation id, stable across competitions and seasons: the national federation for a national team (64 = Germany), the club for a club team (396 = Filou Oostende). Same key space as nationalFederationId, clubId, play-by-play oId and the game page's T_<id>. |
| `fiba_code` | character | FIBA three-letter country code (GER, USA; historic URS, YUG). |
| `team_zone_code` | character | Zone code of the team (E Europe, NAM Americas north...). |
| `rank` | integer | Rank (1 = first). |
| `total_first_rank` | integer | Gold medals (first places). |
| `total_second_rank` | integer | Number of silver medals the federation has won in the competition family. |
| `total_third_rank` | integer | Number of bronze medals the federation has won in the competition family. |
| `total_participations` | integer | Editions played. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fiba_gdap_medal_count-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FIBA has nothing for the request (HTTP 404, or GDAP answered {} or [] for an unknown id). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. HTTP 400 / 422, or a malformed id, season or date. |

### Example {#fiba_gdap_medal_count-example}

```python
fiba_gdap_medal_count(family_id=201)
```

## fiba_gdap_national_teams

Every national federation FIBA lists.

**Endpoint URL:** `GET https://digital-api.fiba.basketball/hapi/getgdapnationalteams`

**Valid URL:** [https://digital-api.fiba.basketball/hapi/getgdapnationalteams](https://digital-api.fiba.basketball/hapi/getgdapnationalteams)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#fiba_gdap_national_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `national_federation_id` | integer | Federation organisation id (organisationId key space: 64 = Germany). |
| `display_name` | character | English display name of the federation (the country), as the FIBA site shows it. |
| `official_name` | character | Federation's official name. |
| `country_id` | integer | FIBA country id (country table key space; not an organisationId). |
| `country_name` | character | Country name in English. |
| `country_fiba_code` | character | FIBA code of the federation's country. |
| `zone_id` | integer | FIBA zone id (1 = World, 23 = Europe; zone table key space). |
| `zone_name` | character | FIBA zone (Europe, Africa...). |
| `organisation_status_code` | character | Organisation status: ACT active. |
| `membership_status_code` | character | FIBA membership status: ACT active. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fiba_gdap_national_teams-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FIBA has nothing for the request (HTTP 404, or GDAP answered {} or [] for an unknown id). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. HTTP 400 / 422, or a malformed id, season or date. |

### Example {#fiba_gdap_national_teams-example}

```python
fiba_gdap_national_teams()
```

## fiba_gdap_nominations

Officials nominated for a competition.

**Endpoint URL:** `GET https://digital-api.fiba.basketball/hapi/getgdapcompetitionnominationsbyid`

**Valid URL:** [https://digital-api.fiba.basketball/hapi/getgdapcompetitionnominationsbyid?gdapCompetitionId=208182&level=competition](https://digital-api.fiba.basketball/hapi/getgdapcompetitionnominationsbyid?gdapCompetitionId=208182&level=competition)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gdapCompetitionId` | `comp` |  | `Y` |  | Required. FIBA competition id (GDAP key space), one per edition: competition.competitionId of any game row (208182 = World Cup 2023, 208722 = Paris 2024 men, 208850 = BCL 2024-25). Find it with fiba_gdap_editions or fiba_gdap_games_between. |
| `level` | `level` |  |  | `Y` | competition = officials nominated for the whole competition (the only value the site sends) |

### Returns {#fiba_gdap_nominations-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `competition_level` | character | Level the nomination is attached to: Competition. |
| `competition_level_id` | integer | Id of the object the nomination is attached to: the competitionId when competitionLevel is Competition. |
| `competition_role` | character | Role label at competition level; null in every sample. |
| `role` | character | Role: Referee, FIBA Table Officials, FIBA Certified Statisticians, FIBA Technical Delegate, FIBA Referee Instructor. |
| `role_code` | character | Role code (REF, FIBATDEL...). |
| `game_assignment_role` | character | Role on a game assignment; null at competition level. |
| `game_assignment_role_code` | character | Code of gameAssignmentRole; null at competition level. |
| `game_assignment_role_order` | character | Order of gameAssignmentRole; null at competition level. |
| `person_id` | integer | FIBA person id, global across competitions and roles (player, coach, official): roster personId = play-by-play pId = game-page P_<id> = gdapPlayerId. |
| `first_name` | character | Given name(s) as registered with FIBA. |
| `last_name` | character | Last name (officials: upper case). |
| `gender` | character | Sex of the person: M or F (Male/Female on getgdapcompetitionnominationsbyid). |
| `nationalities` | character | Nationalities (FIBA codes on player info; country objects on rosters and officials). Stored as a JSON string. |
| `license_nationality` | character | FIBA code of the federation that licenses the official. |
| `affiliated_national_federation_country_code` | character | FIBA code of the federation the official is affiliated to. |
| `affiliated_national_federation_country_name` | character | Country of the federation the official is affiliated to. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fiba_gdap_nominations-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FIBA has nothing for the request (HTTP 404, or GDAP answered {} or [] for an unknown id). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. HTTP 400 / 422, or a malformed id, season or date. |

### Example {#fiba_gdap_nominations-example}

```python
fiba_gdap_nominations(comp=208182)
```

## fiba_gdap_ranking_zones

FIBA world-ranking zones.

**Endpoint URL:** `GET https://digital-api.fiba.basketball/hapi/getgdaprankingzones`

**Valid URL:** [https://digital-api.fiba.basketball/hapi/getgdaprankingzones](https://digital-api.fiba.basketball/hapi/getgdaprankingzones)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#fiba_gdap_ranking_zones-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `zone_ranking_id` | integer | Ranking zone id (getgdaprankingzones key space: 1 = World ... 5 = Europe). |
| `code` | character | Ranking zone code (W, AF, AM, AS, EU). |
| `official_name` | character | Official name of the ranking zone (World, FIBA Europe, FIBA Americas...). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fiba_gdap_ranking_zones-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FIBA has nothing for the request (HTTP 404, or GDAP answered {} or [] for an unknown id). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. HTTP 400 / 422, or a malformed id, season or date. |

### Example {#fiba_gdap_ranking_zones-example}

```python
fiba_gdap_ranking_zones()
```

## fiba_gdap_roster

A team's roster at a competition.

**Endpoint URL:** `GET https://digital-api.fiba.basketball/hapi/getgdapcompetitionteamrosterbyteamid`

**Valid URL:** [https://digital-api.fiba.basketball/hapi/getgdapcompetitionteamrosterbyteamid?gdapTeamId=279569](https://digital-api.fiba.basketball/hapi/getgdapcompetitionteamrosterbyteamid?gdapTeamId=279569)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gdapTeamId` | `team_id` |  | `Y` |  | Required. Competition team id: one per team per competition (teamA.teamId of a game row; Germany is 279569 at the 2023 World Cup and 281545 at Paris 2024). Not the federation organisationId |
| `disciplineFormat` | `discipline_format` |  |  | `Y` | Optional; the site passes its discipline format. 5x5 answers the same as omitting it (verified on awards and rosters) |

### Returns {#fiba_gdap_roster-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | integer | Competition team id: one per team per competition (Germany = 279569 at the 2023 World Cup, 281545 at Paris 2024). Joins getgdapcompetitionteamrosterbyteamid / getgdapcompetitionteamstatisticsbyteamid gdapTeamId; not an organisationId. |
| `team_code` | character | Team code (FIBA country code or club abbreviation). |
| `team_name` | character | Team name (the country for a national team, the club for a club team). |
| `organisation_id` | integer | Stable federation or club id (Int64), the same in every competition and season (64 = Germany). |
| `competition_id` | integer | FIBA competition id (GDAP key space): one per edition (2023 World Cup = 208182, BCL 2024-25 = 208850); gdapCompetitionId. |
| `person_id` | integer | FIBA person id, global across competitions and roles (player, coach, official): roster personId = play-by-play pId = game-page P_<id> = gdapPlayerId. |
| `first_name` | character | Given name(s) as registered with FIBA. |
| `last_name` | character | Last name (officials: upper case). |
| `gender` | character | Sex of the person: M or F (Male/Female on getgdapcompetitionnominationsbyid). |
| `date_of_birth` | character | Date of birth, YYYY-MM-DD. |
| `uniform_number` | character | Jersey number (a string). |
| `uniform_name` | character | Name printed on the jersey. |
| `position` | character | Playing position (roster: Point Guard...; game page: PG, SG, SF, PF, C). |
| `height_in_cm` | integer | Player height in centimetres. |
| `height_in_feet_inches` | character | Height, feet and inches string (6'7"). |
| `nationality` | character | FIBA code of the main nationality. |
| `is_on_final_roster` | logical | True when on the final (12-player) roster. |
| `games_played` | integer | Games played in the competition. |
| `club_id` | integer | Organisation id of the player's club (organisationId key space). |
| `club_name` | character | Club at the time of the competition. |
| `country_fiba_code` | character | FIBA code of the country the person represents. |
| `club_country_fiba_code` | character | FIBA code of the club's country. |
| `is_captain` | logical | True for the team captain. |
| `is_player_free_agent` | logical | True when the player had no club. |
| `final_roster_member_status_code` | character | Roster entry status: VALID, PARTIAL (incomplete registration). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fiba_gdap_roster-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FIBA has nothing for the request (HTTP 404, or GDAP answered {} or [] for an unknown id). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. HTTP 400 / 422, or a malformed id, season or date. |

### Example {#fiba_gdap_roster-example}

```python
fiba_gdap_roster(team_id=279569)
```

## fiba_gdap_roster_coaches

A team's coaches on its roster at a competition.

**Endpoint URL:** `GET https://digital-api.fiba.basketball/hapi/getgdapcompetitionteamrosterbyteamid`

**Valid URL:** [https://digital-api.fiba.basketball/hapi/getgdapcompetitionteamrosterbyteamid?gdapTeamId=279569](https://digital-api.fiba.basketball/hapi/getgdapcompetitionteamrosterbyteamid?gdapTeamId=279569)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gdapTeamId` | `team_id` |  | `Y` |  | Required. Competition team id: one per team per competition (teamA.teamId of a game row; Germany is 279569 at the 2023 World Cup and 281545 at Paris 2024). Not the federation organisationId |
| `disciplineFormat` | `discipline_format` |  |  | `Y` | Optional; the site passes its discipline format. 5x5 answers the same as omitting it (verified on awards and rosters) |

### Returns {#fiba_gdap_roster_coaches-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | integer | Competition team id: one per team per competition (Germany = 279569 at the 2023 World Cup, 281545 at Paris 2024). Joins getgdapcompetitionteamrosterbyteamid / getgdapcompetitionteamstatisticsbyteamid gdapTeamId; not an organisationId. |
| `team_code` | character | Team code (FIBA country code or club abbreviation). |
| `team_name` | character | Team name (the country for a national team, the club for a club team). |
| `organisation_id` | integer | Stable federation or club id (Int64), the same in every competition and season (64 = Germany). |
| `competition_id` | integer | FIBA competition id (GDAP key space): one per edition (2023 World Cup = 208182, BCL 2024-25 = 208850); gdapCompetitionId. |
| `person_id` | integer | FIBA person id, global across competitions and roles (player, coach, official): roster personId = play-by-play pId = game-page P_<id> = gdapPlayerId. |
| `first_name` | character | Given name(s) as registered with FIBA. |
| `last_name` | character | Last name (officials: upper case). |
| `gender` | character | Sex of the person: M or F (Male/Female on getgdapcompetitionnominationsbyid). |
| `function` | character | Role: Head Coach, Coach, Assistant Coach. |
| `is_head_coach` | integer | 1 for the head coach, 0 otherwise (an integer flag, not a boolean). |
| `is_assistant_coach` | integer | 1 for an assistant coach, else null (integer flag). |
| `is_accompanying_coach` | logical | Accompanying-coach flag; always null in the samples, values unverified. |
| `coach_display_order` | integer | Display order; null when unset. |
| `is_on_final_roster` | logical | True when on the final (12-player) roster. |
| `nationalities` | character | Nationalities (FIBA codes on player info; country objects on rosters and officials). Stored as a JSON string. |
| `coach_license_organisation_name` | character | Federation that issued the coaching licence. |
| `coach_license_country_code` | character | FIBA code of the licensing federation's country. |
| `final_roster_member_status_code` | character | Roster entry status: VALID, PARTIAL (incomplete registration). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fiba_gdap_roster_coaches-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FIBA has nothing for the request (HTTP 404, or GDAP answered {} or [] for an unknown id). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. HTTP 400 / 422, or a malformed id, season or date. |

### Example {#fiba_gdap_roster_coaches-example}

```python
fiba_gdap_roster_coaches(team_id=279569)
```

## fiba_gdap_roster_latest

A team's latest roster (same shape as the roster).

**Endpoint URL:** `GET https://digital-api.fiba.basketball/hapi/getgdapcompetitionteamlatestrosterbyteamid`

**Valid URL:** [https://digital-api.fiba.basketball/hapi/getgdapcompetitionteamlatestrosterbyteamid?gdapTeamId=282183](https://digital-api.fiba.basketball/hapi/getgdapcompetitionteamlatestrosterbyteamid?gdapTeamId=282183)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gdapTeamId` | `team_id` |  | `Y` |  | Required. Competition team id: one per team per competition (teamA.teamId of a game row; Germany is 279569 at the 2023 World Cup and 281545 at Paris 2024). Not the federation organisationId |

### Returns {#fiba_gdap_roster_latest-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | integer | Competition team id: one per team per competition (Germany = 279569 at the 2023 World Cup, 281545 at Paris 2024). Joins getgdapcompetitionteamrosterbyteamid / getgdapcompetitionteamstatisticsbyteamid gdapTeamId; not an organisationId. |
| `team_code` | character | Team code (FIBA country code or club abbreviation). |
| `team_name` | character | Team name (the country for a national team, the club for a club team). |
| `organisation_id` | integer | Stable federation or club id (Int64), the same in every competition and season (64 = Germany). |
| `competition_id` | integer | FIBA competition id (GDAP key space): one per edition (2023 World Cup = 208182, BCL 2024-25 = 208850); gdapCompetitionId. |
| `person_id` | integer | FIBA person id, global across competitions and roles (player, coach, official): roster personId = play-by-play pId = game-page P_<id> = gdapPlayerId. |
| `first_name` | character | Given name(s) as registered with FIBA. |
| `last_name` | character | Last name (officials: upper case). |
| `gender` | character | Sex of the person: M or F (Male/Female on getgdapcompetitionnominationsbyid). |
| `date_of_birth` | character | Date of birth, YYYY-MM-DD. |
| `uniform_number` | character | Jersey number (a string). |
| `uniform_name` | character | Name printed on the jersey. |
| `position` | character | Playing position (roster: Point Guard...; game page: PG, SG, SF, PF, C). |
| `height_in_cm` | integer | Player height in centimetres. |
| `height_in_feet_inches` | character | Height, feet and inches string (6'7"). |
| `nationality` | character | FIBA code of the main nationality. |
| `is_on_final_roster` | logical | True when on the final (12-player) roster. |
| `games_played` | integer | Games played in the competition. |
| `club_id` | integer | Organisation id of the player's club (organisationId key space). |
| `club_name` | character | Club at the time of the competition. |
| `country_fiba_code` | character | FIBA code of the country the person represents. |
| `club_country_fiba_code` | character | FIBA code of the club's country. |
| `is_captain` | logical | True for the team captain. |
| `is_player_free_agent` | logical | True when the player had no club. |
| `final_roster_member_status_code` | character | Roster entry status: VALID, PARTIAL (incomplete registration). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fiba_gdap_roster_latest-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FIBA has nothing for the request (HTTP 404, or GDAP answered {} or [] for an unknown id). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. HTTP 400 / 422, or a malformed id, season or date. |

### Example {#fiba_gdap_roster_latest-example}

```python
fiba_gdap_roster_latest(team_id=282183)
```

## fiba_gdap_round_gamedays

Rounds of a competition with their game days.

**Endpoint URL:** `GET https://digital-api.fiba.basketball/hapi/getgdapcompetitionroundgamedaysbyid`

**Valid URL:** [https://digital-api.fiba.basketball/hapi/getgdapcompetitionroundgamedaysbyid?gdapCompetitionId=208182](https://digital-api.fiba.basketball/hapi/getgdapcompetitionroundgamedaysbyid?gdapCompetitionId=208182)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gdapCompetitionId` | `comp` |  | `Y` |  | Required. FIBA competition id (GDAP key space), one per edition: competition.competitionId of any game row (208182 = World Cup 2023, 208722 = Paris 2024 men, 208850 = BCL 2024-25). Find it with fiba_gdap_editions or fiba_gdap_games_between. |

### Returns {#fiba_gdap_round_gamedays-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `competition_id` | integer | FIBA competition id (Int64) the rounds belong to; the comp argument. |
| `round_game_system` | character | Pairing system label (Single, Home and Away, Best of 3). |
| `round_game_system_code` | character | Pairing system code: S single game, HA home and away, BOF3 best of three. |
| `round_parent_round_id` | integer | roundId of the round this one belongs to; null for a top-level round. |
| `round_round_code` | character | Round code (GP group phase, QF, SF, F final, RS regular season, CL17-32 classification...). |
| `round_round_id` | integer | Round id (GDAP key space, unique across competitions); the first segment of gameName. |
| `round_round_name` | character | Round label (Group Phase, Quarter-Finals, Final...). |
| `round_round_number` | integer | Round number within the competition (1-based). |
| `round_round_type` | character | Round type label: Group Round, Non Group Round, Super Round (getgdapgamesbycompetitionid uses the code G/NG here). |
| `round_round_type_code` | character | Round type code: G group, NG non-group (knockout), S super round. |
| `round_round_status_code` | character | Round status: COMPL completed, PROGR in progress, CLOS closed. |
| `round_start_date` | character | Round start, local date-time. |
| `round_end_date` | character | Round end, local date-time (23:59:59.9999999 on the last day). |
| `round_has_teams_assigned` | logical | True once the round's teams are known. |
| `round_round_kind` | character | Standard Round or Qualifiers. |
| `round_standing_header_code` | character | Code of the standings table the round feeds. |
| `round_standing_header_name` | character | Name of the standings table the round feeds. |
| `round_qualified_team_information` | character | Qualification note of the round; null in every sample. |
| `round_round_order` | integer | Display order of the round. |
| `gamedays` | character | Game days of the round as a JSON string; empty (null) for the competitions sampled. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fiba_gdap_round_gamedays-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FIBA has nothing for the request (HTTP 404, or GDAP answered {} or [] for an unknown id). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. HTTP 400 / 422, or a malformed id, season or date. |

### Example {#fiba_gdap_round_gamedays-example}

```python
fiba_gdap_round_gamedays(comp=208182)
```

## fiba_gdap_round_standings

Group standings of every round of a competition.

**Endpoint URL:** `GET https://digital-api.fiba.basketball/hapi/getgdaproundsbycompetitionid`

**Valid URL:** [https://digital-api.fiba.basketball/hapi/getgdaproundsbycompetitionid?gdapCompetitionId=208182](https://digital-api.fiba.basketball/hapi/getgdaproundsbycompetitionid?gdapCompetitionId=208182)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gdapCompetitionId` | `comp` |  | `Y` |  | Required. FIBA competition id (GDAP key space), one per edition: competition.competitionId of any game row (208182 = World Cup 2023, 208722 = Paris 2024 men, 208850 = BCL 2024-25). Find it with fiba_gdap_editions or fiba_gdap_games_between. |

### Returns {#fiba_gdap_round_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `round_id` | integer | Round id (Int64) the group belongs to. |
| `round_name` | character | Round name (Preliminary Round, Final Round...). |
| `round_code` | character | Round code (a short code such as G or F). |
| `group_name` | character | Group name within the round (A, B...). |
| `group_status_code` | character | Group status code (COMPL when the group is complete). |
| `rank` | integer | Rank in the group. |
| `team_team_id` | integer | Competition team id: one per team per competition (Germany = 279569 at the 2023 World Cup, 281545 at Paris 2024). Joins getgdapcompetitionteamrosterbyteamid / getgdapcompetitionteamstatisticsbyteamid gdapTeamId; not an organisationId. |
| `team_organisation_id` | integer | Organisation id, stable across competitions and seasons: the national federation for a national team (64 = Germany), the club for a club team (396 = Filou Oostende). Same key space as nationalFederationId, clubId, play-by-play oId and the game page's T_<id>. |
| `team_code` | character | Team code (FIBA country code or club abbreviation). |
| `team_official_name` | character | Official team name (the country for a national team, the club for a club team). |
| `team_short_name` | character | Short team name: the country code for a national team, the club's short name for a club. |
| `games_played` | integer | Games played in the group. |
| `games_lost` | integer | Losses in the group. |
| `games_won` | integer | Wins in the group. |
| `points` | integer | Standing points (2 per win, 1 per loss). |
| `points_against` | integer | Points conceded. |
| `points_for` | integer | Points the team scored in the group's games. |
| `starting_position` | integer | Seed / draw position in the group. |
| `final_position` | integer | Final position in the group. |
| `plus_minus` | integer | Point differential (pointsFor - pointsAgainst). |
| `penalty_points` | integer | Standing points deducted. |
| `draw_points` | integer | Points for a draw (0); null when not applicable. |
| `group_game_results` | character | This team's games in the group. Stored as a JSON string. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fiba_gdap_round_standings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FIBA has nothing for the request (HTTP 404, or GDAP answered {} or [] for an unknown id). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. HTTP 400 / 422, or a malformed id, season or date. |

### Example {#fiba_gdap_round_standings-example}

```python
fiba_gdap_round_standings(comp=208182)
```
