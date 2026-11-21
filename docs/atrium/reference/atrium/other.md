# Sportradar Atrium — Sportradar Atrium embed API (embed-api.eui.connect.sportradar.com) — Other

> Sportradar Atrium — Sportradar Atrium embed API (embed-api.eui.connect.sportradar.com) — Other — function reference in sdv-py, the SportsDataverse Python package.

## atrium_fixtures

Fixtures (schedule and results) of a season, home and away side by side.

**Endpoint URL:** `GET https://embed-api.eui.connect.sportradar.com/v1/embed/{website_id}/fixtures`

**Valid URL:** [https://embed-api.eui.connect.sportradar.com/v1/embed/14/fixtures](https://embed-api.eui.connect.sportradar.com/v1/embed/14/fixtures)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `website_id` | `website_id` |  | `Y` |  | Site config id: which league site's widget this is (12 LNB France, 14 LNBP Mexico, 3 NBL1 Australia, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro). Not a league id: one site serves several competitions. |
| `season` | `season` |  |  | `Y` | Season END year (2026 = 2025-26 on LNB, site 12, whose own year is the START year; the calendar year on the other sites). Needs competition_id when several competitions share that year (see atrium_seasons). Omit season and season_id for the site's default season. |
| `seasonId` | `season_id` |  |  | `Y` | Season UUID (season_id in atrium_seasons); sent as is, skipping the END-year lookup of season. |
| `competitionId` | `competition_id` |  |  | `Y` | Competition UUID (competition_id in atrium_seasons). Picks the season together with season, and is then not sent; with neither season nor season_id the API filters by it. |
| `entityId` | `entity_id` |  |  | `Y` | Team UUID (entity_id in atrium_teams or any fixture row). |
| `roundNumber` | `round_number` |  |  | `Y` | Round (journee) number to show; the pages that take it list their rounds. |
| `sub` | `sub` |  |  | `Y` | Tab: FIXTURES (upcoming) or RESULTS (played). Omitted = the site's default tab. |
| `locale` | `locale` |  |  | `Y` | Locale of the labels, e.g. en-EN or fr-FR. Omitted = the site's own language. Ids and codes never change. |

### Returns {#atrium_fixtures-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `website_id` | Int64 | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `season_id` | String | Season UUID the page is about (the site's default season when none was asked). |
| `competition_id` | String | Competition UUID of the season. |
| `competition_name` | String | Competition name in the site's language, e.g. Betclic ELITE or LIGA CALIENTE.MX LNBP. |
| `season_end_year` | Int64 | Season END year (the sdv key): the site's year + 1 on LNB (site 12), the calendar year on the other registered sites; null when the site is unregistered, lists no seasons or the body has no season list. |
| `fixture_id` | String | Game UUID (fixture); the key of every game frame and of the fixture_detail routes. |
| `fixture_type` | String | Kind of game as sent: REGULAR, PLAYOFF, ... (the set is open). |
| `name` | String | Fixture name as the site prints it (e.g. Paris - Lyon-Villeurbanne); null on LNBP. |
| `round` | String | Round of the game as the site words it, e.g. Journee: 30. |
| `round_code` | String | Round code; null in every body seen. |
| `pool` | String | Pool the game belongs to; null when the competition has none. |
| `series` | String | Playoff series the game belongs to as sent (JSON text when an object); null outside a series. |
| `stage_code` | String | Opaque 30-hex stage code of the game; null in every body seen. |
| `status` | String | Status code of the game, e.g. CONFIRMED (final), as sent. |
| `status_label` | String | Status of the game as the site words it in its own language (e.g. Termine, Final). |
| `start_time_utc` | Datetime | Scheduled start, naive datetime in UTC (the API sends no offset). |
| `start_time_local` | Datetime | Scheduled start, naive datetime in the venue's local time (timezone gives the zone; no offset sent). |
| `timezone` | String | IANA time zone of the venue (e.g. Europe/Paris, Mexico/General) that start_time_local is in. |
| `times_unconfirmed` | Boolean | True while the start time is not final. |
| `venue` | String | Name of the arena where the game is played. |
| `venue_id` | String | Venue UUID; joins no other route. |
| `attendance` | Int64 | Spectators at the game; null when not reported. |
| `home_entity_id` | String | Team UUID of the home team. |
| `home_code` | String | Short code of the home team (e.g. PAB). |
| `home_name` | String | Name of the home team. |
| `home_score` | Int64 | Points scored by the home team (the API sends a string; Int64 here). |
| `home_result_place` | Int64 | Result place of the home team: 1 for the winner, 2 for the loser. |
| `home_result_status` | String | Result status of the home team's score, e.g. CONFIRMED. |
| `home_draw` | Boolean | True when the game ended level for the home team. |
| `home_shootout_attempts` | String | Shootout attempts of the home team as sent (an empty string becomes null). |
| `home_secondary_score` | String | Secondary score of the home team (unused in the samples; shown as text). |
| `away_entity_id` | String | Team UUID of the away team. |
| `away_code` | String | Short code of the away team (e.g. PAB). |
| `away_name` | String | Name of the away team. |
| `away_score` | Int64 | Points scored by the away team (the API sends a string; Int64 here). |
| `away_result_place` | Int64 | Result place of the away team: 1 for the winner, 2 for the loser. |
| `away_result_status` | String | Result status of the away team's score, e.g. CONFIRMED. |
| `away_draw` | Boolean | True when the game ended level for the away team. |
| `away_shootout_attempts` | String | Shootout attempts of the away team as sent (an empty string becomes null). |
| `away_secondary_score` | String | Secondary score of the away team (unused in the samples; shown as text). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#atrium_fixtures-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, or a fixture id the site does not serve: an unknown id, or a game of another site (the API answers both with an empty fixture)). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#atrium_fixtures-example}

```python
atrium_fixtures(website_id=14)
```

## atrium_seasons

Every season (competition and year) a site lists, with the sdv END year of each.

**Endpoint URL:** `GET https://embed-api.eui.connect.sportradar.com/v1/embed/{website_id}/fixtures`

**Valid URL:** [https://embed-api.eui.connect.sportradar.com/v1/embed/12/fixtures](https://embed-api.eui.connect.sportradar.com/v1/embed/12/fixtures)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `website_id` | `website_id` |  | `Y` |  | Site config id: which league site's widget this is (12 LNB France, 14 LNBP Mexico, 3 NBL1 Australia, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro). Not a league id: one site serves several competitions. |
| `locale` | `locale` |  |  | `Y` | Locale of the labels, e.g. en-EN or fr-FR. Omitted = the site's own language. Ids and codes never change. |

### Returns {#atrium_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `website_id` | Int64 | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `season_id` | String | Season UUID; pass it as season_id to any season-scoped wrapper. |
| `competition_id` | String | Competition UUID; pass it as competition_id to pick one of several seasons of an END year. |
| `competition_name` | String | Competition name in the site's language; the same name can appear for several years. |
| `season_name` | String | Season name in the site's language, e.g. Betclic ELITE 2025. |
| `abbreviation_local` | String | Local abbreviation of the season; null in every body seen. |
| `year` | Int64 | The site's own year of the season: the START year on LNB (site 12), the calendar year elsewhere. |
| `season_end_year` | Int64 | Season END year (the sdv key): the site's year + 1 on LNB (site 12), the calendar year on the other registered sites; null when the site is unregistered, lists no seasons or the body has no season list. |
| `is_default_season` | Boolean | True for the season the body is about (the site's default when none was asked). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#atrium_seasons-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, or a fixture id the site does not serve: an unknown id, or a game of another site (the API answers both with an empty fixture)). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#atrium_seasons-example}

```python
atrium_seasons(website_id=12)
```

## atrium_fixtures_ribbon

The fixtures ribbon: recent and upcoming games across the site's competitions (empty on LNB off-season).

**Endpoint URL:** `GET https://embed-api.eui.connect.sportradar.com/v1/embed/{website_id}/fixtures_ribbon`

**Valid URL:** [https://embed-api.eui.connect.sportradar.com/v1/embed/14/fixtures_ribbon](https://embed-api.eui.connect.sportradar.com/v1/embed/14/fixtures_ribbon)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `website_id` | `website_id` |  | `Y` |  | Site config id: which league site's widget this is (12 LNB France, 14 LNBP Mexico, 3 NBL1 Australia, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro). Not a league id: one site serves several competitions. |
| `season` | `season` |  |  | `Y` | Season END year (2026 = 2025-26 on LNB, site 12, whose own year is the START year; the calendar year on the other sites). Needs competition_id when several competitions share that year (see atrium_seasons). Omit season and season_id for the site's default season. |
| `seasonId` | `season_id` |  |  | `Y` | Season UUID (season_id in atrium_seasons); sent as is, skipping the END-year lookup of season. |
| `competitionId` | `competition_id` |  |  | `Y` | Competition UUID (competition_id in atrium_seasons). Picks the season together with season, and is then not sent; with neither season nor season_id the API filters by it. |
| `entityId` | `entity_id` |  |  | `Y` | Team UUID (entity_id in atrium_teams or any fixture row). |
| `locale` | `locale` |  |  | `Y` | Locale of the labels, e.g. en-EN or fr-FR. Omitted = the site's own language. Ids and codes never change. |

### Returns {#atrium_fixtures_ribbon-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `website_id` | Int64 | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `season_id` | String | Season UUID of the game. |
| `season_name` | String | Name of the season / competition the game belongs to. |
| `fixture_id` | String | Game UUID (fixture); the key of every game frame and of the fixture_detail routes. |
| `series` | String | Playoff series the game belongs to as sent (JSON text when an object); null outside a series. |
| `stage_name` | String | Name of the stage; null in every body seen. |
| `status` | String | Status code of the game, e.g. CONFIRMED. |
| `status_label` | String | Status of the game as the site words it in its own language (e.g. Termine, Final). |
| `is_final` | Boolean | True when the game is over. |
| `is_live` | Boolean | True while the game is being played. |
| `times_unconfirmed` | Boolean | True while the start time is not final. |
| `start_time_utc` | Datetime | Scheduled start, naive datetime in UTC (the API sends no offset). |
| `start_time_local` | Datetime | The ribbon's fixture date: a naive local datetime (no offset; the zone is not given). |
| `home_entity_id` | String | Team UUID of the home team. |
| `home_name` | String | Name of the home team. |
| `home_score` | Int64 | Points scored by the home team (the API sends a string; Int64 here). |
| `home_result_status` | String | Result status of the home team's score, e.g. CONFIRMED. |
| `home_is_leading` | Boolean | True while the home team leads (ribbon rows). |
| `away_entity_id` | String | Team UUID of the away team. |
| `away_name` | String | Name of the away team. |
| `away_score` | Int64 | Points scored by the away team (the API sends a string; Int64 here). |
| `away_result_status` | String | Result status of the away team's score, e.g. CONFIRMED. |
| `away_is_leading` | Boolean | True while the away team leads (ribbon rows). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#atrium_fixtures_ribbon-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, or a fixture id the site does not serve: an unknown id, or a game of another site (the API answers both with an empty fixture)). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#atrium_fixtures_ribbon-example}

```python
atrium_fixtures_ribbon(website_id=14)
```

## atrium_fixture

One game's header: teams, score, venue, status, rules (periods, overtime, shot clock) and captains.

**Endpoint URL:** `GET https://embed-api.eui.connect.sportradar.com/v1/embed/{website_id}/fixture_detail`

**Valid URL:** [https://embed-api.eui.connect.sportradar.com/v1/embed/14/fixture_detail?sub=statistics&fixtureId=0dc631bd-c34c-11f1-8d16-4d5a21afb79d](https://embed-api.eui.connect.sportradar.com/v1/embed/14/fixture_detail?sub=statistics&fixtureId=0dc631bd-c34c-11f1-8d16-4d5a21afb79d)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `website_id` | `website_id` |  | `Y` |  | Site config id: which league site's widget this is (12 LNB France, 14 LNBP Mexico, 3 NBL1 Australia, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro). Not a league id: one site serves several competitions. |
| `fixtureId` | `fixture_id` |  | `Y` |  | Game UUID (fixture_id in atrium_fixtures), e.g. 0dc631bd-c34c-11f1-8d16-4d5a21afb79d. |
| `locale` | `locale` |  |  | `Y` | Locale of the labels, e.g. en-EN or fr-FR. Omitted = the site's own language. Ids and codes never change. |

### Returns {#atrium_fixture-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `website_id` | Int64 | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `fixture_id` | String | Game UUID (fixture); the key of every game frame and of the fixture_detail routes. |
| `season_id` | String | Season UUID of the game. |
| `competition_name` | String | Name of the competition, in the site's language. |
| `name` | String | Fixture name; null on LNBP. |
| `fixture_type` | String | Kind of game as sent: REGULAR, PLAYOFF, ... (the set is open). |
| `age_group` | String | Age group of the competition, e.g. SENIOR. |
| `status` | String | Status code of the game, e.g. CONFIRMED (final), as sent. |
| `status_label` | String | Status of the game as the site words it in its own language (e.g. Termine, Final). |
| `start_time_utc` | Datetime | Scheduled start, naive datetime in UTC (the API sends no offset). |
| `start_time_local` | Datetime | Scheduled start, naive datetime in the venue's local time (timezone gives the zone; no offset sent). |
| `timezone` | String | IANA time zone of the venue (e.g. Europe/Paris, Mexico/General) that start_time_local is in. |
| `venue` | String | Name of the arena where the game is played. |
| `venue_id` | String | Venue UUID; joins no other route. |
| `attendance` | Int64 | Spectators at the game; null when not reported. |
| `external_id` | String | The league's own match number (LNB: the lnb.fr number); null elsewhere. |
| `updated` | Datetime | When the record was last updated, naive datetime (no offset; zone unverified). |
| `live_data_available` | Boolean | True when live data feeds are enabled for the game. |
| `live_video_available` | Boolean | True when a live video stream is attached. |
| `discipline` | String | Discipline label of the game; null in every body seen. |
| `series` | String | Playoff series the game belongs to as sent (JSON text when an object); null outside a series. |
| `number_of_periods` | Int64 | Regulation periods in the game (4). |
| `period_length` | Int64 | Length of a regulation period, in minutes. |
| `overtime_allowed` | Boolean | True when the game may go to overtime. |
| `overtime_length` | Int64 | Length of an overtime period, in minutes. |
| `shot_clock` | Int64 | Shot clock length, in seconds. |
| `max_fouls_personal` | Int64 | Personal fouls that disqualify a player. |
| `fouls_before_bonus` | Int64 | Team fouls in a period before the bonus applies. |
| `maximum_players_on_court` | Int64 | Players a team has on court at once. |
| `maximum_roster_size` | Int64 | Largest roster a team may enter for the game. |
| `home_entity_id` | String | Team UUID of the home team. |
| `home_code` | String | Short code of the home team (e.g. PAB). |
| `home_name` | String | Name of the home team. |
| `home_score` | Int64 | Points scored by the home team (the API sends a string; Int64 here). |
| `home_result_status` | String | Result status of the home team's score, e.g. CONFIRMED. |
| `away_entity_id` | String | Team UUID of the away team. |
| `away_code` | String | Short code of the away team (e.g. PAB). |
| `away_name` | String | Name of the away team. |
| `away_score` | Int64 | Points scored by the away team (the API sends a string; Int64 here). |
| `away_result_status` | String | Result status of the away team's score, e.g. CONFIRMED. |
| `home_captain_person_id` | String | Person UUID of the home team's captain; null when the body names none. |
| `away_captain_person_id` | String | Person UUID of the away team's captain; null when the body names none. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#atrium_fixture-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, or a fixture id the site does not serve: an unknown id, or a game of another site (the API answers both with an empty fixture)). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#atrium_fixture-example}

```python
atrium_fixture(fixture_id='0dc631bd-c34c-11f1-8d16-4d5a21afb79d', website_id=14)
```

## atrium_fixture_periods

One game's periods with the points each team scored in each, overtime periods included (ids 11+).

**Endpoint URL:** `GET https://embed-api.eui.connect.sportradar.com/v1/embed/{website_id}/fixture_detail`

**Valid URL:** [https://embed-api.eui.connect.sportradar.com/v1/embed/14/fixture_detail?sub=statistics&fixtureId=0dc631bd-c34c-11f1-8d16-4d5a21afb79d](https://embed-api.eui.connect.sportradar.com/v1/embed/14/fixture_detail?sub=statistics&fixtureId=0dc631bd-c34c-11f1-8d16-4d5a21afb79d)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `website_id` | `website_id` |  | `Y` |  | Site config id: which league site's widget this is (12 LNB France, 14 LNBP Mexico, 3 NBL1 Australia, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro). Not a league id: one site serves several competitions. |
| `fixtureId` | `fixture_id` |  | `Y` |  | Game UUID (fixture_id in atrium_fixtures), e.g. 0dc631bd-c34c-11f1-8d16-4d5a21afb79d. |
| `locale` | `locale` |  |  | `Y` | Locale of the labels, e.g. en-EN or fr-FR. Omitted = the site's own language. Ids and codes never change. |

### Returns {#atrium_fixture_periods-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `website_id` | Int64 | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `fixture_id` | String | Game UUID (fixture); the key of every game frame and of the fixture_detail routes. |
| `season_id` | String | Season UUID of the game. |
| `period_id` | Int64 | Period number: 1-4 regulation, 11, 12, ... overtimes (not 5). |
| `label` | String | Short period label in the site's language (Q1, P1), as sent. |
| `is_overtime` | Boolean | True for an overtime period. |
| `is_shootout` | Boolean | True for a shootout period. |
| `started` | Boolean | True once the period has started. |
| `ended` | Boolean | True once the period has ended. |
| `duration_minutes` | Int64 | Length of the period in minutes. |
| `elapsed_minutes_before_period` | Int64 | Minutes of game time played before the period starts (40 for the first overtime). |
| `home_entity_id` | String | Team UUID of the home team, from the fixture's isHome flag. |
| `away_entity_id` | String | Team UUID of the away team, from the fixture's isHome flag. |
| `home_points` | Int64 | Points the home team scored in this period (not the running score). |
| `away_points` | Int64 | Points the away team scored in this period (not the running score). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#atrium_fixture_periods-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, or a fixture id the site does not serve: an unknown id, or a game of another site (the API answers both with an empty fixture)). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#atrium_fixture_periods-example}

```python
atrium_fixture_periods(fixture_id='0dc631bd-c34c-11f1-8d16-4d5a21afb79d', website_id=14)
```

## atrium_box_score

One game's box score: one row per rostered player with the statistics the site configures.

**Endpoint URL:** `GET https://embed-api.eui.connect.sportradar.com/v1/embed/{website_id}/fixture_detail`

**Valid URL:** [https://embed-api.eui.connect.sportradar.com/v1/embed/14/fixture_detail?sub=statistics&fixtureId=0dc631bd-c34c-11f1-8d16-4d5a21afb79d](https://embed-api.eui.connect.sportradar.com/v1/embed/14/fixture_detail?sub=statistics&fixtureId=0dc631bd-c34c-11f1-8d16-4d5a21afb79d)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `website_id` | `website_id` |  | `Y` |  | Site config id: which league site's widget this is (12 LNB France, 14 LNBP Mexico, 3 NBL1 Australia, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro). Not a league id: one site serves several competitions. |
| `fixtureId` | `fixture_id` |  | `Y` |  | Game UUID (fixture_id in atrium_fixtures), e.g. 0dc631bd-c34c-11f1-8d16-4d5a21afb79d. |
| `locale` | `locale` |  |  | `Y` | Locale of the labels, e.g. en-EN or fr-FR. Omitted = the site's own language. Ids and codes never change. |

### Returns {#atrium_box_score-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `website_id` | Int64 | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `fixture_id` | String | Game UUID (fixture); the key of every game frame and of the fixture_detail routes. |
| `season_id` | String | Season UUID of the game. |
| `entity_id` | String | Team UUID the player is rostered with for this game. |
| `side` | String | Side of the game the row belongs to: home or away (from the fixture's isHome flag). |
| `person_id` | String | Player UUID (person), shared across seasons and sites. |
| `person_name` | String | Player name as the site prints it (upper case on LNBP and NBL1). |
| `bib` | String | Jersey number the player wears, as text. |
| `position` | String | Position code on the roster (LNB, e.g. PG); null on LNBP. |
| `starter` | Boolean | True when the player started the game. |
| `active` | Boolean | True when the player is on the court in a live game. |
| `participated` | Boolean | True when the player entered the game; false rows have null statistics. |
| `did_not_play_reason` | String | Reason the player did not play; null in every body seen. |
| `minutes` | String | Time played as an ISO 8601 duration (PT7M23S); a team's is the sum over its players (PT200M) (player, this game). |
| `seconds_played` | Int64 | Time played in whole seconds, parsed from minutes (player, this game). |
| `points` | Int64 | Points scored (player, this game). |
| `rebounds` | Int64 | Total rebounds (player, this game). |
| `rebounds_offensive` | Int64 | Offensive rebounds (player, this game). |
| `rebounds_defensive` | Int64 | Defensive rebounds (player, this game). |
| `assists` | Int64 | Assists (player, this game). |
| `steals` | Int64 | Steals (player, this game). |
| `blocks` | Int64 | Shots blocked (player, this game). |
| `blocks_received` | Int64 | Own shots blocked by an opponent (LNB only) (player, this game). |
| `turnovers` | Int64 | Turnovers (player, this game). |
| `fouls_total` | Int64 | Fouls committed (player, this game). |
| `fouls_drawn` | Int64 | Fouls drawn (committed against the player or team; LNBP only) (player, this game). |
| `plus_minus` | Int64 | Plus/minus, the point differential while on court (LNB only; null in some team lines) (player, this game). |
| `efficiency` | Int64 | Efficiency rating, from the site's own formula (player, this game). |
| `field_goals_made` | Int64 | Field goals made, two- and three-pointers together (LNBP only) (player, this game). |
| `field_goals_attempted` | Int64 | Field goals attempted (LNBP only) (player, this game). |
| `field_goals_percentage` | Float64 | Field-goal percentage, 0-100 (LNBP only) (player, this game). |
| `points_two_made` | Int64 | Two-point field goals made (player, this game). |
| `points_two_attempted` | Int64 | Two-point field goals attempted (player, this game). |
| `points_two_percentage` | Float64 | Two-point percentage, 0-100 (player, this game). |
| `points_three_made` | Int64 | Three-point field goals made (player, this game). |
| `points_three_attempted` | Int64 | Three-point field goals attempted (player, this game). |
| `points_three_percentage` | Float64 | Three-point percentage, 0-100 (player, this game). |
| `free_throws_made` | Int64 | Free throws made (player, this game). |
| `free_throws_attempted` | Int64 | Free throws attempted (player, this game). |
| `free_throws_percentage` | Float64 | Free-throw percentage, 0-100 (player, this game). |
| `points_fast_break` | Int64 | Fast-break points (LNBP only) (player, this game). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#atrium_box_score-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, or a fixture id the site does not serve: an unknown id, or a game of another site (the API answers both with an empty fixture)). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#atrium_box_score-example}

```python
atrium_box_score(fixture_id='0dc631bd-c34c-11f1-8d16-4d5a21afb79d', website_id=14)
```

## atrium_pbp

One game's play-by-play: every event with the period clock, the player and the running score.

**Endpoint URL:** `GET https://embed-api.eui.connect.sportradar.com/v1/embed/{website_id}/fixture_detail`

**Valid URL:** [https://embed-api.eui.connect.sportradar.com/v1/embed/14/fixture_detail?sub=pbp&fixtureId=0dc631bd-c34c-11f1-8d16-4d5a21afb79d](https://embed-api.eui.connect.sportradar.com/v1/embed/14/fixture_detail?sub=pbp&fixtureId=0dc631bd-c34c-11f1-8d16-4d5a21afb79d)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `website_id` | `website_id` |  | `Y` |  | Site config id: which league site's widget this is (12 LNB France, 14 LNBP Mexico, 3 NBL1 Australia, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro). Not a league id: one site serves several competitions. |
| `fixtureId` | `fixture_id` |  | `Y` |  | Game UUID (fixture_id in atrium_fixtures), e.g. 0dc631bd-c34c-11f1-8d16-4d5a21afb79d. |
| `locale` | `locale` |  |  | `Y` | Locale of the labels, e.g. en-EN or fr-FR. Omitted = the site's own language. Ids and codes never change. |

### Returns {#atrium_pbp-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `website_id` | Int64 | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `fixture_id` | String | Game UUID (fixture); the key of every game frame and of the fixture_detail routes. |
| `season_id` | String | Season UUID of the game. |
| `sequence` | Int64 | 1-based position of the event in the game (periods in order, events chronological). |
| `event_id` | String | Event UUID, shared with the same event in atrium_shots. |
| `period_id` | Int64 | Period number: 1-4 regulation, 11, 12, ... overtimes (not 5). |
| `clock` | String | Time REMAINING in the period as an ISO 8601 duration (PT10M0S at the start). |
| `clock_seconds` | Int64 | Seconds remaining in the period, parsed from clock. |
| `entity_id` | String | Team UUID the event is credited to. |
| `side` | String | Side of the game the row belongs to: home or away (from the fixture's isHome flag). |
| `person_id` | String | Player UUID; null for team events (time-outs, team rebounds). |
| `person_name` | String | Player the event is credited to; null for team events (time-outs, team rebounds). |
| `bib` | String | Jersey number of the player, as text; null for team events. |
| `event_type` | String | Event type: 2pt, 3pt, freeThrow, rebound, assist, steal, block, turnover, foul, substitution, timeOut, jumpBall, ... (language-independent). |
| `event_sub_type` | String | Event detail: jumpShot, layup, offensive, defensive, in, out, 1Of2, personal, ...; null when none. |
| `description` | String | The event's text in the site's language (e.g. Entre-deux remporte). |
| `success` | Boolean | True when a shot or free throw was made, false when missed; null for other events. |
| `success_label` | String | The made / missed label in the site's language; sent on shot events only. |
| `x` | Float64 | Shot location along the 28 m court length as percent of the court (0-100, origin at a corner); the home team attacks the low-x basket all game; null for non-shots. |
| `y` | Float64 | Shot location across the 15 m width as percent of the court (0-100); which sideline is 0 is unverified; null for non-shots. |
| `home_score` | Int64 | Home team's running score after the event. |
| `away_score` | Int64 | Away team's running score after the event. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#atrium_pbp-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, or a fixture id the site does not serve: an unknown id, or a game of another site (the API answers both with an empty fixture)). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#atrium_pbp-example}

```python
atrium_pbp(fixture_id='0dc631bd-c34c-11f1-8d16-4d5a21afb79d', website_id=14)
```

## atrium_shots

One game's 2pt / 3pt attempts with their court location (percent of the court) and result.

**Endpoint URL:** `GET https://embed-api.eui.connect.sportradar.com/v1/embed/{website_id}/fixture_detail`

**Valid URL:** [https://embed-api.eui.connect.sportradar.com/v1/embed/14/fixture_detail?sub=shot_chart&fixtureId=0dc631bd-c34c-11f1-8d16-4d5a21afb79d](https://embed-api.eui.connect.sportradar.com/v1/embed/14/fixture_detail?sub=shot_chart&fixtureId=0dc631bd-c34c-11f1-8d16-4d5a21afb79d)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `website_id` | `website_id` |  | `Y` |  | Site config id: which league site's widget this is (12 LNB France, 14 LNBP Mexico, 3 NBL1 Australia, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro). Not a league id: one site serves several competitions. |
| `fixtureId` | `fixture_id` |  | `Y` |  | Game UUID (fixture_id in atrium_fixtures), e.g. 0dc631bd-c34c-11f1-8d16-4d5a21afb79d. |
| `locale` | `locale` |  |  | `Y` | Locale of the labels, e.g. en-EN or fr-FR. Omitted = the site's own language. Ids and codes never change. |

### Returns {#atrium_shots-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `website_id` | Int64 | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `fixture_id` | String | Game UUID (fixture); the key of every game frame and of the fixture_detail routes. |
| `season_id` | String | Season UUID of the game. |
| `sequence` | Int64 | 1-based position of the shot among the shots of the game (not the pbp sequence). |
| `event_id` | String | Event UUID of the shot; the same event in atrium_pbp. |
| `period_id` | Int64 | Period number: 1-4 regulation, 11, 12, ... overtimes (not 5). |
| `clock` | String | Time REMAINING in the period as an ISO 8601 duration (PT10M0S at the start). |
| `clock_seconds` | Int64 | Seconds remaining in the period, parsed from clock. |
| `entity_id` | String | Team UUID the event is credited to. |
| `side` | String | Side of the game the row belongs to: home or away (from the fixture's isHome flag). |
| `person_id` | String | Player UUID of the shooter. |
| `person_name` | String | Name of the shooter. |
| `bib` | String | Jersey number of the player, as text; null for team events. |
| `event_type` | String | Event type: 2pt, 3pt, freeThrow, rebound, assist, steal, block, turnover, foul, substitution, timeOut, jumpBall, ... (language-independent). |
| `event_sub_type` | String | Event detail: jumpShot, layup, offensive, defensive, in, out, 1Of2, personal, ...; null when none. |
| `description` | String | The event's text in the site's language (e.g. Entre-deux remporte). |
| `success` | Boolean | True when the shot was made, false when missed. |
| `success_label` | String | The made / missed label in the site's language; sent on shot events only. |
| `x` | Float64 | Shot location along the 28 m court length as percent of the court (0-100, origin at a corner); the home team attacks the low-x basket all game; LNBP and NBL1 clamp rim attempts to 5 / 95. |
| `y` | Float64 | Shot location across the 15 m width as percent of the court (0-100); which sideline is 0 is unverified. |
| `home_score` | Int64 | Home team's running score after the event. |
| `away_score` | Int64 | Away team's running score after the event. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#atrium_shots-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, or a fixture id the site does not serve: an unknown id, or a game of another site (the API answers both with an empty fixture)). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#atrium_shots-example}

```python
atrium_shots(fixture_id='0dc631bd-c34c-11f1-8d16-4d5a21afb79d', website_id=14)
```

## atrium_standings

Standings of a season, with the columns the site configures (LNB home / away splits, LNBP played and points).

**Endpoint URL:** `GET https://embed-api.eui.connect.sportradar.com/v1/embed/{website_id}/standings`

**Valid URL:** [https://embed-api.eui.connect.sportradar.com/v1/embed/14/standings](https://embed-api.eui.connect.sportradar.com/v1/embed/14/standings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `website_id` | `website_id` |  | `Y` |  | Site config id: which league site's widget this is (12 LNB France, 14 LNBP Mexico, 3 NBL1 Australia, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro). Not a league id: one site serves several competitions. |
| `season` | `season` |  |  | `Y` | Season END year (2026 = 2025-26 on LNB, site 12, whose own year is the START year; the calendar year on the other sites). Needs competition_id when several competitions share that year (see atrium_seasons). Omit season and season_id for the site's default season. |
| `seasonId` | `season_id` |  |  | `Y` | Season UUID (season_id in atrium_seasons); sent as is, skipping the END-year lookup of season. |
| `competitionId` | `competition_id` |  |  | `Y` | Competition UUID (competition_id in atrium_seasons). Picks the season together with season, and is then not sent; with neither season nor season_id the API filters by it. |
| `roundNumber` | `round_number` |  |  | `Y` | Round (journee) number to show; the pages that take it list their rounds. |
| `locale` | `locale` |  |  | `Y` | Locale of the labels, e.g. en-EN or fr-FR. Omitted = the site's own language. Ids and codes never change. |

### Returns {#atrium_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `website_id` | Int64 | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `season_id` | String | Season UUID of the table. |
| `season_end_year` | Int64 | Season END year (the sdv key): the site's year + 1 on LNB (site 12), the calendar year on the other registered sites; null when the site is unregistered, lists no seasons or the body has no season list. |
| `group` | String | Label of the table when the standings hold several groups; null for a single table. |
| `position` | Int64 | League table position (1 = first). |
| `entity_id` | String | Team UUID of the row. |
| `team_name` | String | Team name as the site prints it. |
| `is_current_team` | Boolean | True for the team the page was opened for (team context); false on a plain standings request. |
| `logo_url` | String | URL of the team logo image. |
| `wins` | Int64 | Games won; -1 for two teams in the 2025-26 LNB table (an administrative ranking, unverified), kept as sent (team, this table). |
| `losses` | Int64 | Games lost (team, this table). |
| `played` | Int64 | Games played (LNBP and the compact standings) (team, this table). |
| `standing_points` | Int64 | League table points (LNBP) (team, this table). |
| `win_percentage` | Float64 | Share of games won as a 0-1 fraction (not 0-100) (team, this table). |
| `scored_for` | Int64 | Points scored over the season (team, this table). |
| `scored_against` | Int64 | Points conceded over the season (team, this table). |
| `point_difference` | Int64 | Point differential over the season (team, this table). |
| `result_string_home` | String | Home results as a string of result letters in the site's language, one per game (order and letters per locale; unverified) (team, this table). |
| `scored_for_home` | Int64 | Points scored in home games (team, this table). |
| `scored_against_home` | Int64 | Points conceded in home games (team, this table). |
| `point_difference_home` | Int64 | Point differential in home games (team, this table). |
| `result_string_away` | String | Away results as a string of result letters in the site's language, one per game (order and letters per locale; unverified) (team, this table). |
| `scored_for_away` | Int64 | Points scored in away games (team, this table). |
| `scored_against_away` | Int64 | Points conceded in away games (team, this table). |
| `point_difference_away` | Int64 | Point differential in away games (team, this table). |
| `streak` | Int64 | Length of the current run of results (the win / loss sign convention is unverified) (team, this table). |
| `streak_home` | Int64 | Length of the current run of home results (sign convention unverified) (team, this table). |
| `streak_away` | Int64 | Length of the current run of away results (sign convention unverified) (team, this table). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#atrium_standings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, or a fixture id the site does not serve: an unknown id, or a game of another site (the API answers both with an empty fixture)). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#atrium_standings-example}

```python
atrium_standings(website_id=14)
```

## atrium_standings_summary

Compact standings of a season: position, wins, losses and games played.

**Endpoint URL:** `GET https://embed-api.eui.connect.sportradar.com/v1/embed/{website_id}/standings_fragment`

**Valid URL:** [https://embed-api.eui.connect.sportradar.com/v1/embed/12/standings_fragment](https://embed-api.eui.connect.sportradar.com/v1/embed/12/standings_fragment)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `website_id` | `website_id` |  | `Y` |  | Site config id: which league site's widget this is (12 LNB France, 14 LNBP Mexico, 3 NBL1 Australia, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro). Not a league id: one site serves several competitions. |
| `season` | `season` |  |  | `Y` | Season END year (2026 = 2025-26 on LNB, site 12, whose own year is the START year; the calendar year on the other sites). Needs competition_id when several competitions share that year (see atrium_seasons). Omit season and season_id for the site's default season. |
| `seasonId` | `season_id` |  |  | `Y` | Season UUID (season_id in atrium_seasons); sent as is, skipping the END-year lookup of season. |
| `competitionId` | `competition_id` |  |  | `Y` | Competition UUID (competition_id in atrium_seasons). Picks the season together with season, and is then not sent; with neither season nor season_id the API filters by it. |
| `locale` | `locale` |  |  | `Y` | Locale of the labels, e.g. en-EN or fr-FR. Omitted = the site's own language. Ids and codes never change. |

### Returns {#atrium_standings_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `website_id` | Int64 | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `season_id` | String | Season UUID of the table. |
| `season_end_year` | Int64 | Season END year (the sdv key): the site's year + 1 on LNB (site 12), the calendar year on the other registered sites; null when the site is unregistered, lists no seasons or the body has no season list. |
| `group` | String | Label of the table when the standings hold several groups; null for a single table. |
| `position` | Int64 | League table position (1 = first). |
| `entity_id` | String | Team UUID of the row. |
| `team_name` | String | Team name as the site prints it. |
| `is_current_team` | Boolean | True for the team the page was opened for (team context); false on a plain standings request. |
| `logo_url` | String | URL of the team logo image. |
| `wins` | Int64 | Games won; -1 for two teams in the 2025-26 LNB table (an administrative ranking, unverified), kept as sent (team, this table). |
| `losses` | Int64 | Games lost (team, this table). |
| `played` | Int64 | Games played (LNBP and the compact standings) (team, this table). |
| `standing_points` | Int64 | League table points (LNBP) (team, this table). |
| `win_percentage` | Float64 | Share of games won as a 0-1 fraction (not 0-100) (team, this table). |
| `scored_for` | Int64 | Points scored over the season (team, this table). |
| `scored_against` | Int64 | Points conceded over the season (team, this table). |
| `point_difference` | Int64 | Point differential over the season (team, this table). |
| `result_string_home` | String | Home results as a string of result letters in the site's language, one per game (order and letters per locale; unverified) (team, this table). |
| `scored_for_home` | Int64 | Points scored in home games (team, this table). |
| `scored_against_home` | Int64 | Points conceded in home games (team, this table). |
| `point_difference_home` | Int64 | Point differential in home games (team, this table). |
| `result_string_away` | String | Away results as a string of result letters in the site's language, one per game (order and letters per locale; unverified) (team, this table). |
| `scored_for_away` | Int64 | Points scored in away games (team, this table). |
| `scored_against_away` | Int64 | Points conceded in away games (team, this table). |
| `point_difference_away` | Int64 | Point differential in away games (team, this table). |
| `streak` | Int64 | Length of the current run of results (the win / loss sign convention is unverified) (team, this table). |
| `streak_home` | Int64 | Length of the current run of home results (sign convention unverified) (team, this table). |
| `streak_away` | Int64 | Length of the current run of away results (sign convention unverified) (team, this table). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#atrium_standings_summary-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, or a fixture id the site does not serve: an unknown id, or a game of another site (the API answers both with an empty fixture)). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#atrium_standings_summary-example}

```python
atrium_standings_summary(website_id=12)
```

## atrium_leaders

Season leaders per statistic (top 5 each), with their player and team ids.

**Endpoint URL:** `GET https://embed-api.eui.connect.sportradar.com/v1/embed/{website_id}/statistics_leaders`

**Valid URL:** [https://embed-api.eui.connect.sportradar.com/v1/embed/12/statistics_leaders](https://embed-api.eui.connect.sportradar.com/v1/embed/12/statistics_leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `website_id` | `website_id` |  | `Y` |  | Site config id: which league site's widget this is (12 LNB France, 14 LNBP Mexico, 3 NBL1 Australia, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro). Not a league id: one site serves several competitions. |
| `season` | `season` |  |  | `Y` | Season END year (2026 = 2025-26 on LNB, site 12, whose own year is the START year; the calendar year on the other sites). Needs competition_id when several competitions share that year (see atrium_seasons). Omit season and season_id for the site's default season. |
| `seasonId` | `season_id` |  |  | `Y` | Season UUID (season_id in atrium_seasons); sent as is, skipping the END-year lookup of season. |
| `competitionId` | `competition_id` |  |  | `Y` | Competition UUID (competition_id in atrium_seasons). Picks the season together with season, and is then not sent; with neither season nor season_id the API filters by it. |
| `entityId` | `entity_id` |  |  | `Y` | Team UUID (entity_id in atrium_teams or any fixture row). |
| `locale` | `locale` |  |  | `Y` | Locale of the labels, e.g. en-EN or fr-FR. Omitted = the site's own language. Ids and codes never change. |

### Returns {#atrium_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `website_id` | Int64 | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `season_id` | String | Season UUID of the leaders. |
| `stat_key` | String | Statistic the leader is ranked in (points, pointsPerGame, efficiency, doubleDouble, ...). |
| `stat_name` | String | Short column heading the site shows for the statistic (PTS, REB), in the site's language. |
| `stat_label` | String | Longer statistic label the site shows (code in the API), in the site's language. |
| `rank` | Int64 | Position of the leader within the statistic, 1-based (the API lists the top 5). |
| `entity_id` | String | Team UUID of the leader's team. |
| `entity_name` | String | Name of the leader's team. |
| `person_id` | String | Player UUID of the leader. |
| `person_name` | String | Name of the leader. |
| `value` | Float64 | Numeric value of the statistic; null when the API sends a duration or text (see value_text). |
| `value_text` | String | Non-numeric statistic value, e.g. an ISO 8601 duration such as PT12M2S for minutes per game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#atrium_leaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, or a fixture id the site does not serve: an unknown id, or a game of another site (the API answers both with an empty fixture)). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#atrium_leaders-example}

```python
atrium_leaders(website_id=12)
```

## atrium_competition_player_stats

Player statistics of a competition in long form (names only: the bodies carry no ids).

**Endpoint URL:** `GET https://embed-api.eui.connect.sportradar.com/v1/embed/{website_id}/statistics_competition_persons`

**Valid URL:** [https://embed-api.eui.connect.sportradar.com/v1/embed/12/statistics_competition_persons?competitionId=3f4064bb-51ad-11f0-aaaf-2923c944b404](https://embed-api.eui.connect.sportradar.com/v1/embed/12/statistics_competition_persons?competitionId=3f4064bb-51ad-11f0-aaaf-2923c944b404)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `website_id` | `website_id` |  | `Y` |  | Site config id: which league site's widget this is (12 LNB France, 14 LNBP Mexico, 3 NBL1 Australia, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro). Not a league id: one site serves several competitions. |
| `competitionId` | `competition_id` |  | `Y` |  | Competition UUID (competition_id in atrium_seasons); required, e.g. 3f4064bb-51ad-11f0-aaaf-2923c944b404. |
| `locale` | `locale` |  |  | `Y` | Locale of the labels, e.g. en-EN or fr-FR. Omitted = the site's own language. Ids and codes never change. |

### Returns {#atrium_competition_player_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `website_id` | Int64 | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `season_id` | String | Season UUID of the table; null for the competition-scoped routes (their bodies carry none). |
| `table_key` | String | Statistics table the row is from: averages, shootingstatistics, totals, ... (the set is per site). |
| `table_label` | String | The table's heading in the site's language (e.g. Moyennes). |
| `person_id` | String | Player UUID decoded from the widget link beside the name (the tables carry names only); null when the link is missing or undecodable, and always null in team and competition bodies. |
| `person_name` | String | Player name as the site prints it; null in team tables. |
| `entity_id` | String | Team UUID decoded from the widget link (team tables only; player tables name the team but carry no id). |
| `entity_name` | String | Team name as the site prints it; for player rows of a stat table it is the player's team. |
| `stat_key` | String | Statistic key as the API names it (pointsPerGame, fieldGoalsMade, ...), unchanged from header key. |
| `stat_name` | String | Short column heading the site shows for the statistic (PTS, REB), in the site's language. |
| `stat_label` | String | Longer statistic label the site shows (code in the API), in the site's language. |
| `value` | Float64 | Numeric value of the statistic; null when the API sends a duration or text (see value_text). |
| `value_text` | String | Non-numeric statistic value, e.g. an ISO 8601 duration such as PT12M2S for minutes per game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#atrium_competition_player_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, or a fixture id the site does not serve: an unknown id, or a game of another site (the API answers both with an empty fixture)). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#atrium_competition_player_stats-example}

```python
atrium_competition_player_stats(competition_id='3f4064bb-51ad-11f0-aaaf-2923c944b404', website_id=12)
```

## atrium_competition_team_stats

Team statistics of a competition in long form (names only: the bodies carry no ids).

**Endpoint URL:** `GET https://embed-api.eui.connect.sportradar.com/v1/embed/{website_id}/statistics_competition_entities`

**Valid URL:** [https://embed-api.eui.connect.sportradar.com/v1/embed/12/statistics_competition_entities?competitionId=3f4064bb-51ad-11f0-aaaf-2923c944b404](https://embed-api.eui.connect.sportradar.com/v1/embed/12/statistics_competition_entities?competitionId=3f4064bb-51ad-11f0-aaaf-2923c944b404)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `website_id` | `website_id` |  | `Y` |  | Site config id: which league site's widget this is (12 LNB France, 14 LNBP Mexico, 3 NBL1 Australia, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro). Not a league id: one site serves several competitions. |
| `competitionId` | `competition_id` |  | `Y` |  | Competition UUID (competition_id in atrium_seasons); required, e.g. 3f4064bb-51ad-11f0-aaaf-2923c944b404. |
| `locale` | `locale` |  |  | `Y` | Locale of the labels, e.g. en-EN or fr-FR. Omitted = the site's own language. Ids and codes never change. |

### Returns {#atrium_competition_team_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `website_id` | Int64 | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `season_id` | String | Season UUID of the table; null for the competition-scoped routes (their bodies carry none). |
| `table_key` | String | Statistics table the row is from: averages, shootingstatistics, totals, ... (the set is per site). |
| `table_label` | String | The table's heading in the site's language (e.g. Moyennes). |
| `person_id` | String | Player UUID decoded from the widget link beside the name (the tables carry names only); null when the link is missing or undecodable, and always null in team and competition bodies. |
| `person_name` | String | Player name as the site prints it; null in team tables. |
| `entity_id` | String | Team UUID decoded from the widget link (team tables only; player tables name the team but carry no id). |
| `entity_name` | String | Team name as the site prints it; for player rows of a stat table it is the player's team. |
| `stat_key` | String | Statistic key as the API names it (pointsPerGame, fieldGoalsMade, ...), unchanged from header key. |
| `stat_name` | String | Short column heading the site shows for the statistic (PTS, REB), in the site's language. |
| `stat_label` | String | Longer statistic label the site shows (code in the API), in the site's language. |
| `value` | Float64 | Numeric value of the statistic; null when the API sends a duration or text (see value_text). |
| `value_text` | String | Non-numeric statistic value, e.g. an ISO 8601 duration such as PT12M2S for minutes per game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#atrium_competition_team_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, or a fixture id the site does not serve: an unknown id, or a game of another site (the API answers both with an empty fixture)). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#atrium_competition_team_stats-example}

```python
atrium_competition_team_stats(competition_id='3f4064bb-51ad-11f0-aaaf-2923c944b404', website_id=12)
```
