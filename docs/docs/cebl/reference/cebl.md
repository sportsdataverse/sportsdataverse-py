---
title: CEBL — CEBL stats API (api.data.cebl.ca, Canada)
sidebar_label: CEBL stats API (api.data.cebl.ca, Canada)
description: "CEBL — CEBL stats API (api.data.cebl.ca, Canada) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
toc_max_heading_level: 2
---
# CEBL — CEBL stats API (api.data.cebl.ca, Canada)

`sportsdataverse.basketball.cebl` — 12 endpoints.

> **Note:** CEBL own API, owner-approved gray area: these wrappers read the league's own FastAPI service (api.data.cebl.ca, the backend of the CEBL Stats Hub) with the X-Api-Key that the hub's own page bundle ships to every browser. The key is not issued to third parties and no developer terms were found; cebl.ca's terms of use were not reviewed. The runtime reads the key from the bundle at run time, falls back to a documented constant, never prints it, and raises AssetFetchError with a 'key rotated?' hint on a persistent 401. www.cebl.ca/robots.txt disallows nothing. Payloads are not redistributed as release assets; requests are spaced at least 1 s apart.

> **Note:** Season is the calendar year (May to August) = the sdv END year: no mapping. The first season is 2019. An unknown season, team or player answers an empty list, which the runtime raises as NoDataError. Ids are CEBL's own; a relocated franchise has two team ids (cebl_teams lists all 15).

> **Note:** This API has no box-score, play-by-play or shot route. They live in Genius LiveStats (organisation CEBL, see the genius family): cebl_games and cebl_player_gamelog carry the Genius match id as genius_match_id, parsed from the stats URLs (derived, not fetched; the match of a cancelled game may have no data). home_team_id is Genius team 1 and away_team_id team 2 (checked on match 2600563).

> **Note:** Some completed schedule rows have null scores (2026), and a cancelled game has status CANELLED (the API's spelling) with null or zero scores. The schedule lists unplayed games too, so no wrapper refuses an unfinished game. Aggregates are decimal strings upstream and Float64 here. Players' names, nationality and birth date are the public bio; no contact data is served.

## cebl_seasons

Every CEBL season (2019 on), with the active and live flags.

**Endpoint URL:** `GET https://api.data.cebl.ca/seasons/`

**Valid URL:** [https://api.data.cebl.ca/seasons/](https://api.data.cebl.ca/seasons/)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#cebl_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season_id` | integer | CEBL season row id (1 = 2019 ... 8 = 2026). No route takes it: routes take `year`. |
| `season` | integer | Season: the calendar year it is played in (May-August) = the sdv END year. The key every route's `season` param takes. |
| `is_active` | logical | True for the current season (one row: 2026 on 2026-10-12). |
| `is_live` | logical | True while the current season is in progress or open for live data (true for 2026 on 2026-10-12, after its last game: the exact meaning is unverified). |

**`return_parsed=False`** — the raw record (`dict`): `{'rows': <API body>, '_request': {...}}` with the request (kind, season, mode, competition, segment, ids) stamped under `_request`.

### Raises {#cebl_seasons-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an unknown season, team or player answers an empty list, an all-null profile; an empty news feed). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key after a re-read, a non-2xx, a non-JSON body, an error envelope, a body for another season, team or player). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A mode, competition, segment or conference outside its set, a season that is not a 4-digit year, an id that is not an integer, or HTTP 400 / 422 (a missing required query parameter). |

### Example {#cebl_seasons-example}

```python
cebl_seasons()
```

## cebl_teams

Every franchise identity, past and present (15 team ids; a relocated franchise has a second id).

**Endpoint URL:** `GET https://api.data.cebl.ca/teams/`

**Valid URL:** [https://api.data.cebl.ca/teams/](https://api.data.cebl.ca/teams/)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#cebl_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | integer | CEBL team id (its own key space; not a Genius id). Joins /teams/ `id`, the games' `home_team_id` / `away_team_id`, `team_id` in player rows and the `team_id` / `vs_team_id` params. A relocated or renamed franchise has a second id: the Stats Hub groups 15+56 (Saskatchewan Rattlers, Saskatoon Mamba), 17+53 (Calgary Surge, Guelph Nighthawks), 19+55 (Brampton, Hamilton Honey Badgers) and 16+54 (Vancouver, Fraser Valley Bandits). |
| `name_en` | character | Team full name in English ('Niagara River Lions'). |
| `short_name_en` | character | Team short name in English, usually the city ('Niagara'). |
| `name_fr` | character | Team full name in French ('River Lions de Niagara'). |
| `short_name_fr` | character | Team short name in French, usually the city. |
| `is_active` | logical | True for a team in the current league; false for a former identity (Guelph Nighthawks, Hamilton Honey Badgers, Fraser Valley Bandits, Saskatchewan Rattlers, Newfoundland Growlers). |
| `logo_url` | character | Team logo URL (icon with the team name; upstream: 'Should include the icon and team name'). |
| `icon_url` | character | Team icon URL (upstream: 'Should not include the team name'). |

**`return_parsed=False`** — the raw record (`dict`): `{'rows': <API body>, '_request': {...}}` with the request (kind, season, mode, competition, segment, ids) stamped under `_request`.

### Raises {#cebl_teams-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an unknown season, team or player answers an empty list, an all-null profile; an empty news feed). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key after a re-read, a non-2xx, a non-JSON body, an error envelope, a body for another season, team or player). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A mode, competition, segment or conference outside its set, a season that is not a 4-digit year, an id that is not an integer, or HTTP 400 / 422 (a missing required query parameter). |

### Example {#cebl_teams-example}

```python
cebl_teams()
```

## cebl_standings

Standings of one season, one row per team: record, home/away splits, games behind, streak.

**Endpoint URL:** `GET https://api.data.cebl.ca/teams/{season}/`

**Valid URL:** [https://api.data.cebl.ca/teams/2025/](https://api.data.cebl.ca/teams/2025/)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Required. Season: the calendar year it is played in (May to August) = the sdv END year (2025 = the 2025 season). The CEBL's first season is 2019. A season with no data answers NoDataError. |

### Returns {#cebl_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season: the calendar year it is played in (May to August) = the sdv END year (2025). Taken from the request (the body does not echo it); null for the all-time table and for a bare body. |
| `team_id` | integer | CEBL team id (its own key space; not a Genius id). Joins /teams/ `id`, the games' `home_team_id` / `away_team_id`, `team_id` in player rows and the `team_id` / `vs_team_id` params. A relocated or renamed franchise has a second id: the Stats Hub groups 15+56 (Saskatchewan Rattlers, Saskatoon Mamba), 17+53 (Calgary Surge, Guelph Nighthawks), 19+55 (Brampton, Hamilton Honey Badgers) and 16+54 (Vancouver, Fraser Valley Bandits). |
| `name_en` | character | Team full name in English ('Niagara River Lions'). |
| `short_name_en` | character | Team short name in English, usually the city ('Niagara'). |
| `name_fr` | character | Team full name in French ('River Lions de Niagara'). |
| `short_name_fr` | character | Team short name in French, usually the city. |
| `conference` | character | Conference: EAST or WEST. |
| `position` | integer | Rank in the team's conference standings (1 = first), not a playing position. |
| `played` | integer | Regular-season games played (24 a team in 2025). |
| `wins` | integer | Regular-season games won. |
| `win_percentage` | numeric | Win fraction, 0-1 (0.583), not a percentage; the API sends a decimal string. |
| `home_wins` | integer | Regular-season home games won. |
| `away_wins` | integer | Regular-season away games won. |
| `losses` | integer | Regular-season games lost. |
| `home_losses` | integer | Regular-season home games lost. |
| `away_losses` | integer | Regular-season away games lost. |
| `games_behind` | numeric | Games behind the conference leader; the API sends a decimal string ('0.0'). |
| `points_difference` | integer | Points scored minus points allowed over the season. An integer. |
| `last_five` | character | Record in the last 5 games, 'W-L' ('4-1'). |
| `streak` | character | Current streak: W or L and its length ('W4', 'L1'). |
| `logo_url` | character | Team logo URL (icon with the team name) on Google Cloud Storage. |
| `icon_url` | character | Team icon URL (without the team name) on Google Cloud Storage. |
| `tickets_url_en` | character | Ticket-sales page for the team's home games (English). |
| `tickets_url_fr` | character | Ticket-sales page for the team's home games (French). |
| `website_url_en` | character | The team's own website (English). |
| `website_url_fr` | character | The team's own website (French). |
| `shop_url_en` | character | The team's merchandise shop (English). |
| `shop_url_fr` | character | The team's merchandise shop (French). |
| `stats_url_en` | character | The team's stats page on cebl.ca (`https://cebl.ca/stats/{slug}`). |
| `stats_url_fr` | character | The team's stats page on the French cebl.ca (`https://cebl.ca/fr-ca/stats/{slug}`). |

**`return_parsed=False`** — the raw record (`dict`): `{'rows': <API body>, '_request': {...}}` with the request (kind, season, mode, competition, segment, ids) stamped under `_request`.

### Raises {#cebl_standings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an unknown season, team or player answers an empty list, an all-null profile; an empty news feed). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key after a re-read, a non-2xx, a non-JSON body, an error envelope, a body for another season, team or player). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A mode, competition, segment or conference outside its set, a season that is not a 4-digit year, an id that is not an integer, or HTTP 400 / 422 (a missing required query parameter). |

### Example {#cebl_standings-example}

```python
cebl_standings(season=2025)
```

## cebl_roster

One team's roster in one season: player, nationality, birth date, position and jersey.

**Endpoint URL:** `GET https://api.data.cebl.ca/teams/{team_id}/roster/{season}/`

**Valid URL:** [https://api.data.cebl.ca/teams/14/roster/2025/](https://api.data.cebl.ca/teams/14/roster/2025/)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Required. CEBL team id of that season (``team_id`` in cebl_standings), e.g. 20 (Ottawa BlackJacks). |
| `season` | `season` |  | `Y` |  | Required. Season: the calendar year it is played in (May to August) = the sdv END year (2025 = the 2025 season). The CEBL's first season is 2019. A season with no data answers NoDataError. |

### Returns {#cebl_roster-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season of the roster asked for (from the request; the body does not echo it). |
| `team_id` | integer | CEBL team id of the roster asked for (from the request; the body does not echo it). |
| `player_id` | integer | CEBL player id (its own key space). Joins /players/{player_id}/ and `id` in the other player rows. Genius LiveStats `data.json` carries no person id, so the join to Genius play-by-play is by name, team and jersey number (unverified). |
| `full_name` | character | Player's full name. |
| `nationality` | character | Nationality: an upper-case English country name ('CANADA', 'UNITED STATES', 'SOUTH SUDAN'); null when unknown. |
| `birth_date` | character | Date of birth, YYYY-MM-DD (players' public bio field); null when unknown. |
| `position` | character | Playing position: G (guard), F (forward), C (center), or a combination such as 'C/F'; null when unknown. |
| `jersey_number` | character | Jersey number, as a string of up to 2 characters ('0', '00' and '1' are distinct); null when unknown. |
| `photo_url` | character | Player headshot URL on the Genius Sports image CDN (images.statsengine.playbyplay.api.geniussports.com). |

**`return_parsed=False`** — the raw record (`dict`): `{'rows': <API body>, '_request': {...}}` with the request (kind, season, mode, competition, segment, ids) stamped under `_request`.

### Raises {#cebl_roster-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an unknown season, team or player answers an empty list, an all-null profile; an empty news feed). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key after a re-read, a non-2xx, a non-JSON body, an error envelope, a body for another season, team or player). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A mode, competition, segment or conference outside its set, a season that is not a 4-digit year, an id that is not an integer, or HTTP 400 / 422 (a missing required query parameter). |

### Example {#cebl_roster-example}

```python
cebl_roster(season=2025, team_id=14)
```

## cebl_team_stats

Team statistic table (totals or per game) with a league row; filter by season, competition, segment, opponent team or opponent conference.

**Endpoint URL:** `GET https://api.data.cebl.ca/teams/statistics/aggregated/`

**Valid URL:** [https://api.data.cebl.ca/teams/statistics/aggregated/?mode=TOTALS&season=2025&competition=REGULAR](https://api.data.cebl.ca/teams/statistics/aggregated/?mode=TOTALS&season=2025&competition=REGULAR)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `mode` | `mode` |  |  | `Y` | TOTALS (sums over the selected games) or PER_GAME (per-game averages); the API requires one. |
| `season` | `season` |  |  | `Y` | Season: the calendar year (May to August) = the sdv END year. None = every season combined (the Stats Hub's All-Time). An unknown year answers NoDataError. |
| `competition` | `competition` |  |  | `Y` | REGULAR (regular season) or FINALS (the playoffs). None = both. |
| `segment` | `segment` |  |  | `Y` | Game subset: HOME, AWAY, L7D (last 7 days), L5G (last 5 games), L10G (last 10 games), MAY, JUNE, JULY or AUGUST. None = all games. Values from the CEBL Stats Hub bundle. |
| `vs_team_id` | `vs_team_id` |  |  | `Y` | CEBL team id of the opponent: only games against that team count. |
| `vs_conference` | `vs_conference` |  |  | `Y` | EAST or WEST: only games against that conference's teams count. |

### Returns {#cebl_team_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season: the calendar year it is played in (May to August) = the sdv END year (2025). Taken from the request (the body does not echo it); null for the all-time table and for a bare body. |
| `mode` | character | Table mode of the request: TOTALS (sums over the selected games) or PER_GAME (per-game averages); the body does not echo it. |
| `competition` | character | Competition filter of the request: REGULAR or FINALS; null when both were asked. The body does not echo it. |
| `segment` | character | Game-subset filter of the request (HOME, AWAY, L5G, MAY ...); null for all games. The body does not echo it. |
| `vs_conference` | character | Opponent-conference filter of the request (EAST or WEST); null when not asked. The body does not echo it. |
| `vs_team_id` | integer | Opponent team filter of the request (CEBL team id); null when not asked. The body does not echo it. |
| `team_id` | integer | CEBL team id (its own key space; not a Genius id). Joins /teams/ `id`, the games' `home_team_id` / `away_team_id`, `team_id` in player rows and the `team_id` / `vs_team_id` params. A relocated or renamed franchise has a second id: the Stats Hub groups 15+56 (Saskatchewan Rattlers, Saskatoon Mamba), 17+53 (Calgary Surge, Guelph Nighthawks), 19+55 (Brampton, Hamilton Honey Badgers) and 16+54 (Vancouver, Fraser Valley Bandits). Null on the league row. |
| `is_league_row` | logical | True on the one league row of a team table (null team_id and names): the league's own figures, where played counts team-games and, in TOTALS, points equals points_against. |
| `name_en` | character | Team full name in English ('Niagara River Lions'). Null on the league row. |
| `short_name_en` | character | Team short name in English, usually the city ('Niagara'). Null on the league row. |
| `name_fr` | character | Team full name in French ('River Lions de Niagara'). Null on the league row. |
| `short_name_fr` | character | Team short name in French, usually the city. Null on the league row. |
| `played` | integer | Games played (a count in both modes). On the league row: team-games (twice the games). |
| `wins` | integer | Wins (a count in both modes). |
| `win_percentage` | numeric | Wins / games played, 0-1 with 3 decimals ('0.625'). A string. |
| `losses` | integer | Losses (a count in both modes). |
| `points` | numeric | Points scored. TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `points_against` | numeric | Points allowed. TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `point_difference` | numeric | Points scored minus points allowed. TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `field_goals_made` | numeric | Field goals made (2- and 3-pointers). TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `field_goals_attempted` | numeric | Field goals attempted (2- and 3-pointers). TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `field_goal_percentage` | numeric | Field-goal percentage, 0-100 with 1 decimal ('44.7'): made / attempted over the selected games (the same in both modes). A string. |
| `three_points_made` | numeric | Three-pointers made. TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `three_points_attempted` | numeric | Three-pointers attempted. TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `three_point_percentage` | numeric | Three-point percentage, 0-100 with 1 decimal ('44.7'): made / attempted over the selected games (the same in both modes). A string. |
| `free_throws_made` | numeric | Free throws made. TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `free_throws_attempted` | numeric | Free throws attempted. TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `free_throw_percentage` | numeric | Free-throw percentage, 0-100 with 1 decimal ('44.7'): made / attempted over the selected games (the same in both modes). A string. |
| `offensive_rebounds` | numeric | Offensive rebounds. TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `defensive_rebounds` | numeric | Defensive rebounds. TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `rebounds` | numeric | Total rebounds (offensive + defensive). TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `assists` | numeric | Assists. TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `turn_overs` | numeric | Turnovers. TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `steals` | numeric | Steals. TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `blocks` | numeric | Shots blocked. TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `fouls` | numeric | Personal fouls committed. TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `fouls_drawn` | numeric | Fouls drawn (committed by opponents). TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `efficiency` | numeric | Efficiency rating (EFF); CEBL does not publish its formula (unverified: the usual Genius / FIBA EFF is PTS + REB + AST + STL + BLK - missed FG - missed FT - TO). TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `logo_url` | character | Team logo URL (icon with the team name) on Google Cloud Storage. Null on the league row. |
| `icon_url` | character | Team icon URL (without the team name) on Google Cloud Storage. Null on the league row. |
| `stats_url_en` | character | The team's stats page on cebl.ca (`https://cebl.ca/stats/{slug}`). Null on the league row. |
| `stats_url_fr` | character | The team's stats page on the French cebl.ca (`https://cebl.ca/fr-ca/stats/{slug}`). Null on the league row. |

**`return_parsed=False`** — the raw record (`dict`): `{'rows': <API body>, '_request': {...}}` with the request (kind, season, mode, competition, segment, ids) stamped under `_request`.

### Raises {#cebl_team_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an unknown season, team or player answers an empty list, an all-null profile; an empty news feed). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key after a re-read, a non-2xx, a non-JSON body, an error envelope, a body for another season, team or player). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A mode, competition, segment or conference outside its set, a season that is not a 4-digit year, an id that is not an integer, or HTTP 400 / 422 (a missing required query parameter). |

### Example {#cebl_team_stats-example}

```python
cebl_team_stats(competition='REGULAR', mode='TOTALS', season=2025)
```

## cebl_games

Schedule and results of one season (regular season and finals), with the Genius match id that joins play-by-play and shots; optionally one team's games.

**Endpoint URL:** `GET https://api.data.cebl.ca/games/{season}/`

**Valid URL:** [https://api.data.cebl.ca/games/2025/](https://api.data.cebl.ca/games/2025/)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Required. Season: the calendar year it is played in (May to August) = the sdv END year (2025 = the 2025 season). The CEBL's first season is 2019. A season with no data answers NoDataError. |
| `team_id` | `team_id` |  |  | `Y` | CEBL team id (``team_id`` in cebl_teams): limits the rows to that team; the rows echo it. |

### Returns {#cebl_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season of the game: the year of start_time_utc (the calendar year = the sdv END year). |
| `game_id` | integer | CEBL game id (its own key space: 568 in 2019, 8 digits from 2025, e.g. 14708701). No route is keyed by it; the Genius match id is in `stats_url_en`. |
| `genius_match_id` | integer | Genius LiveStats match id parsed from the stats URL (the join to the genius family, organisation CEBL, where play-by-play and shots live). Derived from the URL, not fetched; null when the URL is absent. |
| `start_time_utc` | character | Scheduled tip-off, ISO 8601 in UTC ('2025-05-11T22:00:00Z'). |
| `competition` | character | REGULAR (regular season) or FINALS (playoffs). |
| `status` | character | Game status. Observed: COMPLETE (final) and CANELLED (sic: cancelled; 1 game in 2025, 2 in 2026). The Stats Hub also knows SCHEDULED, IN_PROGRESS, DELAYED and FINISHED. |
| `period` | integer | Last period played when the API sets it (seen only on a cancelled game, which carried period 3); null on a finished game. |
| `venue_name` | character | Arena name ('Edmonton EXPO Centre'). |
| `home_team_id` | integer | CEBL team id (its own key space; not a Genius id). Joins /teams/ `id`, the games' `home_team_id` / `away_team_id`, `team_id` in player rows and the `team_id` / `vs_team_id` params. A relocated or renamed franchise has a second id: the Stats Hub groups 15+56 (Saskatchewan Rattlers, Saskatoon Mamba), 17+53 (Calgary Surge, Guelph Nighthawks), 19+55 (Brampton, Hamilton Honey Badgers) and 16+54 (Vancouver, Fraser Valley Bandits). The home team. |
| `home_team_name` | character | Home team full English name. |
| `home_team_score` | integer | Home team final score. On the 3 cancelled games: null, 0 or a partial score; before tip-off unobserved. |
| `away_team_id` | integer | CEBL team id (its own key space; not a Genius id). Joins /teams/ `id`, the games' `home_team_id` / `away_team_id`, `team_id` in player rows and the `team_id` / `vs_team_id` params. A relocated or renamed franchise has a second id: the Stats Hub groups 15+56 (Saskatchewan Rattlers, Saskatoon Mamba), 17+53 (Calgary Surge, Guelph Nighthawks), 19+55 (Brampton, Hamilton Honey Badgers) and 16+54 (Vancouver, Fraser Valley Bandits). The away team. |
| `away_team_name` | character | Away team full English name. |
| `away_team_score` | integer | Away team final score. On the 3 cancelled games: null, 0 or a partial score; before tip-off unobserved. |
| `home_team_logo_url` | character | URL of the home team's logo image as the games feed lists it (a team-level asset, the same on every game of that team in the season). |
| `home_team_stats_url_en` | character | Home team's cebl.ca stats page (English). |
| `home_team_stats_url_fr` | character | Home team's cebl.ca stats page (French). |
| `away_team_logo_url` | character | URL of the away team's logo image as the games feed lists it (a team-level asset, the same on every game of that team in the season). |
| `away_team_stats_url_en` | character | Away team's cebl.ca stats page (English). |
| `away_team_stats_url_fr` | character | Away team's cebl.ca stats page (French). |
| `stats_url_en` | character | Genius LiveStats box-score page, `https://fibalivestats.dcd.shared.geniussports.com/u/CEBL/{matchId}/`. The segment after `/u/CEBL/` is the Genius match id: the join to genius-livestats/ (org CEBL), whose `data/{matchId}/data.json` holds the box score, play-by-play and shot chart. Only match 2600563 is banked there; the others are derived, and cancelled (CANELLED) games carry a filled-in URL whose `data.json` is unverified. |
| `stats_url_fr` | character | The same Genius LiveStats page in French (`.../u/CEBL/{matchId}/index_fr_FR.html`). |
| `cebl_stats_url_en` | character | The game page on cebl.ca, `https://www.cebl.ca/game?id={matchId}`: `id` is the Genius match id, not this row's `id`. |
| `cebl_stats_url_fr` | character | The French cebl.ca game page (`https://www.cebl.ca/fr-ca/game?id={matchId}`). |
| `tickets_url_en` | character | Ticket-sales page for the game (English). |
| `tickets_url_fr` | character | Ticket-sales page for the game (French). |
| `broadcast_partner_logo_urls` | character | Logo URLs of the game's broadcasters (TSN+, CEBL+, ...); 2 to 7 per game. |

**`return_parsed=False`** — the raw record (`dict`): `{'rows': <API body>, '_request': {...}}` with the request (kind, season, mode, competition, segment, ids) stamped under `_request`.

### Raises {#cebl_games-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an unknown season, team or player answers an empty list, an all-null profile; an empty news feed). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key after a re-read, a non-2xx, a non-JSON body, an error envelope, a body for another season, team or player). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A mode, competition, segment or conference outside its set, a season that is not a 4-digit year, an id that is not an integer, or HTTP 400 / 422 (a missing required query parameter). |

### Example {#cebl_games-example}

```python
cebl_games(season=2025)
```

## cebl_player_stats

Player statistic table (totals or per game) for a season or all-time; filter by competition, segment or team.

**Endpoint URL:** `GET https://api.data.cebl.ca/players/statistics/aggregated/`

**Valid URL:** [https://api.data.cebl.ca/players/statistics/aggregated/?mode=PER_GAME&season=2025](https://api.data.cebl.ca/players/statistics/aggregated/?mode=PER_GAME&season=2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `mode` | `mode` |  |  | `Y` | TOTALS (sums over the selected games) or PER_GAME (per-game averages); the API requires one. |
| `season` | `season` |  |  | `Y` | Season: the calendar year (May to August) = the sdv END year. None = every season combined (the Stats Hub's All-Time). An unknown year answers NoDataError. |
| `competition` | `competition` |  |  | `Y` | REGULAR (regular season) or FINALS (the playoffs). None = both. |
| `segment` | `segment` |  |  | `Y` | Game subset: HOME, AWAY, L7D (last 7 days), L5G (last 5 games), L10G (last 10 games), MAY, JUNE, JULY or AUGUST. None = all games. Values from the CEBL Stats Hub bundle. |
| `team_id` | `team_id` |  |  | `Y` | CEBL team id (``team_id`` in cebl_teams): limits the rows to that team; the rows echo it. |

### Returns {#cebl_player_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season: the calendar year it is played in (May to August) = the sdv END year (2025). Taken from the request (the body does not echo it); null for the all-time table and for a bare body. |
| `mode` | character | Table mode of the request: TOTALS (sums over the selected games) or PER_GAME (per-game averages); the body does not echo it. |
| `competition` | character | Competition filter of the request: REGULAR or FINALS; null when both were asked. The body does not echo it. |
| `segment` | character | Game-subset filter of the request (HOME, AWAY, L5G, MAY ...); null for all games. The body does not echo it. |
| `player_id` | integer | CEBL player id (its own key space). Joins /players/{player_id}/ and `id` in the other player rows. Genius LiveStats `data.json` carries no person id, so the join to Genius play-by-play is by name, team and jersey number (unverified). |
| `full_name` | character | Player's full name. |
| `nationality` | character | Nationality: an upper-case English country name ('CANADA', 'UNITED STATES', 'SOUTH SUDAN'); null when unknown. |
| `team_id` | integer | CEBL team id (its own key space; not a Genius id). Joins /teams/ `id`, the games' `home_team_id` / `away_team_id`, `team_id` in player rows and the `team_id` / `vs_team_id` params. A relocated or renamed franchise has a second id: the Stats Hub groups 15+56 (Saskatchewan Rattlers, Saskatoon Mamba), 17+53 (Calgary Surge, Guelph Nighthawks), 19+55 (Brampton, Hamilton Honey Badgers) and 16+54 (Vancouver, Fraser Valley Bandits). |
| `team_name_en` | character | Full English name of the player's team. |
| `team_short_name_en` | character | Short English name (city) of the player's team. |
| `team_name_fr` | character | Full French name of the player's team. |
| `team_short_name_fr` | character | Short French name (city) of the player's team. |
| `team_logo_url` | character | Logo URL (icon with the name) of the player's team. |
| `played` | integer | Games played (a count in both modes). |
| `started` | integer | Games started (a count in both modes). |
| `minutes` | numeric | Minutes played, decimal minutes (not mm:ss). TOTALS: the sum ('1281.7'); PER_GAME: the per-game average ('29.5'). A string. |
| `points` | numeric | Points scored. TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `field_goals_made` | numeric | Field goals made (2- and 3-pointers). TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `field_goals_attempted` | numeric | Field goals attempted (2- and 3-pointers). TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `field_goal_percentage` | numeric | Field-goal percentage, 0-100 with 1 decimal ('44.7'): made / attempted over the selected games (the same in both modes). A string. |
| `three_points_made` | numeric | Three-pointers made. TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `three_points_attempted` | numeric | Three-pointers attempted. TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `three_point_percentage` | numeric | Three-point percentage, 0-100 with 1 decimal ('44.7'): made / attempted over the selected games (the same in both modes). A string. |
| `free_throws_made` | numeric | Free throws made. TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `free_throws_attempted` | numeric | Free throws attempted. TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `free_throw_percentage` | numeric | Free-throw percentage, 0-100 with 1 decimal ('44.7'): made / attempted over the selected games (the same in both modes). A string. |
| `offensive_rebounds` | numeric | Offensive rebounds. TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `defensive_rebounds` | numeric | Defensive rebounds. TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `rebounds` | numeric | Total rebounds (offensive + defensive). TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `assists` | numeric | Assists. TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `turn_overs` | numeric | Turnovers. TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `steals` | numeric | Steals. TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `blocks` | numeric | Shots blocked. TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `fouls` | numeric | Personal fouls committed. TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `double_doubles` | numeric | Double-doubles: games with 10 or more in two of points, rebounds, assists, steals, blocks. TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `triple_doubles` | numeric | Triple-doubles: games with 10 or more in three of those. TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `target_scores` | numeric | Target-score baskets: games in which the player made the basket that reached the Elam Ending target score and ended the game (CEBL has used the Elam Ending since 2020, so 2019 is 0). TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |

**`return_parsed=False`** — the raw record (`dict`): `{'rows': <API body>, '_request': {...}}` with the request (kind, season, mode, competition, segment, ids) stamped under `_request`.

### Raises {#cebl_player_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an unknown season, team or player answers an empty list, an all-null profile; an empty news feed). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key after a re-read, a non-2xx, a non-JSON body, an error envelope, a body for another season, team or player). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A mode, competition, segment or conference outside its set, a season that is not a 4-digit year, an id that is not an integer, or HTTP 400 / 422 (a missing required query parameter). |

### Example {#cebl_player_stats-example}

```python
cebl_player_stats(mode='PER_GAME', season=2025)
```

## cebl_player_leaders

Top five players per requested statistic (long frame, one row per statistic and leader).

**Endpoint URL:** `GET https://api.data.cebl.ca/players/statistics/leaders/`

**Valid URL:** [https://api.data.cebl.ca/players/statistics/leaders/?statistics=POINTS%2CASSISTS&mode=PER_GAME&season=2025](https://api.data.cebl.ca/players/statistics/leaders/?statistics=POINTS%2CASSISTS&mode=PER_GAME&season=2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `statistics` | `statistics` |  |  | `Y` | Comma-separated statistic names; the frame has one block per name. Names the Stats Hub sends: POINTS, REBOUNDS, ASSISTS, BLOCKS, STEALS, TARGET_SCORES, FIELD_GOAL_PERCENTAGE, THREE_POINTS_MADE, THREE_POINT_PERCENTAGE, FREE_THROW_PERCENTAGE, FOULS, TURNOVERS, MINUTES. |
| `mode` | `mode` |  |  | `Y` | TOTALS (sums over the selected games) or PER_GAME (per-game averages); the API requires one. |
| `season` | `season` |  |  | `Y` | Season: the calendar year (May to August) = the sdv END year. None = every season combined (the Stats Hub's All-Time). An unknown year answers NoDataError. |
| `competition` | `competition` |  |  | `Y` | REGULAR (regular season) or FINALS (the playoffs). None = both. |
| `team_id` | `team_id` |  |  | `Y` | CEBL team id (``team_id`` in cebl_teams): limits the rows to that team; the rows echo it. |

### Returns {#cebl_player_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `statistic` | character | Statistic this leader row ranks, the body's key for the list: POINTS, REBOUNDS, ASSISTS, TARGET_SCORES and the other names the request asked for. |
| `ranking` | integer | Rank in this statistic, 1 = best; ties share a rank and skip the next (1, 1, 1, 4). The list is the top 5 rows, no more. |
| `season` | integer | Season: the calendar year it is played in (May to August) = the sdv END year (2025). Taken from the request (the body does not echo it); null for the all-time table and for a bare body. |
| `mode` | character | Table mode of the request: TOTALS (sums over the selected games) or PER_GAME (per-game averages); the body does not echo it. |
| `competition` | character | Competition filter of the request: REGULAR or FINALS; null when both were asked. The body does not echo it. |
| `player_id` | integer | CEBL player id (its own key space). Joins /players/{player_id}/ and `id` in the other player rows. Genius LiveStats `data.json` carries no person id, so the join to Genius play-by-play is by name, team and jersey number (unverified). |
| `full_name` | character | Player's full name. |
| `team_id` | integer | CEBL team id (its own key space; not a Genius id). Joins /teams/ `id`, the games' `home_team_id` / `away_team_id`, `team_id` in player rows and the `team_id` / `vs_team_id` params. A relocated or renamed franchise has a second id: the Stats Hub groups 15+56 (Saskatchewan Rattlers, Saskatoon Mamba), 17+53 (Calgary Surge, Guelph Nighthawks), 19+55 (Brampton, Hamilton Honey Badgers) and 16+54 (Vancouver, Fraser Valley Bandits). |
| `team_name_en` | character | Full English name of the player's team. |
| `team_short_name_en` | character | Short English name (city) of the player's team. |
| `team_name_fr` | character | Full French name of the player's team. |
| `team_short_name_fr` | character | Short French name (city) of the player's team. |
| `team_abbreviated_name` | character | Three-letter code of the player's team ('OTT', 'VAN'). |
| `team_logo_url` | character | Logo URL (icon with the name) of the player's team. |
| `value` | numeric | The player's figure for the statistic this list is keyed by, in the requested mode (TOTALS sum or PER_GAME average; percentages 0-100). A decimal string ('25.9'). |
| `photo_url` | character | Player headshot URL on the Genius Sports image CDN (images.statsengine.playbyplay.api.geniussports.com). |

**`return_parsed=False`** — the raw record (`dict`): `{'rows': <API body>, '_request': {...}}` with the request (kind, season, mode, competition, segment, ids) stamped under `_request`.

### Raises {#cebl_player_leaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an unknown season, team or player answers an empty list, an all-null profile; an empty news feed). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key after a re-read, a non-2xx, a non-JSON body, an error envelope, a body for another season, team or player). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A mode, competition, segment or conference outside its set, a season that is not a 4-digit year, an id that is not an integer, or HTTP 400 / 422 (a missing required query parameter). |

### Example {#cebl_player_leaders-example}

```python
cebl_player_leaders(mode='PER_GAME', season=2025, statistics='POINTS,ASSISTS')
```

## cebl_player_seasons

One player's per-season rows and career row (totals or per game).

**Endpoint URL:** `GET https://api.data.cebl.ca/players/{player_id}/statistics/`

**Valid URL:** [https://api.data.cebl.ca/players/570965/statistics/?mode=PER_GAME&career_only=False](https://api.data.cebl.ca/players/570965/statistics/?mode=PER_GAME&career_only=False)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Required. CEBL player id (global across seasons and teams), e.g. 570965 (Javonte Smart); ``player_id`` in cebl_player_stats. |
| `mode` | `mode` |  |  | `Y` | TOTALS (sums over the selected games) or PER_GAME (per-game averages); the API requires one. |
| `career_only` | `career_only` |  |  | `Y` | True returns the career row alone; False the per-season rows and the career row. |
| `competition` | `competition` |  |  | `Y` | REGULAR (regular season) or FINALS (the playoffs). None = both. |

### Returns {#cebl_player_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `mode` | character | Table mode of the request: TOTALS (sums over the selected games) or PER_GAME (per-game averages); the body does not echo it. |
| `competition` | character | Competition filter of the request: REGULAR or FINALS; null when both were asked. The body does not echo it. |
| `player_id` | integer | CEBL player id: the `player_id` of the request, on every row. Joins /players/{player_id}/ and `id` in the other player rows. Genius LiveStats `data.json` carries no person id, so the join to Genius play-by-play is by name, team and jersey number (unverified). |
| `season` | integer | Season (calendar year = sdv END year); null on the career row. |
| `is_career` | logical | True on the career row (null season): the sum or average over every season and team; false on a per-season row. |
| `team_id` | integer | CEBL team id (its own key space; not a Genius id). Joins /teams/ `id`, the games' `home_team_id` / `away_team_id`, `team_id` in player rows and the `team_id` / `vs_team_id` params. A relocated or renamed franchise has a second id: the Stats Hub groups 15+56 (Saskatchewan Rattlers, Saskatoon Mamba), 17+53 (Calgary Surge, Guelph Nighthawks), 19+55 (Brampton, Hamilton Honey Badgers) and 16+54 (Vancouver, Fraser Valley Bandits). The player's team that season (on the career row, his latest). |
| `team_name_en` | character | Full English name of the player's team. |
| `team_short_name_en` | character | Short English name (city) of the player's team. |
| `team_name_fr` | character | Full French name of the player's team. |
| `team_short_name_fr` | character | Short French name (city) of the player's team. |
| `team_logo_url` | character | Logo URL (icon with the name) of the player's team. |
| `played` | integer | Games played (a count in both modes). |
| `started` | integer | Games started (a count in both modes). |
| `minutes` | numeric | Minutes played, decimal minutes (not mm:ss). TOTALS: the sum ('1281.7'); PER_GAME: the per-game average ('29.5'). A string. |
| `points` | numeric | Points scored. TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `field_goals_made` | numeric | Field goals made (2- and 3-pointers). TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `field_goals_attempted` | numeric | Field goals attempted (2- and 3-pointers). TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `field_goal_percentage` | numeric | Field-goal percentage, 0-100 with 1 decimal ('44.7'): made / attempted over the selected games (the same in both modes). A string. |
| `three_points_made` | numeric | Three-pointers made. TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `three_points_attempted` | numeric | Three-pointers attempted. TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `three_point_percentage` | numeric | Three-point percentage, 0-100 with 1 decimal ('44.7'): made / attempted over the selected games (the same in both modes). A string. |
| `free_throws_made` | numeric | Free throws made. TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `free_throws_attempted` | numeric | Free throws attempted. TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `free_throw_percentage` | numeric | Free-throw percentage, 0-100 with 1 decimal ('44.7'): made / attempted over the selected games (the same in both modes). A string. |
| `offensive_rebounds` | numeric | Offensive rebounds. TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `defensive_rebounds` | numeric | Defensive rebounds. TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `rebounds` | numeric | Total rebounds (offensive + defensive). TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `assists` | numeric | Assists. TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `turn_overs` | numeric | Turnovers. TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `steals` | numeric | Steals. TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `blocks` | numeric | Shots blocked. TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `fouls` | numeric | Personal fouls committed. TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `double_doubles` | numeric | Double-doubles: games with 10 or more in two of points, rebounds, assists, steals, blocks. TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `triple_doubles` | numeric | Triple-doubles: games with 10 or more in three of those. TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |
| `target_scores` | numeric | Target-score baskets: games in which the player made the basket that reached the Elam Ending target score and ended the game (CEBL has used the Elam Ending since 2020, so 2019 is 0). TOTALS: the sum over the selected games; PER_GAME: the per-game average, 1 decimal. A decimal sent as a string ('2256.0', '12.7'). |

**`return_parsed=False`** — the raw record (`dict`): `{'rows': <API body>, '_request': {...}}` with the request (kind, season, mode, competition, segment, ids) stamped under `_request`.

### Raises {#cebl_player_seasons-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an unknown season, team or player answers an empty list, an all-null profile; an empty news feed). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key after a re-read, a non-2xx, a non-JSON body, an error envelope, a body for another season, team or player). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A mode, competition, segment or conference outside its set, a season that is not a 4-digit year, an id that is not an integer, or HTTP 400 / 422 (a missing required query parameter). |

### Example {#cebl_player_seasons-example}

```python
cebl_player_seasons(mode='PER_GAME', player_id=570965)
```

## cebl_player

One player's profile and current team: name, nationality, birth date, position, jersey.

**Endpoint URL:** `GET https://api.data.cebl.ca/players/{player_id}/`

**Valid URL:** [https://api.data.cebl.ca/players/570965/](https://api.data.cebl.ca/players/570965/)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Required. CEBL player id (global across seasons and teams), e.g. 570965 (Javonte Smart); ``player_id`` in cebl_player_stats. |

### Returns {#cebl_player-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | integer | CEBL player id (its own key space). Joins /players/{player_id}/ and `id` in the other player rows. Genius LiveStats `data.json` carries no person id, so the join to Genius play-by-play is by name, team and jersey number (unverified). |
| `full_name` | character | Player's full name. |
| `nationality` | character | Nationality: an upper-case English country name ('CANADA', 'UNITED STATES', 'SOUTH SUDAN'); null when unknown. |
| `birth_date` | character | Date of birth, YYYY-MM-DD (players' public bio field); null when unknown. |
| `age` | integer | Age in whole years on the day of the request (computed by the API); null when the birth date is unknown. |
| `position` | character | Playing position: G (guard), F (forward), C (center), or a combination such as 'C/F'; null when unknown. |
| `jersey_number` | character | Jersey number, as a string of up to 2 characters ('0', '00' and '1' are distinct); null when unknown. |
| `team_id` | integer | CEBL team id (its own key space; not a Genius id). Joins /teams/ `id`, the games' `home_team_id` / `away_team_id`, `team_id` in player rows and the `team_id` / `vs_team_id` params. A relocated or renamed franchise has a second id: the Stats Hub groups 15+56 (Saskatchewan Rattlers, Saskatoon Mamba), 17+53 (Calgary Surge, Guelph Nighthawks), 19+55 (Brampton, Hamilton Honey Badgers) and 16+54 (Vancouver, Fraser Valley Bandits). The player's latest team. |
| `team_name_en` | character | Full English name of the player's team. |
| `team_short_name_en` | character | Short English name (city) of the player's team. |
| `team_name_fr` | character | Full French name of the player's team. |
| `team_short_name_fr` | character | Short French name (city) of the player's team. |
| `team_icon_url` | character | Icon URL (without the name) of the player's team. |
| `team_stats_url_en` | character | cebl.ca stats page of the player's team (English). |
| `team_stats_url_fr` | character | cebl.ca stats page of the player's team (French). |
| `photo_url` | character | Player headshot URL on the Genius Sports image CDN (images.statsengine.playbyplay.api.geniussports.com). |

**`return_parsed=False`** — the raw record (`dict`): `{'rows': <API body>, '_request': {...}}` with the request (kind, season, mode, competition, segment, ids) stamped under `_request`.

### Raises {#cebl_player-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an unknown season, team or player answers an empty list, an all-null profile; an empty news feed). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key after a re-read, a non-2xx, a non-JSON body, an error envelope, a body for another season, team or player). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A mode, competition, segment or conference outside its set, a season that is not a 4-digit year, an id that is not an integer, or HTTP 400 / 422 (a missing required query parameter). |

### Example {#cebl_player-example}

```python
cebl_player(player_id=570965)
```

## cebl_player_gamelog

One player's game log: one row per game played, with the Genius match id of the game.

**Endpoint URL:** `GET https://api.data.cebl.ca/players/{player_id}/gamelog/`

**Valid URL:** [https://api.data.cebl.ca/players/570965/gamelog/?season=2025](https://api.data.cebl.ca/players/570965/gamelog/?season=2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Required. CEBL player id (global across seasons and teams), e.g. 570965 (Javonte Smart); ``player_id`` in cebl_player_stats. |
| `season` | `season` |  |  | `Y` | Season: the calendar year (May to August) = the sdv END year. None = every season combined (the Stats Hub's All-Time). An unknown year answers NoDataError. |
| `competition` | `competition` |  |  | `Y` | REGULAR (regular season) or FINALS (the playoffs). None = both. |

### Returns {#cebl_player_gamelog-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | integer | CEBL player id of the log asked for (from the request; the body does not echo it). |
| `season` | integer | Season of the game: the year of game_date (the calendar year = the sdv END year). |
| `game_date` | character | Tip-off of the game, ISO 8601 in UTC (same as the game's `start_time_utc`). |
| `genius_match_id` | integer | Genius LiveStats match id parsed from the stats URL (the join to the genius family, organisation CEBL, where play-by-play and shots live). Derived from the URL, not fetched; null when the URL is absent. |
| `team_abbreviated_name` | character | Three-letter code of the player's team in that game ('OTT'). |
| `matchup` | character | '{TEAM} {team score} {vs or @} {opponent score} {OPP}': the player's team code and score first; `vs` = a home game, `@` = an away game ('OTT 91 @ 95 BHB'). |
| `minutes` | numeric | Minutes played as decimal minutes (26.4), not mm:ss; the API sends a string. |
| `points` | integer | Points scored in the game. An integer. |
| `field_goals_made` | integer | Field goals made (2- and 3-pointers) in the game. An integer. |
| `field_goals_attempted` | integer | Field goals attempted (2- and 3-pointers) in the game. An integer. |
| `field_goal_percentage` | numeric | Field-goal percentage, 0-100 with 1 decimal ('44.7'): made / attempted over the game. A string. |
| `three_points_made` | integer | Three-pointers made in the game. An integer. |
| `three_points_attempted` | integer | Three-pointers attempted in the game. An integer. |
| `three_point_percentage` | numeric | Three-point percentage, 0-100 with 1 decimal ('44.7'): made / attempted over the game. A string. |
| `free_throws_made` | integer | Free throws made in the game. An integer. |
| `free_throws_attempted` | integer | Free throws attempted in the game. An integer. |
| `free_throw_percentage` | numeric | Free-throw percentage, 0-100 with 1 decimal ('44.7'): made / attempted over the game. A string. |
| `offensive_rebounds` | integer | Offensive rebounds in the game. An integer. |
| `defensive_rebounds` | integer | Defensive rebounds in the game. An integer. |
| `rebounds` | integer | Total rebounds in the game; the API sends it as a string ('5'), cast to Int64. |
| `assists` | integer | Assists in the game. An integer. |
| `turn_overs` | integer | Turnovers in the game. An integer. |
| `steals` | integer | Steals in the game. An integer. |
| `blocks` | integer | Shots blocked in the game. An integer. |
| `fouls` | integer | Personal fouls committed in the game. An integer. |
| `is_target_scorer` | logical | True when the player made the basket that reached the Elam Ending target score and ended the game. |
| `game_stats_url_en` | character | cebl.ca game page, `https://www.cebl.ca/game?id={matchId}`: `id` is the Genius match id (the join to genius-livestats/). |
| `game_stats_url_fr` | character | French cebl.ca game page (`https://www.cebl.ca/fr-ca/game?id={matchId}`). |
| `team_stats_url_en` | character | cebl.ca stats page of the player's team in that game (English). |
| `team_stats_url_fr` | character | cebl.ca stats page of the player's team in that game (French). |

**`return_parsed=False`** — the raw record (`dict`): `{'rows': <API body>, '_request': {...}}` with the request (kind, season, mode, competition, segment, ids) stamped under `_request`.

### Raises {#cebl_player_gamelog-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an unknown season, team or player answers an empty list, an all-null profile; an empty news feed). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key after a re-read, a non-2xx, a non-JSON body, an error envelope, a body for another season, team or player). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A mode, competition, segment or conference outside its set, a season that is not a 4-digit year, an id that is not an integer, or HTTP 400 / 422 (a missing required query parameter). |

### Example {#cebl_player_gamelog-example}

```python
cebl_player_gamelog(player_id=570965, season=2025)
```

## cebl_news

News articles in one language. The API answered an empty list on every probe, so this raises NoDataError.

**Endpoint URL:** `GET https://api.data.cebl.ca/news/articles/{language}/`

**Valid URL:** [https://api.data.cebl.ca/news/articles/en/](https://api.data.cebl.ca/news/articles/en/)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `language` | `language` |  | `Y` |  | Required. Language code, e.g. ``en``; every language tried answered an empty list. |
| `team_source_id` | `team_source_id` |  |  | `Y` | CEBL team id: only that team's articles. |

### Returns {#cebl_news-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `news_id` | integer | CEBL news article id; null when the API gives none. Unobserved: the route answered an empty list on every probe. |
| `language` | character | Language code of the request ('en'); the body does not echo it. |
| `title` | character | Article headline (upstream schema; unobserved, the route answered an empty list on every probe). |
| `published_at_utc` | character | Publication time in UTC (upstream schema; unobserved, no article was ever served). |
| `image_url` | character | Article image URL (upstream schema; unobserved, no article was ever served). |
| `html_content` | character | Article body as HTML (upstream schema; unobserved, no article was ever served). |

**`return_parsed=False`** — the raw record (`dict`): `{'rows': <API body>, '_request': {...}}` with the request (kind, season, mode, competition, segment, ids) stamped under `_request`.

### Raises {#cebl_news-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an unknown season, team or player answers an empty list, an all-null profile; an empty news feed). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key after a re-read, a non-2xx, a non-JSON body, an error envelope, a body for another season, team or player). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A mode, competition, segment or conference outside its set, a season that is not a 4-digit year, an id that is not an integer, or HTTP 400 / 422 (a missing required query parameter). |

### Example {#cebl_news-example}

```python
cebl_news(language='en')
```
