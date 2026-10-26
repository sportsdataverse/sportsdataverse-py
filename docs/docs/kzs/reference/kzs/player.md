---
title: "KZS — KZS FMS public API (Basketball Federation of Slovenia) — Player"
sidebar_label: "Player"
sidebar_position: 1
description: "KZS — KZS FMS public API (Basketball Federation of Slovenia) — Player — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# KZS — KZS FMS public API (Basketball Federation of Slovenia) — Player

## kzs_player_leaderboard

Player leaderboard of a competition for one stat category (total, per-game average and single-game best).

**Endpoint URL:** `GET https://api.kzs.si/api/v1/public/competitions/{competition_id}/playerStats`

**Valid URL:** [https://api.kzs.si/api/v1/public/competitions/579/playerStats?limit=1000&statsCategory=PTS&orderType=AVERAGE&sortBy=avg&sort=DESC](https://api.kzs.si/api/v1/public/competitions/579/playerStats?limit=1000&statsCategory=PTS&orderType=AVERAGE&sortBy=avg&sort=DESC)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competition_id` | `competition_id` |  | `Y` |  | Competition id (competition_id in kzs_competitions): one per competition per season, e.g. 579 (men's first league 2025/26). |
| `statsCategory` | `stats_category` |  |  | `Y` | Stat category (the site's statsCategory): PTS points, M_2/A_2/PERC_2 two-pointers, M_3/A_3/PERC_3 three-pointers, M_FG/A_FG/PERC_FG field goals, M_FT/A_FT/PERC_FT free throws, R rebounds (R_D defensive, R_O offensive), ASS assists, ST steals, TOV turnovers, PF_CM/PF_RV fouls committed/received, ASS_TOV and ST_TOV ratios. Competitions without live stats offer only PTS, M_2, M_3, M_FT, A_FT, PERC_FT. |
| `orderType` | `order_type` |  |  | `Y` | Figure the leaderboard is built on: AVERAGE (per game), SUM (season total) or BEST_PERFORMANCE. |
| `sortBy` | `sort_by` |  |  | `Y` | Row column the board is ordered by: avg (average), sum or max. The runtime checks the order. |
| `sort` | `sort` |  |  | `Y` | Sort direction. |

### Returns {#kzs_player_leaderboard-returns}

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
| `games_played` | Int64 | Games the player played in the competition. |
| `sum` | Float64 | Season total of the requested statsCategory. |
| `average` | Float64 | Per-game average of the statsCategory. |
| `max` | Float64 | Single-game best of the statsCategory. |
| `player_id` | Int64 | Player id (KZS player registry). Joins to /players/{playerId}, box-score and play-by-play `playerId`. |
| `player_firstname` | String | The player's given name as registered with KZS (lower-case n in this route). |
| `player_lastname` | String | The player's family name as registered with KZS (lower-case n in this route). |
| `photo_uuid` | String | Player photo: image file id, served at https://api.kzs.si/public/images/{uuid}. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kzs_player_leaderboard-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, a competition without standings, a final game with no play-by-play or box score). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API answered HTTP 400 (its only error status: an unknown id or a bad parameter). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: a full list page, a game that is not final, an envelope whose status is not OK, or a body for another competition, match, team, player or season than the one asked for. |

### Example {#kzs_player_leaderboard-example}

```python
kzs_player_leaderboard(competition_id=579)
```

## kzs_player

Player profile: names, year of birth, nationality and positions.

**Endpoint URL:** `GET https://api.kzs.si/api/v1/public/players/{player_id}`

**Valid URL:** [https://api.kzs.si/api/v1/public/players/106285](https://api.kzs.si/api/v1/public/players/106285)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Player registry id (player_id in a box score or roster), e.g. 106285. Not the person id. |

### Returns {#kzs_player-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Person id: the federation's person record behind a player, coach or official; a different key space from player, coach and official ids. |
| `identification_number` | Int64 | Player's KZS registration (licence) number; a different key space from the player id, used by no captured route. |
| `person_id` | Int64 | Person id: the federation's person record behind a player, coach or official; a different key space from player, coach and official ids. |
| `first_name` | String | The player's given name. |
| `last_name` | String | The player's family name. |
| `year_of_birth` | Int64 | Year of birth (players: public bio). |
| `nationality_id` | Int64 | Country id (FMS internal; use the ISO codes). |
| `nationality_alpha2_code` | String | ISO 3166-1 alpha-2 country code. |
| `nationality_alpha3_code` | String | ISO 3166-1 alpha-3 country code. |
| `nationality_name` | String | Country name in Slovenian (e.g. 'Združene države' = United States); use `engName` or the ISO codes. |
| `nationality_eng_name` | String | Country name in English. |
| `homegrown` | Boolean | True when the player counts as homegrown (domači igralec) for roster limits. |
| `photo_uuid` | String | Player photo: image file id, served at https://api.kzs.si/public/images/{uuid}. |
| `positions` | String | Unobserved: null, or an empty array, in every sample, so its type and meaning are unconfirmed. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kzs_player-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, a competition without standings, a final game with no play-by-play or box score). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API answered HTTP 400 (its only error status: an unknown id or a bad parameter). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: a full list page, a game that is not final, an envelope whose status is not OK, or a body for another competition, match, team, player or season than the one asked for. |

### Example {#kzs_player-example}

```python
kzs_player(player_id=106285)
```

## kzs_player_stats

A player's totals and per-game averages in each competition of one season.

**Endpoint URL:** `GET https://api.kzs.si/api/v1/public/players/{player_id}/stats`

**Valid URL:** [https://api.kzs.si/api/v1/public/players/106285/stats?season=2026](https://api.kzs.si/api/v1/public/players/106285/stats?season=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Player registry id (player_id in a box score or roster), e.g. 106285. Not the person id. |
| `season` | `season` |  | `Y` |  | Season END year (2027 = 2026/2027); the wrapper maps it to KZS' opaque season id. |

### Returns {#kzs_player_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Player registry id of the request (the body does not echo it). |
| `season_id` | Int64 | Opaque KZS season id (27 = 2026/2027); the sdv season is the END year in season. |
| `season` | Int64 | Season END year (2026/2027 = 2027), from the row's own season name or id, else from the request. |
| `competition_id` | Int64 | Competition id: one per competition per season (men's top league 2025-26 = 579, 2026-27 = 608). Joins to /competitions/{competitionId} and the `competitionId` query params. |
| `a1` | Int64 | Season total: free throws attempted. |
| `a2` | Int64 | Season total: two-pointers attempted. |
| `a3` | Int64 | Season total: three-pointers attempted. |
| `pts` | Int64 | Season total: points. |
| `ass` | Int64 | Season total: assists. |
| `blag` | Int64 | Season total: own shots blocked (blocks against). |
| `blfv` | Int64 | Season total: blocks made (blocks in favour). |
| `captain` | Int64 | Games as captain. |
| `m1` | Int64 | Season total: free throws made. |
| `m2` | Int64 | Season total: two-pointers made. |
| `m3` | Int64 | Season total: three-pointers made. |
| `mins` | Float64 | Season total: minutes played (decimal minutes). |
| `pfcm` | Int64 | Season total: fouls committed. |
| `pfrv` | Int64 | Season total: fouls received. |
| `rd` | Int64 | Season total: defensive rebounds. |
| `ro` | Int64 | Season total: offensive rebounds. |
| `st` | Int64 | Season total: steals. |
| `startinglineup` | Int64 | Games the player started in the competition. |
| `tov` | Int64 | Season total: turnovers. |
| `played` | Int64 | Games the player played in the competition. |
| `plus_minus` | Int64 | Season total: the team's point difference while the player was on court. |
| `efficiency` | Int64 | Season total: efficiency (PIR). |
| `avgpts` | Float64 | Per game: points. |
| `avga1` | Float64 | Per game: free throws attempted. |
| `avga2` | Float64 | Per game: two-pointers attempted. |
| `avga3` | Float64 | Per game: three-pointers attempted. |
| `avgass` | Float64 | Per game: assists. |
| `avgblag` | Float64 | Per game: own shots blocked (blocks against). |
| `avgblfv` | Float64 | Per game: blocks made (blocks in favour). |
| `avgm1` | Float64 | Per game: free throws made. |
| `avgm2` | Float64 | Per game: two-pointers made. |
| `avgm3` | Float64 | Per game: three-pointers made. |
| `avgmins` | Float64 | Per game: minutes played (decimal minutes). |
| `avgpfcm` | Float64 | Per game: fouls committed. |
| `avgpfrv` | Float64 | Per game: fouls received. |
| `avgrd` | Float64 | Per game: defensive rebounds. |
| `avgro` | Float64 | Per game: offensive rebounds. |
| `avgsecs` | Int64 | Per game: seconds (0 in every sample: unused). |
| `avgst` | Float64 | Per game: steals. |
| `avgtov` | Float64 | Per game: turnovers. |
| `avgplus_minus` | Float64 | Per game: the team's point difference while the player was on court. |
| `avgefficiency` | Float64 | Per game: efficiency (PIR). |
| `live_stats` | Boolean | True when the competition has live stats. |
| `competition_name` | String | Competition name, Slovenian (e.g. 'Liga OTP banka', 'Pokal Spar'). |
| `competition_rank` | String | Tier of the competition (code table). |
| `competition_category` | String | Category of the competition (code table). |
| `competition_gender` | String | MALE or FEMALE competition. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kzs_player_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, a competition without standings, a final game with no play-by-play or box score). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API answered HTTP 400 (its only error status: an unknown id or a bad parameter). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: a full list page, a game that is not final, an envelope whose status is not OK, or a body for another competition, match, team, player or season than the one asked for. |

### Example {#kzs_player_stats-example}

```python
kzs_player_stats(player_id=106285, season=2026)
```
