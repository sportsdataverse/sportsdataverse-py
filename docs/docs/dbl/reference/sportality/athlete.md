---
title: "DBL — Basketligaen site API (www.basketligaen.dk/api, Denmark; Sportality) — Athlete"
sidebar_label: "Athlete"
sidebar_position: 1
description: "DBL — Basketligaen site API (www.basketligaen.dk/api, Denmark; Sportality) — Athlete — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# DBL — Basketligaen site API (www.basketligaen.dk/api, Denmark; Sportality) — Athlete

## sportality_athlete_career

Player career totals, one row per ssgt.

**Endpoint URL:** `GET https://www.basketligaen.dk/api/statistics-v2/athlete/playerProfile_gameLog`

**Valid URL:** [https://www.basketligaen.dk/api/statistics-v2/athlete/playerProfile_gameLog?playerUuid=dpu0901c0t&masterSiteInstanceId=dbl1_dbl](https://www.basketligaen.dk/api/statistics-v2/athlete/playerProfile_gameLog?playerUuid=dpu0901c0t&masterSiteInstanceId=dbl1_dbl)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `playerUuid` | `athlete_uuid` |  | `Y` |  | Required. Athlete uuid. |
| `masterSiteInstanceId` | `master_site_instance_id` |  |  | `Y` | Site instance (dbl1_dbl). No effect in the drop-one probe. |

### Returns {#sportality_athlete_career-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `g` | integer | Games played in the ssgt. |
| `gps` | integer | Games started (0 on every row seen). |
| `mpg` | numeric | Minutes per game, 'M.m' string. |
| `ppg` | numeric | Points per game. |
| `drpg` | numeric | Defensive rebounds per game. |
| `orpg` | numeric | Offensive rebounds per game. |
| `rpg` | numeric | Rebounds per game. |
| `apg` | numeric | Assists per game. |
| `stpg` | numeric | Steals per game. |
| `blkpg` | numeric | Blocks per game. |
| `pfpg` | numeric | Personal fouls per game. |
| `2_p_perc` | numeric | Two-point percentage, 0-100. |
| `3_pm` | integer | Three-pointers made (total). |
| `3_pa` | numeric | Three-pointers attempted (total, or a percentage string in the checked rows; unverified). |
| `3_p_perc` | numeric | Three-point percentage, 0-100. |
| `ft_perc` | numeric | Free-throw percentage, 0-100. |
| `topg` | numeric | Turnovers per game. |
| `plus_minus_pg` | numeric | Plus/minus per game. |
| `info_uuid` | character | Athlete uuid on player tables, team uuid on team tables (platform key space). |
| `info_full_name` | character | Full display name of the player. |
| `info_first_name` | character | Given (first) name(s) of the player. |
| `info_last_name` | character | Family name (surname) of the player. |
| `info_nationality` | character | ISO 3166-1 alpha-2 country code. |
| `info_gender` | character | Gender of the person as recorded by the site (male in every row seen). |
| `info_age_value` | integer | Age in whole years. |
| `info_age_format` | character | Unit of the value (years). Values: `years` = years. |
| `info_weight_value` | integer | Weight of the player in kilograms. |
| `info_weight_format` | character | Unit of the value (kg). Values: `kg` = kilograms. |
| `info_height_value` | integer | Height of the player in centimetres. |
| `info_height_format` | character | Unit of the value (cm). Values: `cm` = centimetres. |
| `info_jersey_number` | character | Jersey number ('N/A' on career rows). |
| `info_position` | character | Position code (G, F, C, SG, SF ...). |
| `info_position_code` | character | Position code ('N/A' on career rows). Values: `N/A` = not applicable (career rows). |
| `info_shoots` | character | Shooting hand (null). |
| `info_team_uuid` | character | Team uuid (platform key space); joins /sports-v2/teams/{team_uuid}. |
| `info_team_instance_id` | character | Site instance id of the club (platform key space). |
| `info_team_name` | character | Name of the team. |
| `info_team_code` | character | Three-letter team code. |
| `info_season_stats` | character | Season totals (always empty here). |
| `info_career_stats` | character | Career totals (always empty here). |
| `info_statistics_provider` | character | Stats provider of the data (genius-basketball = Genius Sports LiveStats). |
| `info_is_in_squad` | logical | Whether the player is in the team's current squad. |
| `info_player_id` | integer | Genius person id (Genius key space; an integer). |
| `info_ssgt_uuid` | character | ssgt uuid of the career row (platform key space). |
| `info_team_id` | integer | Genius team id (Genius key space; an integer here). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sportality_athlete_career-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site answered 404, an empty body (gameday routes before 2023/24, or an unknown game), a table with no rows, or the season and game type are not in layout-info. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. An error envelope or non-JSON 200, another game / team / athlete / ssgt than the one asked for, an unfinished game or truncated play-by-play, a cut stats table, or any other failed fetch (a 500 is the site's answer for an unknown uuid and is not retried). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportality_athlete_career-example}

```python
sportality_athlete_career(athlete_uuid='dpu0901c0t', master_site_instance_id='dbl1_dbl')
```

_Last validated n/a._

## sportality_athlete_details

Player bio plus playerExtIds (the Genius person id).

**Endpoint URL:** `GET https://www.basketligaen.dk/api/sports-v2/athlete-details/{athlete_uuid}`

**Valid URL:** [https://www.basketligaen.dk/api/sports-v2/athlete-details/dpu0901c0t](https://www.basketligaen.dk/api/sports-v2/athlete-details/dpu0901c0t)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_uuid` | `athlete_uuid` |  | `Y` |  | Athlete uuid: roster `players[].uuid` or stats-info `stats[].info.uuid`. Key space: platform athlete uuids (not the Genius person id). |

### Returns {#sportality_athlete_details-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `athlete_uuid` | character | Platform athlete uuid (rosters and athlete routes key on it); not the Genius person id; null where the site has none. |
| `player_id` | integer | Genius person id (Int64). A string key in the players maps, an integer in play-by-play and box-score rows; cast here so the frames join, and join the genius family. |
| `created_at` | character | Creation timestamp. |
| `created_by` | character | Creating site instance. |
| `creator_site` | character | Unobserved: null in every sample and every full body. When filled: Creating site (null). |
| `height` | integer | Height of the player in centimetres. |
| `deleted` | logical | Soft-delete flag. |
| `first_name` | character | Given (first) name(s) of the player. |
| `gender` | character | Gender of the person as recorded by the site (male in every row seen). |
| `last_modified` | character | Last-modified timestamp. |
| `last_name` | character | Family name (surname) of the player. |
| `metadata` | character | Free metadata (always empty). |
| `nationality` | character | ISO 3166-1 alpha-2 country code. |
| `shoots` | character | Shooting hand (''). |
| `weight` | integer | Weight of the player in kilograms. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sportality_athlete_details-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site answered 404, an empty body (gameday routes before 2023/24, or an unknown game), a table with no rows, or the season and game type are not in layout-info. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. An error envelope or non-JSON 200, another game / team / athlete / ssgt than the one asked for, an unfinished game or truncated play-by-play, a cut stats table, or any other failed fetch (a 500 is the site's answer for an unknown uuid and is not retried). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportality_athlete_details-example}

```python
sportality_athlete_details(athlete_uuid='dpu0901c0t')
```

_Last validated n/a._

## sportality_athlete_profile

Player profile: age, height, position, season and career totals.

**Endpoint URL:** `GET https://www.basketligaen.dk/api/statistics-v2/athlete/profile-page`

**Valid URL:** [https://www.basketligaen.dk/api/statistics-v2/athlete/profile-page?playerUuid=dpu0901c0t&masterSiteInstanceId=dbl1_dbl](https://www.basketligaen.dk/api/statistics-v2/athlete/profile-page?playerUuid=dpu0901c0t&masterSiteInstanceId=dbl1_dbl)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `playerUuid` | `athlete_uuid` |  | `Y` |  | Required. Athlete uuid. |
| `masterSiteInstanceId` | `master_site_instance_id` |  |  | `Y` | Site instance (dbl1_dbl). No effect in the drop-one probe. |

### Returns {#sportality_athlete_profile-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `uuid` | character | Platform uuid of this record (10-character lowercase key space of its record type; joins the route that serves it). |
| `full_name` | character | Full display name of the player. |
| `first_name` | character | Given (first) name(s) of the player. |
| `last_name` | character | Family name (surname) of the player. |
| `nationality` | character | ISO 3166-1 alpha-2 country code. |
| `gender` | character | Gender of the team or player: male or female (every record on this host is male). |
| `age_value` | integer | Age in whole years. |
| `age_format` | character | Unit of the value (years). Values: `years` = years. |
| `weight_value` | integer | Weight of the player in kilograms. |
| `weight_format` | character | Unit of the value (kg). Values: `kg` = kilograms. |
| `height_value` | integer | Height of the player in centimetres. |
| `height_format` | character | Unit of the value (cm). Values: `cm` = centimetres. |
| `jersey_number` | integer | Jersey (shirt) number of the player. |
| `position` | character | Playing position. |
| `position_code` | character | Position code (G, F, C, or a finer code such as SG on the profile route). Values: `G` = guard. |
| `shoots` | character | Shooting hand (empty or null for every player seen). |
| `team_uuid` | character | Platform team uuid. |
| `team_instance_id` | character | Site instance id of the club (platform key space). |
| `team_name` | character | Name of the team. |
| `team_code` | character | Platform team code (HOL, RAN). |
| `team_media` | character | URL of the team logo image. |
| `season_stats` | character | The player's current-season totals as field/value pairs (GP, PAvg, ...). |
| `career_stats` | character | The player's career totals as field/value pairs. |
| `statistics_provider` | character | Stats provider of the data (genius-basketball = Genius Sports LiveStats). |
| `is_in_squad` | logical | Whether the player is in the team's current squad. |
| `media_url` | character | Portrait image URL (imgix CDN). |
| `media_alt` | character | Image alt text (''). |
| `media_srcset` | character | Responsive image set (imgix CDN URLs with widths). |
| `media_url_img_original_proportion` | character | Portrait URL at its original proportions. |
| `media_url_placeholder_image` | character | Placeholder image URL (''). |
| `media_videourl` | character | Video URL (''). |
| `media_photographer` | character | Photographer credit (''). |
| `media_focal_point` | character | Focal point record (empty). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sportality_athlete_profile-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site answered 404, an empty body (gameday routes before 2023/24, or an unknown game), a table with no rows, or the season and game type are not in layout-info. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. An error envelope or non-JSON 200, another game / team / athlete / ssgt than the one asked for, an unfinished game or truncated play-by-play, a cut stats table, or any other failed fetch (a 500 is the site's answer for an unknown uuid and is not retried). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportality_athlete_profile-example}

```python
sportality_athlete_profile(athlete_uuid='dpu0901c0t', master_site_instance_id='dbl1_dbl')
```

_Last validated n/a._
