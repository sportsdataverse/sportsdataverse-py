# Basketligaen — Basketligaen site API (www.basketligaen.dk/api, Denmark; Sportality) — Athlete

> Basketligaen — Basketligaen site API (www.basketligaen.dk/api, Denmark; Sportality) — Athlete — function reference in sdv-py, the SportsDataverse Python package.

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
| `g` | Int64 | Games played in the ssgt. |
| `gps` | Int64 | Games started (0 on every row seen). |
| `mpg` | Float64 | Minutes per game, 'M.m' string. |
| `ppg` | Float64 | Points per game. |
| `drpg` | Float64 | Defensive rebounds per game. |
| `orpg` | Float64 | Offensive rebounds per game. |
| `rpg` | Float64 | Rebounds per game. |
| `apg` | Float64 | Assists per game. |
| `stpg` | Float64 | Steals per game. |
| `blkpg` | Float64 | Blocks per game. |
| `pfpg` | Float64 | Personal fouls per game. |
| `2_p_perc` | Float64 | Two-point percentage, 0-100. |
| `3_pm` | Int64 | Three-pointers made (total). |
| `3_pa` | Float64 | Three-pointers attempted (total, or a percentage string in the checked rows; unverified). |
| `3_p_perc` | Float64 | Three-point percentage, 0-100. |
| `ft_perc` | Float64 | Free-throw percentage, 0-100. |
| `topg` | Float64 | Turnovers per game. |
| `plus_minus_pg` | Float64 | Plus/minus per game. |
| `info_uuid` | String | Athlete uuid on player tables, team uuid on team tables (platform key space). |
| `info_full_name` | String | Full display name of the player. |
| `info_first_name` | String | Given (first) name(s) of the player. |
| `info_last_name` | String | Family name (surname) of the player. |
| `info_nationality` | String | ISO 3166-1 alpha-2 country code. |
| `info_gender` | String | Gender of the person as recorded by the site (male in every row seen). |
| `info_age_value` | Int64 | Age in whole years. |
| `info_age_format` | String | Unit of the value (years). Values: `years` = years. |
| `info_weight_value` | Int64 | Weight of the player in kilograms. |
| `info_weight_format` | String | Unit of the value (kg). Values: `kg` = kilograms. |
| `info_height_value` | Int64 | Height of the player in centimetres. |
| `info_height_format` | String | Unit of the value (cm). Values: `cm` = centimetres. |
| `info_jersey_number` | String | Jersey number ('N/A' on career rows). |
| `info_position` | String | Position code (G, F, C, SG, SF ...). |
| `info_position_code` | String | Position code ('N/A' on career rows). Values: `N/A` = not applicable (career rows). |
| `info_shoots` | String | Shooting hand (null). |
| `info_team_uuid` | String | Team uuid (platform key space); joins /sports-v2/teams/{team_uuid}. |
| `info_team_instance_id` | String | Site instance id of the club (platform key space). |
| `info_team_name` | String | Name of the team. |
| `info_team_code` | String | Three-letter team code. |
| `info_season_stats` | String | Season totals (always empty here). |
| `info_career_stats` | String | Career totals (always empty here). |
| `info_statistics_provider` | String | Stats provider of the data (genius-basketball = Genius Sports LiveStats). |
| `info_is_in_squad` | Boolean | Whether the player is in the team's current squad. |
| `info_player_id` | Int64 | Genius person id (Genius key space; an integer). |
| `info_ssgt_uuid` | String | ssgt uuid of the career row (platform key space). |
| `info_team_id` | Int64 | Genius team id (Genius key space; an integer here). |

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
| `athlete_uuid` | String | Platform athlete uuid (rosters and athlete routes key on it); not the Genius person id; null where the site has none. |
| `player_id` | Int64 | Genius person id (Int64). A string key in the players maps, an integer in play-by-play and box-score rows; cast here so the frames join, and join the genius family. |
| `created_at` | String | Creation timestamp. |
| `created_by` | String | Creating site instance. |
| `creator_site` | String | Unobserved: null in every sample and every full body. When filled: Creating site (null). |
| `height` | Int64 | Height of the player in centimetres. |
| `deleted` | Boolean | Soft-delete flag. |
| `first_name` | String | Given (first) name(s) of the player. |
| `gender` | String | Gender of the person as recorded by the site (male in every row seen). |
| `last_modified` | String | Last-modified timestamp. |
| `last_name` | String | Family name (surname) of the player. |
| `metadata` | String | Free metadata (always empty). |
| `nationality` | String | ISO 3166-1 alpha-2 country code. |
| `shoots` | String | Shooting hand (''). |
| `weight` | Int64 | Weight of the player in kilograms. |

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
| `uuid` | String | Platform uuid of this record (10-character lowercase key space of its record type; joins the route that serves it). |
| `full_name` | String | Full display name of the player. |
| `first_name` | String | Given (first) name(s) of the player. |
| `last_name` | String | Family name (surname) of the player. |
| `nationality` | String | ISO 3166-1 alpha-2 country code. |
| `gender` | String | Gender of the team or player: male or female (every record on this host is male). |
| `age_value` | Int64 | Age in whole years. |
| `age_format` | String | Unit of the value (years). Values: `years` = years. |
| `weight_value` | Int64 | Weight of the player in kilograms. |
| `weight_format` | String | Unit of the value (kg). Values: `kg` = kilograms. |
| `height_value` | Int64 | Height of the player in centimetres. |
| `height_format` | String | Unit of the value (cm). Values: `cm` = centimetres. |
| `jersey_number` | Int64 | Jersey (shirt) number of the player. |
| `position` | String | Playing position. |
| `position_code` | String | Position code (G, F, C, or a finer code such as SG on the profile route). Values: `G` = guard. |
| `shoots` | String | Shooting hand (empty or null for every player seen). |
| `team_uuid` | String | Platform team uuid. |
| `team_instance_id` | String | Site instance id of the club (platform key space). |
| `team_name` | String | Name of the team. |
| `team_code` | String | Platform team code (HOL, RAN). |
| `team_media` | String | URL of the team logo image. |
| `season_stats` | String | The player's current-season totals as field/value pairs (GP, PAvg, ...). |
| `career_stats` | String | The player's career totals as field/value pairs. |
| `statistics_provider` | String | Stats provider of the data (genius-basketball = Genius Sports LiveStats). |
| `is_in_squad` | Boolean | Whether the player is in the team's current squad. |
| `media_url` | String | Portrait image URL (imgix CDN). |
| `media_alt` | String | Image alt text (''). |
| `media_srcset` | String | Responsive image set (imgix CDN URLs with widths). |
| `media_url_img_original_proportion` | String | Portrait URL at its original proportions. |
| `media_url_placeholder_image` | String | Placeholder image URL (''). |
| `media_videourl` | String | Video URL (''). |
| `media_photographer` | String | Photographer credit (''). |
| `media_focal_point` | String | Focal point record (empty). |

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
