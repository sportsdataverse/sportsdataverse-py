---
title: "SHL — SHL site API (www.shl.se/api, Sweden) — Athlete"
sidebar_label: "Athlete"
sidebar_position: 1
description: "SHL — SHL site API (www.shl.se/api, Sweden) — Athlete — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# SHL — SHL site API (www.shl.se/api, Sweden) — Athlete

## shl_athlete_career

Player career totals by season.

**Endpoint URL:** `GET https://www.shl.se/api/statistics-v2/athlete/playerProfile_gameLog`

**Valid URL:** [https://www.shl.se/api/statistics-v2/athlete/playerProfile_gameLog?playerUuid=qQ9-349c7WDv6&masterSiteInstanceId=shl1_shl](https://www.shl.se/api/statistics-v2/athlete/playerProfile_gameLog?playerUuid=qQ9-349c7WDv6&masterSiteInstanceId=shl1_shl)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `playerUuid` | `athlete_uuid` |  | `Y` |  | Required. Athlete uuid. |
| `masterSiteInstanceId` | `master_site_instance_id` |  |  | `Y` | Site instance (shl1_shl). No effect in the drop-one probe. |

### Returns {#shl_athlete_career-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `gp` | numeric | Number of games played. |
| `gpi` | numeric | Games played with ice time. |
| `ga` | numeric | Number of goals conceded (goals against). |
| `gaa` | numeric | Goals against average (per 60 minutes). Numeric, but sent as a string in some rows and as a number in others. |
| `svs` | numeric | Number of saves made by the goalie. |
| `svs_perc` | numeric | Save %. Numeric, but sent as a string in some rows and as a number in others. |
| `w` | numeric | Goalie wins: games the goalie was credited with the win, overtime and shootout wins included. |
| `t` | numeric | Goalie ties: games that ended level with the goalie credited a tie; 0 in every sampled row, as overtime and shootouts now decide every game. |
| `l` | numeric | Goalie losses: games the goalie was charged with the loss, overtime and shootout losses included. |
| `so` | numeric | Number of shutouts (games without a goal against). |
| `info_uuid` | character | Athlete uuid (platform key space) in player and goalie modules, team uuid in team modules; joins to the athlete routes or /sports-v2/teams/{team_uuid}. |
| `info_full_name` | character | Given name and surname. |
| `info_first_name` | character | Given (first) name of the player. |
| `info_last_name` | character | Surname (family name) of the player. |
| `info_birth_date` | character | Date of birth (YYYY-MM-DD). |
| `info_nationality` | character | Nationality (ISO country code). |
| `info_gender` | character | Sex of the athlete or team (male / female). |
| `info_age_value` | integer | Age of the player in whole years. |
| `info_age_format` | character | Unit of value: years. |
| `info_weight_value` | integer | Weight of the player in kilograms. |
| `info_weight_format` | character | Unit of value: kg. |
| `info_height_value` | integer | Height of the player in centimetres. |
| `info_height_format` | character | Unit of value: cm. |
| `info_jersey_number` | character | Jersey (shirt) number of the player. |
| `info_position` | character | Position code (CE = centre, LW, RW, D, GK, F); N/A in the athlete game logs. |
| `info_position_code` | character | Position code (GK, D, F; CE / LW / RW in stats rows); N/A when unknown. |
| `info_shoots` | character | Shooting side (L / R; Left / Right). |
| `info_team_uuid` | character | Team uuid (platform key space, e.g. 087a-087aTQv9u); joins to /sports-v2/teams/{team_uuid} and schedule homeTeamInfo/awayTeamInfo.uuid. Not the team code. |
| `info_team_instance_id` | character | Club site instance id (e.g. fhc1_fhc). |
| `info_team_name` | character | Team name (e.g. Frölunda HC); empty in some rows. |
| `info_team_code` | character | Team code of the player's team (e.g. FHC). |
| `info_season_stats` | character | Current-season stat rows of the player (empty in the sample). |
| `info_career_stats` | character | Career stat rows of the player (empty in the sample). |
| `info_statistics_provider` | character | Stats provider of the data (statnet seen). |
| `info_is_in_squad` | logical | Whether the player is in the current squad. |
| `info_player_id` | integer | isa (StatNet) player id as an INTEGER. Join trap: play-by-play sends the same id as a STRING (player.playerId, pop/nep, assists) and map keys are strings; cast before joining. |
| `info_ssgt_uuid` | character | ssgt uuid (platform key space: one season + series + game type); label via layout-info seasonSeriesGameTypeTeams[]. |
| `info_team_id` | character | Team code (e.g. FHC) in gameday and standings payloads; not the team uuid. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#shl_athlete_career-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SHL answered 404, an empty body (gameday routes before 2023/24), a table with no rows, another game / team / ssgt than the one asked for, or the season and game type are not in layout-info. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#shl_athlete_career-example}

```python
shl_athlete_career(athlete_uuid='qQ9-349c7WDv6', master_site_instance_id='shl1_shl')
```

_Last validated n/a._

## shl_athlete_details

Player bio plus playerExtIds (isa, ramses_legacy, ...).

**Endpoint URL:** `GET https://www.shl.se/api/sports-v2/athlete-details/{athlete_uuid}`

**Valid URL:** [https://www.shl.se/api/sports-v2/athlete-details/qQ9-349c7WDv6](https://www.shl.se/api/sports-v2/athlete-details/qQ9-349c7WDv6)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_uuid` | `athlete_uuid` |  | `Y` |  | Athlete uuid (stats-info stats[].info.uuid, roster rows, or athlete-details). |

### Returns {#shl_athlete_details-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `athlete_data_uuid` | character | Athlete uuid (platform key space; qQ9-... or 10-char form); joins to the athlete routes and stats-info stats[].info.uuid. Not the isa or ramses_legacy id. |
| `athlete_data_contact_info` | character | Contact info (null in captures). |
| `athlete_data_created_at` | character | Record creation time (ISO 8601; often null). |
| `athlete_data_created_by` | character | Record creator (site instance id; often null). |
| `athlete_data_creator_site` | character | Site that created the record (null in captures). |
| `athlete_data_height` | integer | Height of the player in centimetres. |
| `athlete_data_date_of_birth` | character | Date of birth (YYYY-MM-DD). |
| `athlete_data_deleted` | logical | Soft-delete flag of the record (false in every sample). |
| `athlete_data_first_name` | character | Given (first) name of the player. |
| `athlete_data_gender` | character | Sex of the athlete or team (male / female). |
| `athlete_data_last_modified` | character | Record last-modified time (ISO 8601). |
| `athlete_data_last_name` | character | Surname (family name) of the player. |
| `athlete_data_metadata` | character | Free-form metadata. |
| `athlete_data_nationality` | character | Nationality (ISO country code). |
| `athlete_data_shoots` | character | Shooting side (L / R; Left / Right). |
| `athlete_data_weight` | integer | Weight of the player in kilograms. |
| `athlete_data_player_ext_ids` | character | External ids of the athlete (isa, ramses_legacy, ramses_generic_legacy, ramses_merged). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#shl_athlete_details-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SHL answered 404, an empty body (gameday routes before 2023/24), a table with no rows, another game / team / ssgt than the one asked for, or the season and game type are not in layout-info. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#shl_athlete_details-example}

```python
shl_athlete_details(athlete_uuid='qQ9-349c7WDv6')
```

_Last validated n/a._

## shl_athlete_game_log

Player game log of one season.

**Endpoint URL:** `GET https://www.shl.se/api/statistics-v2/athlete/playerProfile_seasonGameLog`

**Valid URL:** [https://www.shl.se/api/statistics-v2/athlete/playerProfile_seasonGameLog?playerUuid=qQ9-349c7WDv6&masterSiteInstanceId=shl1_shl&season=2025](https://www.shl.se/api/statistics-v2/athlete/playerProfile_seasonGameLog?playerUuid=qQ9-349c7WDv6&masterSiteInstanceId=shl1_shl&season=2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `playerUuid` | `athlete_uuid` |  | `Y` |  | Required. Athlete uuid. |
| `masterSiteInstanceId` | `master_site_instance_id` |  |  | `Y` | Site instance (shl1_shl). No effect in the drop-one probe. |
| `season` | `season` |  |  | `Y` | Season END year (2026 = 2025/26); the wrapper sends the START year this route takes (season - 1). None = the player's default (shorter) log. |

### Returns {#shl_athlete_game_log-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `date` | character | Date of the game (YYYY-MM-DD). |
| `series` | character | Series id in the career log. |
| `home_team` | character | Home team SHORT NAME (e.g. Frölunda; may carry a trailing space), not a code. |
| `home_team_icon` | character | Home team logo URL. |
| `guest_team` | character | Away team SHORT NAME (e.g. Luleå), not a code. |
| `guest_team_icon` | character | Away team logo URL. |
| `gp` | numeric | Number of games played. |
| `gpi` | numeric | Games played with ice time. |
| `gs` | numeric | Games started (goalie log). |
| `w` | numeric | 1 when the goalie was credited with the win in this game (overtime and shootout included), else 0. |
| `t` | numeric | 1 when the goalie was credited a tie in this game, else 0; 0 in every sampled row, as overtime and shootouts now decide every game. |
| `l` | numeric | 1 when the goalie was charged with the loss in this game (overtime and shootout included), else 0. |
| `so` | numeric | Number of shutouts (games without a goal against). |
| `ga` | numeric | Number of goals conceded (goals against). |
| `gaa` | numeric | Goals against average (per 60 minutes). Numeric, but sent as a string in some rows and as a number in others. |
| `soga` | numeric | Shots on goal against. |
| `svs` | numeric | Number of saves made by the goalie. |
| `svs_perc` | numeric | Save %. Numeric, but sent as a string in some rows and as a number in others. |
| `info_full_name` | character | Given name and surname. |
| `info_first_name` | character | Given (first) name of the player. |
| `info_last_name` | character | Surname (family name) of the player. |
| `info_birth_date` | character | Date of birth (YYYY-MM-DD). |
| `info_nationality` | character | Nationality (ISO country code). |
| `info_gender` | character | Sex of the athlete or team (male / female). |
| `info_jersey_number` | character | Jersey (shirt) number of the player. |
| `info_position` | character | Position code (CE = centre, LW, RW, D, GK, F); N/A in the athlete game logs. |
| `info_position_code` | character | Position code (GK, D, F; CE / LW / RW in stats rows); N/A when unknown. |
| `info_shoots` | character | Shooting side (L / R; Left / Right). '-' placeholder in every sample. |
| `info_team_name` | character | Team name (e.g. Frölunda HC); empty in some rows. Empty string in every sample (placeholder). |
| `info_season_stats` | character | Current-season stat rows of the player (empty in the sample). |
| `info_career_stats` | character | Career stat rows of the player (empty in the sample). |
| `info_statistics_provider` | character | Stats provider of the data (statnet seen). |
| `info_is_in_squad` | logical | Whether the player is in the current squad. |
| `info_player_id` | integer | isa (StatNet) player id as an INTEGER. Join trap: play-by-play sends the same id as a STRING (player.playerId, pop/nep, assists) and map keys are strings; cast before joining. |
| `info_ssgt_uuid` | character | ssgt uuid (platform key space: one season + series + game type); label via layout-info seasonSeriesGameTypeTeams[]. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#shl_athlete_game_log-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SHL answered 404, an empty body (gameday routes before 2023/24), a table with no rows, another game / team / ssgt than the one asked for, or the season and game type are not in layout-info. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#shl_athlete_game_log-example}

```python
shl_athlete_game_log(athlete_uuid='qQ9-349c7WDv6', master_site_instance_id='shl1_shl', season=2026)
```

_Last validated n/a._

## shl_athlete_profile

Player profile: age, height, weight, position.

**Endpoint URL:** `GET https://www.shl.se/api/statistics-v2/athlete/profile-page`

**Valid URL:** [https://www.shl.se/api/statistics-v2/athlete/profile-page?playerUuid=qQ9-349c7WDv6&masterSiteInstanceId=shl1_shl](https://www.shl.se/api/statistics-v2/athlete/profile-page?playerUuid=qQ9-349c7WDv6&masterSiteInstanceId=shl1_shl)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `playerUuid` | `athlete_uuid` |  | `Y` |  | Required. Athlete uuid. |
| `masterSiteInstanceId` | `master_site_instance_id` |  |  | `Y` | Site instance (shl1_shl). No effect in the drop-one probe. |

### Returns {#shl_athlete_profile-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `uuid` | character | Athlete uuid (platform key space, qQ9-... or 10-char form); joins to the athlete routes and stats-info stats[].info.uuid. Not the isa or ramses_legacy id. |
| `full_name` | character | Given name and surname. |
| `first_name` | character | Given (first) name of the player. |
| `last_name` | character | Surname (family name) of the player. |
| `birth_date` | character | Date of birth (YYYY-MM-DD). |
| `nationality` | character | Nationality (ISO country code). |
| `gender` | character | Sex of the athlete or team (male / female). |
| `age_value` | integer | Age of the player in whole years. |
| `age_format` | character | Unit of value: years. |
| `weight_value` | integer | Weight of the player in kilograms. |
| `weight_format` | character | Unit of value: kg. |
| `height_value` | integer | Height of the player in centimetres. |
| `height_format` | character | Unit of value: cm. |
| `jersey_number` | integer | Jersey (shirt) number of the player. |
| `position` | character | Position label in Swedish (Målvakt = goalkeeper). |
| `position_code` | character | Position code (GK, D, F; CE / LW / RW in stats rows); N/A when unknown. |
| `shoots` | character | Shooting side (L / R; Left / Right). |
| `team_uuid` | character | Platform team uuid. |
| `team_instance_id` | character | Club site instance id (e.g. fhc1_fhc). |
| `team_name` | character | Name of the team. |
| `team_code` | character | StatNet team code (SAIK, FHC). |
| `team_media` | character | Media URL or string. |
| `season_stats` | character | Current-season stat rows of the player (empty in the sample). |
| `career_stats` | character | Career stat rows of the player (empty in the sample). |
| `is_in_squad` | logical | Whether the player is in the current squad. |
| `media_url` | character | Portrait URL on the imgix CDN. |
| `media_alt` | character | Image alt text. Empty string in every sample (placeholder). |
| `media_srcset` | character | Responsive image srcset. |
| `media_url_img_original_proportion` | character | Image URL, original proportion. |
| `media_url_placeholder_image` | character | Placeholder image URL. Empty string in every sample (placeholder). |
| `media_videourl` | character | Link to a club video. Empty string in every sample (placeholder). |
| `media_photographer` | character | Image credit. Empty string in every sample (placeholder). |
| `media_focal_point` | character | Image crop focal point. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#shl_athlete_profile-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SHL answered 404, an empty body (gameday routes before 2023/24), a table with no rows, another game / team / ssgt than the one asked for, or the season and game type are not in layout-info. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#shl_athlete_profile-example}

```python
shl_athlete_profile(athlete_uuid='qQ9-349c7WDv6', master_site_instance_id='shl1_shl')
```

_Last validated n/a._

## shl_athlete_seasons

Seasons a player has stats for.

**Endpoint URL:** `GET https://www.shl.se/api/statistics-v2/athlete/seasonList`

**Valid URL:** [https://www.shl.se/api/statistics-v2/athlete/seasonList?playerUuid=qQ9-349c7WDv6&masterSiteInstanceId=shl1_shl](https://www.shl.se/api/statistics-v2/athlete/seasonList?playerUuid=qQ9-349c7WDv6&masterSiteInstanceId=shl1_shl)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `playerUuid` | `athlete_uuid` |  | `Y` |  | Required. Athlete uuid. |
| `masterSiteInstanceId` | `master_site_instance_id` |  |  | `Y` | Site instance (shl1_shl). No effect in the drop-one probe. |

### Returns {#shl_athlete_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `value` | character | Season START year (2025 = 2025/2026); the season= param of playerProfile_seasonGameLog. Numeric value sent as a string. |
| `name` | character | Season label (e.g. 2025/2026). |
| `label` | character | Display label (season list: 2025/2026). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#shl_athlete_seasons-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SHL answered 404, an empty body (gameday routes before 2023/24), a table with no rows, another game / team / ssgt than the one asked for, or the season and game type are not in layout-info. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#shl_athlete_seasons-example}

```python
shl_athlete_seasons(athlete_uuid='qQ9-349c7WDv6', master_site_instance_id='shl1_shl')
```

_Last validated n/a._
