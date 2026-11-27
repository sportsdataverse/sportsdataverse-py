# College football — 247Sports Recruit Database (ipa.247sports.com)

> College football — 247Sports Recruit Database (ipa.247sports.com) — endpoint reference in sdv-py, the SportsDataverse Python package.

`sportsdataverse.cfb` — 12 endpoints.

## sports247_teams

247Sports RDB college team directory (teamId / institutionKey / conference) for a sport.

**Endpoint URL:** `GET https://ipa.247sports.com/rdb/v1/teams/`

**Valid URL:** [https://ipa.247sports.com/rdb/v1/teams/?sportKey=1](https://ipa.247sports.com/rdb/v1/teams/?sportKey=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportKey` | `sport_key` |  |  | `Y` | 247Sports sport key (1 = football, 2 = basketball). |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `institutionType` | `institution_type` |  |  | `Y` | institutionType query parameter. |

### Returns {#sports247_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `name` | String | Team display name (school + nickname). |
| `team_id` | Int64 | 247Sports RDB team id (per-sport). |
| `institution_key` | Int64 | 247Sports institution key (school-level, sport-agnostic). |
| `conference` | String | Full name of the athletic conference the team competes in (e.g. ACC, Big Ten). |
| `conference_abbreviation` | String | Conference abbreviation. |
| `sport` | String | Sport name (Football, Basketball, ...). |
| `type` | String | Institution type (College, ...). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sports247_teams-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sports247_teams-example}

```python
sports247_teams()
```

## sports247_institution_rankings

247Sports team recruiting-class rankings (247 rank/rating + industry composite) for a sport and class year.

**Endpoint URL:** `GET https://ipa.247sports.com/rdb/v1/rankings/{sport_key}/{year}/institutionrankings/`

**Valid URL:** [https://ipa.247sports.com/rdb/v1/rankings/1/2026/institutionrankings/?pagesize=50](https://ipa.247sports.com/rdb/v1/rankings/1/2026/institutionrankings/?pagesize=50)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | year path parameter. |
| `sport_key` | `sport_key` |  |  | `Y` | 247Sports sport key (1 = football, 2 = basketball). |
| `pagesize` | `page_size` |  |  | `Y` | pagesize query parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `useComposite` | `use_composite` |  |  | `Y` | useComposite query parameter. |
| `conferenceAbbreviation` | `conference_abbreviation` |  |  | `Y` | conferenceAbbreviation query parameter. |
| `institutionKey` | `institution_key` |  |  | `Y` | institutionKey query parameter. |

### Returns {#sports247_institution_rankings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `name` | String | Short display name of the institution as shown on the 247Sports class-ranking page (e.g. USC, Notre Dame). |
| `full_name` | String | Institution full name (school + nickname). |
| `conference_rank` | Int64 | 247Sports class rank within the conference. |
| `conference_composite_rank` | Int64 | Composite class rank within the conference. |
| `rank` | Int64 | 247Sports national team-class rank. |
| `composite_rank` | Int64 | Industry-composite national team-class rank. |
| `institution_key` | Int64 | 247Sports institution key (school-level). |
| `team_key` | Int64 | 247Sports RDB team key (per-sport). |
| `average_rating` | Float64 | Average 247Sports rating of counted commits. |
| `rating` | Float64 | Total 247Sports class rating (team-ranking formula points). |
| `composite_rating` | Float64 | Total composite class rating (formula points). |
| `average_composite_rating` | Float64 | Average composite rating of counted commits. |
| `default_asset` | String | CDN URL of the institution's default logo. |
| `alternate_asset` | String | CDN URL of the institution's alternate logo. |
| `light_asset` | String | CDN URL of the institution's light-background logo. |
| `high_school_ranking_position` | String | Position of the class in the high-school-only ranking. |
| `transfer_points` | String | Transfer-portal points contributed to the class rating. |
| `transfer_number` | Int64 | Number of incoming transfers counted in the class. |
| `five_stars` | Int64 | Count of 247Sports five-star commits. |
| `composite_five_stars` | Int64 | Count of composite five-star commits. |
| `four_stars` | Int64 | Count of 247Sports four-star commits. |
| `composite_four_stars` | Int64 | Count of composite four-star commits. |
| `three_stars` | Int64 | Count of 247Sports three-star commits. |
| `composite_three_stars` | Int64 | Count of composite three-star commits. |
| `commits` | Int64 | Number of commits in the class. |
| `site_key` | Int64 | 247Sports team-site key. |
| `institution_root_path` | String | 247sports.com root path of the institution's team site. |
| `ranking_date` | String | Timestamp the ranking row was last updated. |
| `city` | String | Institution city. |
| `state` | String | Full name of the U.S. state where the institution is located. |
| `state_abbreviation` | String | Institution state abbreviation. |
| `institution_ranking_url` | String | 247sports.com URL of the institution's class-ranking page. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sports247_institution_rankings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sports247_institution_rankings-example}

```python
sports247_institution_rankings(year=2026, sport_key=1)
```

## sports247_recruits

247Sports individual recruit rankings for a sport and class year (247 + industry-composite ratings, stars, commit status; paginated).

**Endpoint URL:** `GET https://ipa.247sports.com/rdb/v1/recruits/`

**Valid URL:** [https://ipa.247sports.com/rdb/v1/recruits/?sportKey=1&year=2026&pagesize=50](https://ipa.247sports.com/rdb/v1/recruits/?sportKey=1&year=2026&pagesize=50)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportKey` | `sport_key` |  |  | `Y` | 247Sports sport key (1 = football, 2 = basketball). |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `pagesize` | `page_size` |  |  | `Y` | pagesize query parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `positionAbbreviation` | `position_abbreviation` |  |  | `Y` | positionAbbreviation query parameter. |
| `stateAbbreviation` | `state_abbreviation` |  |  | `Y` | stateAbbreviation query parameter. |

### Returns {#sports247_recruits-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `key` | Int64 | 247Sports player key of the recruit. |
| `cbs_key` | Int64 | Legacy CBS player key (0 when unmapped). |
| `first_name` | String | Given name of the recruit. |
| `last_name` | String | Family (surname) of the recruit. |
| `profile_url` | String | 247sports.com path to the recruit's profile. |
| `default_asset_url` | String | CDN URL of the recruit's headshot. |
| `primary_position` | String | Recruit's primary position abbreviation. |
| `composite_rating` | Float64 | 247Sports Composite rating (industry-blended 0-1 scale). |
| `composite_star_rating` | Int64 | 247Sports Composite star rating (2-5). |
| `composite_national_rank` | Int64 | Composite national rank. |
| `composite_position_rank` | Int64 | Composite rank at the recruit's position. |
| `composite_state_rank` | Int64 | Composite rank within the recruit's state. |
| `signed_institution` | Float64 | Signed-program object when present; null (this scalar) for unsigned recruits (see signed_institution_* columns). |
| `home_town_city` | String | Recruit's home-town city. |
| `home_town_state` | String | Recruit's home-town state. |
| `committed_institution_institution_key` | Int64 | 247Sports institution key of the program the recruit is committed to. |
| `committed_institution_team_key` | Int64 | 247Sports RDB team key of the program the recruit is committed to. |
| `committed_institution_cbs_key` | Int64 | Legacy CBS key of the program the recruit is committed to. |
| `committed_institution_name` | String | Short name of the program the recruit is committed to. |
| `committed_institution_abbreviation` | String | Abbreviation of the program the recruit is committed to. |
| `committed_institution_full_name` | String | Full name of the program the recruit is committed to. |
| `current_institution_institution_key` | Int64 | 247Sports institution key of the recruit's current program. |
| `current_institution_team_key` | Float64 | 247Sports RDB team key of the recruit's current program. |
| `current_institution_cbs_key` | Float64 | Legacy CBS key of the recruit's current program. |
| `current_institution_name` | String | Short name of the recruit's current program. |
| `current_institution_abbreviation` | String | Abbreviation of the recruit's current program. |
| `current_institution_full_name` | String | Full name of the recruit's current program. |
| `signed_institution_institution_key` | Float64 | 247Sports institution key of the program the recruit signed with. |
| `signed_institution_team_key` | Float64 | 247Sports RDB team key of the program the recruit signed with. |
| `signed_institution_cbs_key` | Float64 | Legacy CBS key of the program the recruit signed with. |
| `signed_institution_name` | String | Short name of the program the recruit signed with. |
| `signed_institution_abbreviation` | String | Abbreviation of the program the recruit signed with. |
| `signed_institution_full_name` | String | Full name of the program the recruit signed with. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sports247_recruits-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sports247_recruits-example}

```python
sports247_recruits(year=2026, sport_key=1)
```

## sports247_transfers

247Sports transfer-portal player entries for a sport and year (paginated).

**Endpoint URL:** `GET https://ipa.247sports.com/rdb/v1/transfers/`

**Valid URL:** [https://ipa.247sports.com/rdb/v1/transfers/?sportKey=1&year=2026&pagesize=50](https://ipa.247sports.com/rdb/v1/transfers/?sportKey=1&year=2026&pagesize=50)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportKey` | `sport_key` |  |  | `Y` | 247Sports sport key (1 = football, 2 = basketball). |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `pagesize` | `page_size` |  |  | `Y` | pagesize query parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |

### Returns {#sports247_transfers-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_key` | Int64 | 247Sports player key of the transfer. |
| `player_first_name` | String | Given name of the transfer player. |
| `player_last_name` | String | Family (surname) of the transfer player. |
| `player_avatar` | String | CDN URL of the transfer's headshot. |
| `player_transfer_date` | String | Timestamp the player entered the transfer portal. |
| `player_transfer_rating` | Float64 | 247Sports transfer rating. |
| `player_high_school_rating` | Float64 | The player's original high-school recruiting rating. |
| `player_rating` | Float64 | 247Sports current rating. |
| `player_star_rating` | Float64 | 247Sports star rating (2-5). |
| `player_transfer_rank` | Float64 | National rank among transfers. |
| `player_high_school_rank` | Float64 | The player's original high-school national rank. |
| `player_rank` | Float64 | 247Sports current national rank. |
| `player_rank_trend` | Float64 | Change in rank since the previous update. |
| `player_institution_status` | String | Transfer status relative to institutions (e.g. Entered, Committed). |
| `player_position` | String | Position abbreviation. |
| `player_position_key` | Int64 | 247Sports position key. |
| `player_eligibility_type` | String | Eligibility classification (e.g. Grad, Underclassman). |
| `player_eligibility_years` | Float64 | Years of eligibility remaining. |
| `player_state_rank` | Float64 | Rank within the player's state. |
| `player_status` | String | Portal status label. |
| `player_status_date` | String | Timestamp of the latest status change. |
| `player_transfer_source_institution` | String | Name of the program the player is transferring from. |
| `player_transfer_source_institution_key` | Int64 | 247Sports institution key of the source program. |
| `player_transfer_source_logo` | String | CDN URL of the source program's logo. |
| `player_transfer_source_default_asset` | String | CDN URL of the source program's default logo asset. |
| `player_transfer_source_alternate_asset` | String | CDN URL of the source program's alternate logo asset. |
| `player_transfer_source_light_asset` | String | CDN URL of the source program's light-background logo asset. |
| `player_transfer_source_institution_root_path` | String | 247sports.com root path of the source program's team site. |
| `player_transfer_destination` | String | Name of the program the player is transferring to (null if uncommitted). |
| `player_position_group_key` | Int64 | 247Sports position-group key. |
| `player_position_group_name` | String | Position-group name (e.g. Quarterback). |
| `player_position_rank` | Float64 | Rank at the player's position among transfers. |
| `player_last_update_date` | String | Timestamp of the last record update. |
| `player_transfer_commit_date_time` | String | Timestamp of the transfer commitment (null if uncommitted). |
| `player_weight` | Int64 | Weight in pounds. |
| `player_height` | String | Height as a formatted string. |
| `player_player_profile_url` | String | 247sports.com path to the player's profile. |
| `player_start_date` | String | Start date of the transfer window record. |
| `player_end_date` | String | End date of the transfer window record. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sports247_transfers-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sports247_transfers-example}

```python
sports247_transfers(year=2026, sport_key=1)
```

## sports247_coaches

247Sports coach recruiting rankings for a sport and year (paginated).

**Endpoint URL:** `GET https://ipa.247sports.com/rdb/v1/coaches/`

**Valid URL:** [https://ipa.247sports.com/rdb/v1/coaches/?sportKey=1&year=2026&pageSize=50](https://ipa.247sports.com/rdb/v1/coaches/?sportKey=1&year=2026&pageSize=50)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportKey` | `sport_key` |  |  | `Y` | 247Sports sport key (1 = football, 2 = basketball). |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |

### Returns {#sports247_coaches-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `key` | Int64 | 247Sports coach key. |
| `cbs_key` | Int64 | Legacy CBS coach key. |
| `first_name` | String | Given name of the coach. |
| `last_name` | String | Family (surname) of the coach. |
| `profile_url` | String | 247sports.com path to the coach's profile. |
| `default_asset_url` | String | CDN URL of the coach's headshot. |
| `composite_rating` | String | Coach's composite recruiting rating (formula points). |
| `current_job` | Float64 | Current-job program object when present; null (this scalar) otherwise (see current_job_* columns). |
| `average_composite_rating` | String | Coach's average composite recruit rating. |
| `overall_rank` | String | Coach's national recruiting rank. |
| `division_rank` | String | Coach's rank within the division. |
| `conference_rank` | String | Coach's rank within the conference. |
| `current_job_institution_key` | Float64 | 247Sports institution key of the coach's current program. |
| `current_job_team_key` | Float64 | 247Sports RDB team key of the coach's current program. |
| `current_job_cbs_key` | Float64 | Legacy CBS key of the coach's current program. |
| `current_job_name` | String | Short name of the coach's current program. |
| `current_job_abbreviation` | String | Abbreviation of the coach's current program. |
| `current_job_full_name` | String | Full name of the coach's current program. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sports247_coaches-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sports247_coaches-example}

```python
sports247_coaches(year=2026, sport_key=1)
```

## sports247_transfer_portal_player_feed

247Sports transfer-portal player ranking feed for a sport and class year.

**Endpoint URL:** `GET https://ipa.247sports.com/rdb/v1/rankings/{sport_key}/{year}/transferPortalPlayerfeed/`

**Valid URL:** [https://ipa.247sports.com/rdb/v1/rankings/1/2026/transferPortalPlayerfeed/?pageSize=50](https://ipa.247sports.com/rdb/v1/rankings/1/2026/transferPortalPlayerfeed/?pageSize=50)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | year path parameter. |
| `sport_key` | `sport_key` |  |  | `Y` | 247Sports sport key (1 = football, 2 = basketball). |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns {#sports247_transfer_portal_player_feed-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `key` | Int64 | 247Sports player key. |
| `target_institution` | String | Name of the program the player is targeting / committed to. |
| `target_institution_key` | String | 247Sports institution key of the target program. |
| `full_name` | String | Full name of the transfer player. |
| `position_abbr` | String | Position abbreviation. |
| `current_institution` | String | Name of the player's current program. |
| `current_institution_key` | Int64 | 247Sports institution key of the current program. |
| `current_institution_state_abbreviation` | String | State abbreviation of the current program. |
| `current_institution_city` | String | City of the current program. |
| `first_name` | String | Given name of the transfer player. |
| `last_name` | String | Family (surname) of the transfer player. |
| `state_abbreviation` | String | Player's state abbreviation. |
| `player_image` | String | CDN URL of the player's headshot. |
| `star_rating` | Int64 | 247Sports star rating (2-5). |
| `group_rank` | Int64 | Rank within the transfer position group. |
| `position_rank` | Int64 | Rank at the player's position among transfers. |
| `state_rank` | Int64 | Rank within the player's state. |
| `formatted_height` | String | Height as a formatted string. |
| `weight` | Float64 | Weight in pounds. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sports247_transfer_portal_player_feed-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sports247_transfer_portal_player_feed-example}

```python
sports247_transfer_portal_player_feed(year=2026, sport_key=1)
```

## sports247_composite_team_ranking_feed

247Sports composite team recruiting-class ranking feed for a sport and class year.

**Endpoint URL:** `GET https://ipa.247sports.com/rdb/v1/rankings/{sport_key}/{year}/compositeTeamRankingFeed/`

**Valid URL:** [https://ipa.247sports.com/rdb/v1/rankings/1/2026/compositeTeamRankingFeed/?pageSize=50](https://ipa.247sports.com/rdb/v1/rankings/1/2026/compositeTeamRankingFeed/?pageSize=50)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | year path parameter. |
| `sport_key` | `sport_key` |  |  | `Y` | 247Sports sport key (1 = football, 2 = basketball). |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns {#sports247_composite_team_ranking_feed-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `name` | String | Short display name of the program. |
| `full_name` | String | Full name of the program (school plus nickname). |
| `state_abbreviation` | String | Program's state abbreviation. |
| `conference_name` | String | Name of the athletic conference the program competes in. |
| `key` | Int64 | 247Sports institution/team key. |
| `logo` | String | CDN URL of the program's logo. |
| `alternate_logo` | String | CDN URL of the program's alternate logo. |
| `position` | Int64 | Row position in the feed. |
| `team_ranking_position` | Int64 | Rank in the high-school team recruiting class. |
| `transfer_ranking_position` | Int64 | Rank in the transfer-portal team class. |
| `overall_rank` | Int64 | 247Sports overall national team-class rank. |
| `composite_overall_rank` | Int64 | Composite overall national team-class rank. |
| `conference_rank` | Int64 | 247Sports class rank within the conference. |
| `composite_conference_rank` | Int64 | Composite class rank within the conference. |
| `five_stars` | Int64 | Count of 247Sports five-star commits. |
| `composite_five_stars` | Int64 | Count of composite five-star commits. |
| `four_stars` | Int64 | Count of 247Sports four-star commits. |
| `composite_four_stars` | Int64 | Count of composite four-star commits. |
| `three_stars` | Int64 | Count of 247Sports three-star commits. |
| `composite_three_stars` | Int64 | Count of composite three-star commits. |
| `average_rating` | Int64 | Average 247Sports commit rating. |
| `composite_average_rating` | Float64 | Average composite commit rating. |
| `rating` | Int64 | Total 247Sports class rating (formula points). |
| `composite_rating` | Float64 | Total composite class rating (formula points). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sports247_composite_team_ranking_feed-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sports247_composite_team_ranking_feed-example}

```python
sports247_composite_team_ranking_feed(year=2026, sport_key=1)
```

## sports247_transfer_portal_team_feed

247Sports transfer-portal team ranking feed for a sport and class year.

**Endpoint URL:** `GET https://ipa.247sports.com/rdb/v1/rankings/{sport_key}/{year}/transferPortalOnlyTeamFeed/`

**Valid URL:** [https://ipa.247sports.com/rdb/v1/rankings/1/2026/transferPortalOnlyTeamFeed/?pageSize=50](https://ipa.247sports.com/rdb/v1/rankings/1/2026/transferPortalOnlyTeamFeed/?pageSize=50)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | year path parameter. |
| `sport_key` | `sport_key` |  |  | `Y` | 247Sports sport key (1 = football, 2 = basketball). |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns {#sports247_transfer_portal_team_feed-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `name` | String | Display name of the program. |
| `key` | Int64 | 247Sports institution/team key. |
| `logo` | String | CDN URL of the program's logo. |
| `alternate_logo` | String | CDN URL of the program's alternate logo. |
| `position` | Int64 | Rank in the transfer-portal team class. |
| `number_of_transfers` | Int64 | Number of incoming transfers in the class. |
| `transfer_points` | Float64 | Total transfer-portal class rating points. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sports247_transfer_portal_team_feed-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sports247_transfer_portal_team_feed-example}

```python
sports247_transfer_portal_team_feed(year=2026, sport_key=1)
```

## sports247_target_predictions

247Sports current expert target predictions ("crystal ball") for a site, class year, and sport.

**Endpoint URL:** `GET https://ipa.247sports.com/rdb/v1/sites/{site_key}/years/{year}/sports/{sport_key}/currentTargetPredictions/`

**Valid URL:** [https://ipa.247sports.com/rdb/v1/sites/1/years/2026/sports/1/currentTargetPredictions/?pageSize=50](https://ipa.247sports.com/rdb/v1/sites/1/years/2026/sports/1/currentTargetPredictions/?pageSize=50)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `site_key` | `site_key` |  | `Y` |  | site_key path parameter. |
| `year` | `year` |  | `Y` |  | year path parameter. |
| `sport_key` | `sport_key` |  |  | `Y` | 247Sports sport key (1 = football, 2 = basketball). |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns {#sports247_target_predictions-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_key` | Int64 | 247Sports player key of the recruit the prediction is about. |
| `player_institution_key` | Int64 | 247Sports institution key predicted for the recruit. |
| `prediction_type` | Int64 | Numeric prediction-type discriminator. |
| `rating` | Float64 | Recruit's 247Sports rating at prediction time. |
| `star_rating` | Int64 | Recruit's star rating (2-5). |
| `position` | String | Recruit's position abbreviation. |
| `weight` | Float64 | Recruit's weight in pounds. |
| `height` | String | Recruit's height as a formatted string. |
| `prediction` | String | Predicted destination program name. |
| `prediction_level` | Int64 | Expert confidence level of the prediction. |
| `image` | String | CDN URL of the predicted program's logo. |
| `alt_image` | String | CDN URL of the predicted program's alternate logo. |
| `light_image` | String | CDN URL of the predicted program's light-background logo. |
| `player_name` | String | Recruit's full name. |
| `player_image` | String | CDN URL of the recruit's headshot. |
| `prediction_date` | String | Timestamp the prediction was made. |
| `expert_name` | String | Name of the expert who made the prediction. |
| `expert_alias` | String | 247Sports handle of the expert. |
| `expert_key` | Int64 | 247Sports key of the expert. |
| `expert_role` | String | Expert's role/title. |
| `expert_image` | String | CDN URL of the expert's avatar. |
| `expert_prediction_year` | Int64 | Class year the expert's accuracy stats cover. |
| `expert_yearly_total_correct` | Int64 | Expert's correct predictions this year. |
| `expert_yearly_total_made` | Int64 | Expert's total predictions made this year. |
| `expert_all_time_total_correct` | Int64 | Expert's correct predictions all-time. |
| `expert_all_time_total_made` | Int64 | Expert's total predictions made all-time. |
| `prediction_page_url` | String | 247sports.com path to the prediction detail. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sports247_target_predictions-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sports247_target_predictions-example}

```python
sports247_target_predictions(site_key=1, year=2026, sport_key=1)
```

## sports247_sport_years

Class years for which the 247Sports RDB has data for a given sport.

**Endpoint URL:** `GET https://ipa.247sports.com/rdb/v1/sports/{sport_key}/year/`

**Valid URL:** [https://ipa.247sports.com/rdb/v1/sports/1/year/](https://ipa.247sports.com/rdb/v1/sports/1/year/)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport_key` | `sport_key` |  |  | `Y` | 247Sports sport key (1 = football, 2 = basketball). |

### Returns {#sports247_sport_years-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `value` | Int64 | A class year for which the 247Sports RDB has data for the sport. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sports247_sport_years-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sports247_sport_years-example}

```python
sports247_sport_years(sport_key=1)
```

## sports247_tags_autocomplete

247Sports taggable-entity autocomplete (players / teams / institutions) by name prefix.

**Endpoint URL:** `GET https://ipa.247sports.com/rdb/v1/tags/autocomplete/`

**Valid URL:** [https://ipa.247sports.com/rdb/v1/tags/autocomplete/?defaultName=smith&items=10](https://ipa.247sports.com/rdb/v1/tags/autocomplete/?defaultName=smith&items=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `defaultName` | `default_name` |  |  | `Y` | defaultName query parameter. |
| `items` | `items` |  |  | `Y` | items query parameter. |

### Returns {#sports247_tags_autocomplete-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | String | 247Sports tag id (prefixed key, e.g. Player_46151084). |
| `name` | String | Display name of the tagged entity. |
| `type` | String | Tag entity type (Player, Team, Institution, ...). |
| `annotation` | String | Disambiguating annotation (e.g. class year, position, school). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sports247_tags_autocomplete-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sports247_tags_autocomplete-example}

```python
sports247_tags_autocomplete(default_name='smith')
```

## sports247_positions

247Sports position lookup for a sport (position group, abbreviation, and key).

**Endpoint URL:** `GET https://ipa.247sports.com/rdb/v1/positions/`

**Valid URL:** [https://ipa.247sports.com/rdb/v1/positions/?sportKey=1](https://ipa.247sports.com/rdb/v1/positions/?sportKey=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportKey` | `sport_key` |  |  | `Y` | 247Sports sport key (1 = football, 2 = basketball). |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `rankingKey` | `ranking_key` |  |  | `Y` | rankingKey query parameter. |

### Returns {#sports247_positions-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `group` | String | Position group name (e.g. Quarterback, Running Back). |
| `group_key` | Int64 | 247Sports position group key. |
| `name` | String | Full position group name (e.g. Quarterback). |
| `label` | String | Position abbreviation label (e.g. QB, RB). |
| `value` | String | 247Sports position key (returned as a string). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sports247_positions-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sports247_positions-example}

```python
sports247_positions(sport_key=1)
```
