---
title: CFB — 247Sports Recruit Database (ipa.247sports.com)
sidebar_label: 247Sports Recruit Database (ipa.247sports.com)
sidebar_position: 11
---
# CFB — 247Sports Recruit Database (ipa.247sports.com)

`sportsdataverse.cfb` — 11 endpoints.

## `sports247_teams`

247Sports RDB college team directory (teamId / institutionKey / conference) for a sport.

**Endpoint URL:** `GET https://ipa.247sports.com/rdb/v1/teams/`

**Valid URL:** [https://ipa.247sports.com/rdb/v1/teams/](https://ipa.247sports.com/rdb/v1/teams/)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportKey` | `sport_key` |  |  | `Y` | 247Sports sport key (1 = football, 2 = basketball). |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `institutionType` | `institution_type` |  |  | `Y` | institutionType query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `name` | character | Team display name (school + nickname). |
| `team_id` | integer | 247Sports RDB team id (per-sport). |
| `institution_key` | integer | 247Sports institution key (school-level, sport-agnostic). |
| `conference` | character | Full name of the athletic conference the team competes in (e.g. ACC, Big Ten). |
| `conference_abbreviation` | character | Conference abbreviation. |
| `sport` | character | Sport name (Football, Basketball, ...). |
| `type` | character | Institution type (College, ...). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_teams()
```

_Last validated n/a._

## `sports247_institution_rankings`

247Sports team recruiting-class rankings (247 rank/rating + industry composite) for a sport and class year.

**Endpoint URL:** `GET https://ipa.247sports.com/rdb/v1/rankings/{sport_key}/{year}/institutionrankings/`

**Valid URL:** [https://ipa.247sports.com/rdb/v1/rankings/1/2026/institutionrankings/](https://ipa.247sports.com/rdb/v1/rankings/1/2026/institutionrankings/)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | year path parameter. |
| `sport_key` | `sport_key` |  |  | `Y` | 247Sports sport key (1 = football, 2 = basketball). |
| `pagesize` | `page_size` |  |  | `Y` | pagesize query parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `useComposite` | `use_composite` |  |  | `Y` | useComposite query parameter. |
| `conferenceAbbreviation` | `conference_abbreviation` |  |  | `Y` | conferenceAbbreviation query parameter. |
| `institutionKey` | `institution_key` |  |  | `Y` | institutionKey query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `name` | character | Short display name of the institution as shown on the 247Sports class-ranking page (e.g. USC, Notre Dame). |
| `full_name` | character | Institution full name (school + nickname). |
| `conference_rank` | integer | 247Sports class rank within the conference. |
| `conference_composite_rank` | integer | Composite class rank within the conference. |
| `rank` | integer | 247Sports national team-class rank. |
| `composite_rank` | integer | Industry-composite national team-class rank. |
| `institution_key` | integer | 247Sports institution key (school-level). |
| `team_key` | integer | 247Sports RDB team key (per-sport). |
| `average_rating` | double | Average 247Sports rating of counted commits. |
| `rating` | double | Total 247Sports class rating (team-ranking formula points). |
| `composite_rating` | double | Total composite class rating (formula points). |
| `average_composite_rating` | double | Average composite rating of counted commits. |
| `default_asset` | character | CDN URL of the institution's default logo. |
| `alternate_asset` | character | CDN URL of the institution's alternate logo. |
| `light_asset` | character | CDN URL of the institution's light-background logo. |
| `high_school_ranking_position` | character | Position of the class in the high-school-only ranking. |
| `transfer_points` | character | Transfer-portal points contributed to the class rating. |
| `transfer_number` | integer | Number of incoming transfers counted in the class. |
| `five_stars` | integer | Count of 247Sports five-star commits. |
| `composite_five_stars` | integer | Count of composite five-star commits. |
| `four_stars` | integer | Count of 247Sports four-star commits. |
| `composite_four_stars` | integer | Count of composite four-star commits. |
| `three_stars` | integer | Count of 247Sports three-star commits. |
| `composite_three_stars` | integer | Count of composite three-star commits. |
| `commits` | integer | Number of commits in the class. |
| `site_key` | integer | 247Sports team-site key. |
| `institution_root_path` | character | 247sports.com root path of the institution's team site. |
| `ranking_date` | character | Timestamp the ranking row was last updated. |
| `city` | character | Institution city. |
| `state` | character | Full name of the U.S. state where the institution is located. |
| `state_abbreviation` | character | Institution state abbreviation. |
| `institution_ranking_url` | character | 247sports.com URL of the institution's class-ranking page. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_institution_rankings(year=2026, sport_key=1)
```

_Last validated n/a._

## `sports247_recruits`

247Sports individual recruit rankings for a sport and class year (247 + industry-composite ratings, stars, commit status; paginated).

**Endpoint URL:** `GET https://ipa.247sports.com/rdb/v1/recruits/`

**Valid URL:** [https://ipa.247sports.com/rdb/v1/recruits/?sportKey=1&year=2026](https://ipa.247sports.com/rdb/v1/recruits/?sportKey=1&year=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportKey` | `sport_key` |  |  | `Y` | 247Sports sport key (1 = football, 2 = basketball). |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `pagesize` | `page_size` |  |  | `Y` | pagesize query parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `positionAbbreviation` | `position_abbreviation` |  |  | `Y` | positionAbbreviation query parameter. |
| `stateAbbreviation` | `state_abbreviation` |  |  | `Y` | stateAbbreviation query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | 247Sports player key of the recruit. |
| `cbs_key` | integer | Legacy CBS player key (0 when unmapped). |
| `first_name` | character | Given name of the recruit. |
| `last_name` | character | Family (surname) of the recruit. |
| `profile_url` | character | 247sports.com path to the recruit's profile. |
| `default_asset_url` | character | CDN URL of the recruit's headshot. |
| `primary_position` | character | Recruit's primary position abbreviation. |
| `composite_rating` | double | 247Sports Composite rating (industry-blended 0-1 scale). |
| `composite_star_rating` | integer | 247Sports Composite star rating (2-5). |
| `composite_national_rank` | integer | Composite national rank. |
| `composite_position_rank` | integer | Composite rank at the recruit's position. |
| `composite_state_rank` | integer | Composite rank within the recruit's state. |
| `signed_institution` | double | Signed-program object when present; null (this scalar) for unsigned recruits (see signed_institution_* columns). |
| `home_town_city` | character | Recruit's home-town city. |
| `home_town_state` | character | Recruit's home-town state. |
| `committed_institution_institution_key` | integer | 247Sports institution key of the program the recruit is committed to. |
| `committed_institution_team_key` | integer | 247Sports RDB team key of the program the recruit is committed to. |
| `committed_institution_cbs_key` | integer | Legacy CBS key of the program the recruit is committed to. |
| `committed_institution_name` | character | Short name of the program the recruit is committed to. |
| `committed_institution_abbreviation` | character | Abbreviation of the program the recruit is committed to. |
| `committed_institution_full_name` | character | Full name of the program the recruit is committed to. |
| `current_institution_institution_key` | integer | 247Sports institution key of the recruit's current program. |
| `current_institution_team_key` | double | 247Sports RDB team key of the recruit's current program. |
| `current_institution_cbs_key` | double | Legacy CBS key of the recruit's current program. |
| `current_institution_name` | character | Short name of the recruit's current program. |
| `current_institution_abbreviation` | character | Abbreviation of the recruit's current program. |
| `current_institution_full_name` | character | Full name of the recruit's current program. |
| `signed_institution_institution_key` | double | 247Sports institution key of the program the recruit signed with. |
| `signed_institution_team_key` | double | 247Sports RDB team key of the program the recruit signed with. |
| `signed_institution_cbs_key` | double | Legacy CBS key of the program the recruit signed with. |
| `signed_institution_name` | character | Short name of the program the recruit signed with. |
| `signed_institution_abbreviation` | character | Abbreviation of the program the recruit signed with. |
| `signed_institution_full_name` | character | Full name of the program the recruit signed with. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_recruits(year=2026, sport_key=1)
```

_Last validated n/a._

## `sports247_transfers`

247Sports transfer-portal player entries for a sport and year (paginated).

**Endpoint URL:** `GET https://ipa.247sports.com/rdb/v1/transfers/`

**Valid URL:** [https://ipa.247sports.com/rdb/v1/transfers/?sportKey=1&year=2026](https://ipa.247sports.com/rdb/v1/transfers/?sportKey=1&year=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportKey` | `sport_key` |  |  | `Y` | 247Sports sport key (1 = football, 2 = basketball). |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `pagesize` | `page_size` |  |  | `Y` | pagesize query parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_key` | integer | 247Sports player key of the transfer. |
| `player_first_name` | character | Given name of the transfer player. |
| `player_last_name` | character | Family (surname) of the transfer player. |
| `player_avatar` | character | CDN URL of the transfer's headshot. |
| `player_transfer_date` | character | Timestamp the player entered the transfer portal. |
| `player_transfer_rating` | double | 247Sports transfer rating. |
| `player_high_school_rating` | double | The player's original high-school recruiting rating. |
| `player_rating` | double | 247Sports current rating. |
| `player_star_rating` | double | 247Sports star rating (2-5). |
| `player_transfer_rank` | double | National rank among transfers. |
| `player_high_school_rank` | double | The player's original high-school national rank. |
| `player_rank` | double | 247Sports current national rank. |
| `player_rank_trend` | double | Change in rank since the previous update. |
| `player_institution_status` | character | Transfer status relative to institutions (e.g. Entered, Committed). |
| `player_position` | character | Position abbreviation. |
| `player_position_key` | integer | 247Sports position key. |
| `player_eligibility_type` | character | Eligibility classification (e.g. Grad, Underclassman). |
| `player_eligibility_years` | double | Years of eligibility remaining. |
| `player_state_rank` | double | Rank within the player's state. |
| `player_status` | character | Portal status label. |
| `player_status_date` | character | Timestamp of the latest status change. |
| `player_transfer_source_institution` | character | Name of the program the player is transferring from. |
| `player_transfer_source_institution_key` | integer | 247Sports institution key of the source program. |
| `player_transfer_source_logo` | character | CDN URL of the source program's logo. |
| `player_transfer_source_default_asset` | character | CDN URL of the source program's default logo asset. |
| `player_transfer_source_alternate_asset` | character | CDN URL of the source program's alternate logo asset. |
| `player_transfer_source_light_asset` | character | CDN URL of the source program's light-background logo asset. |
| `player_transfer_source_institution_root_path` | character | 247sports.com root path of the source program's team site. |
| `player_transfer_destination` | character | Name of the program the player is transferring to (null if uncommitted). |
| `player_position_group_key` | integer | 247Sports position-group key. |
| `player_position_group_name` | character | Position-group name (e.g. Quarterback). |
| `player_position_rank` | double | Rank at the player's position among transfers. |
| `player_last_update_date` | character | Timestamp of the last record update. |
| `player_transfer_commit_date_time` | character | Timestamp of the transfer commitment (null if uncommitted). |
| `player_weight` | integer | Weight in pounds. |
| `player_height` | character | Height as a formatted string. |
| `player_player_profile_url` | character | 247sports.com path to the player's profile. |
| `player_start_date` | character | Start date of the transfer window record. |
| `player_end_date` | character | End date of the transfer window record. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_transfers(year=2026, sport_key=1)
```

_Last validated n/a._

## `sports247_coaches`

247Sports coach recruiting rankings for a sport and year (paginated).

**Endpoint URL:** `GET https://ipa.247sports.com/rdb/v1/coaches/`

**Valid URL:** [https://ipa.247sports.com/rdb/v1/coaches/?sportKey=1&year=2026](https://ipa.247sports.com/rdb/v1/coaches/?sportKey=1&year=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportKey` | `sport_key` |  |  | `Y` | 247Sports sport key (1 = football, 2 = basketball). |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | 247Sports coach key. |
| `cbs_key` | integer | Legacy CBS coach key. |
| `first_name` | character | Given name of the coach. |
| `last_name` | character | Family (surname) of the coach. |
| `profile_url` | character | 247sports.com path to the coach's profile. |
| `default_asset_url` | character | CDN URL of the coach's headshot. |
| `composite_rating` | character | Coach's composite recruiting rating (formula points). |
| `current_job` | double | Current-job program object when present; null (this scalar) otherwise (see current_job_* columns). |
| `average_composite_rating` | character | Coach's average composite recruit rating. |
| `overall_rank` | character | Coach's national recruiting rank. |
| `division_rank` | character | Coach's rank within the division. |
| `conference_rank` | character | Coach's rank within the conference. |
| `current_job_institution_key` | double | 247Sports institution key of the coach's current program. |
| `current_job_team_key` | double | 247Sports RDB team key of the coach's current program. |
| `current_job_cbs_key` | double | Legacy CBS key of the coach's current program. |
| `current_job_name` | character | Short name of the coach's current program. |
| `current_job_abbreviation` | character | Abbreviation of the coach's current program. |
| `current_job_full_name` | character | Full name of the coach's current program. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_coaches(year=2026, sport_key=1)
```

_Last validated n/a._

## `sports247_transfer_portal_player_feed`

247Sports transfer-portal player ranking feed for a sport and class year.

**Endpoint URL:** `GET https://ipa.247sports.com/rdb/v1/rankings/{sport_key}/{year}/transferPortalPlayerfeed/`

**Valid URL:** [https://ipa.247sports.com/rdb/v1/rankings/1/2026/transferPortalPlayerfeed/](https://ipa.247sports.com/rdb/v1/rankings/1/2026/transferPortalPlayerfeed/)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | year path parameter. |
| `sport_key` | `sport_key` |  |  | `Y` | 247Sports sport key (1 = football, 2 = basketball). |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | 247Sports player key. |
| `target_institution` | character | Name of the program the player is targeting / committed to. |
| `target_institution_key` | character | 247Sports institution key of the target program. |
| `full_name` | character | Full name of the transfer player. |
| `position_abbr` | character | Position abbreviation. |
| `current_institution` | character | Name of the player's current program. |
| `current_institution_key` | integer | 247Sports institution key of the current program. |
| `current_institution_state_abbreviation` | character | State abbreviation of the current program. |
| `current_institution_city` | character | City of the current program. |
| `first_name` | character | Given name of the transfer player. |
| `last_name` | character | Family (surname) of the transfer player. |
| `state_abbreviation` | character | Player's state abbreviation. |
| `player_image` | character | CDN URL of the player's headshot. |
| `star_rating` | integer | 247Sports star rating (2-5). |
| `group_rank` | integer | Rank within the transfer position group. |
| `position_rank` | integer | Rank at the player's position among transfers. |
| `state_rank` | integer | Rank within the player's state. |
| `formatted_height` | character | Height as a formatted string. |
| `weight` | double | Weight in pounds. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_transfer_portal_player_feed(year=2026, sport_key=1)
```

_Last validated n/a._

## `sports247_composite_team_ranking_feed`

247Sports composite team recruiting-class ranking feed for a sport and class year.

**Endpoint URL:** `GET https://ipa.247sports.com/rdb/v1/rankings/{sport_key}/{year}/compositeTeamRankingFeed/`

**Valid URL:** [https://ipa.247sports.com/rdb/v1/rankings/1/2026/compositeTeamRankingFeed/](https://ipa.247sports.com/rdb/v1/rankings/1/2026/compositeTeamRankingFeed/)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | year path parameter. |
| `sport_key` | `sport_key` |  |  | `Y` | 247Sports sport key (1 = football, 2 = basketball). |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `name` | character | Short display name of the program. |
| `full_name` | character | Full name of the program (school plus nickname). |
| `state_abbreviation` | character | Program's state abbreviation. |
| `conference_name` | character | Name of the athletic conference the program competes in. |
| `key` | integer | 247Sports institution/team key. |
| `logo` | character | CDN URL of the program's logo. |
| `alternate_logo` | character | CDN URL of the program's alternate logo. |
| `position` | integer | Row position in the feed. |
| `team_ranking_position` | integer | Rank in the high-school team recruiting class. |
| `transfer_ranking_position` | integer | Rank in the transfer-portal team class. |
| `overall_rank` | integer | 247Sports overall national team-class rank. |
| `composite_overall_rank` | integer | Composite overall national team-class rank. |
| `conference_rank` | integer | 247Sports class rank within the conference. |
| `composite_conference_rank` | integer | Composite class rank within the conference. |
| `five_stars` | integer | Count of 247Sports five-star commits. |
| `composite_five_stars` | integer | Count of composite five-star commits. |
| `four_stars` | integer | Count of 247Sports four-star commits. |
| `composite_four_stars` | integer | Count of composite four-star commits. |
| `three_stars` | integer | Count of 247Sports three-star commits. |
| `composite_three_stars` | integer | Count of composite three-star commits. |
| `average_rating` | integer | Average 247Sports commit rating. |
| `composite_average_rating` | double | Average composite commit rating. |
| `rating` | integer | Total 247Sports class rating (formula points). |
| `composite_rating` | double | Total composite class rating (formula points). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_composite_team_ranking_feed(year=2026, sport_key=1)
```

_Last validated n/a._

## `sports247_transfer_portal_team_feed`

247Sports transfer-portal team ranking feed for a sport and class year.

**Endpoint URL:** `GET https://ipa.247sports.com/rdb/v1/rankings/{sport_key}/{year}/transferPortalOnlyTeamFeed/`

**Valid URL:** [https://ipa.247sports.com/rdb/v1/rankings/1/2026/transferPortalOnlyTeamFeed/](https://ipa.247sports.com/rdb/v1/rankings/1/2026/transferPortalOnlyTeamFeed/)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | year path parameter. |
| `sport_key` | `sport_key` |  |  | `Y` | 247Sports sport key (1 = football, 2 = basketball). |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `name` | character | Display name of the program. |
| `key` | integer | 247Sports institution/team key. |
| `logo` | character | CDN URL of the program's logo. |
| `alternate_logo` | character | CDN URL of the program's alternate logo. |
| `position` | integer | Rank in the transfer-portal team class. |
| `number_of_transfers` | integer | Number of incoming transfers in the class. |
| `transfer_points` | double | Total transfer-portal class rating points. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_transfer_portal_team_feed(year=2026, sport_key=1)
```

_Last validated n/a._

## `sports247_target_predictions`

247Sports current expert target predictions ("crystal ball") for a site, class year, and sport.

**Endpoint URL:** `GET https://ipa.247sports.com/rdb/v1/sites/{site_key}/years/{year}/sports/{sport_key}/currentTargetPredictions/`

**Valid URL:** [https://ipa.247sports.com/rdb/v1/sites/1/years/2026/sports/1/currentTargetPredictions/](https://ipa.247sports.com/rdb/v1/sites/1/years/2026/sports/1/currentTargetPredictions/)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `site_key` | `site_key` |  | `Y` |  | site_key path parameter. |
| `year` | `year` |  | `Y` |  | year path parameter. |
| `sport_key` | `sport_key` |  |  | `Y` | 247Sports sport key (1 = football, 2 = basketball). |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_key` | integer | 247Sports player key of the recruit the prediction is about. |
| `player_institution_key` | integer | 247Sports institution key predicted for the recruit. |
| `prediction_type` | integer | Numeric prediction-type discriminator. |
| `rating` | double | Recruit's 247Sports rating at prediction time. |
| `star_rating` | integer | Recruit's star rating (2-5). |
| `position` | character | Recruit's position abbreviation. |
| `weight` | double | Recruit's weight in pounds. |
| `height` | character | Recruit's height as a formatted string. |
| `prediction` | character | Predicted destination program name. |
| `prediction_level` | integer | Expert confidence level of the prediction. |
| `image` | character | CDN URL of the predicted program's logo. |
| `alt_image` | character | CDN URL of the predicted program's alternate logo. |
| `light_image` | character | CDN URL of the predicted program's light-background logo. |
| `player_name` | character | Recruit's full name. |
| `player_image` | character | CDN URL of the recruit's headshot. |
| `prediction_date` | character | Timestamp the prediction was made. |
| `expert_name` | character | Name of the expert who made the prediction. |
| `expert_alias` | character | 247Sports handle of the expert. |
| `expert_key` | integer | 247Sports key of the expert. |
| `expert_role` | character | Expert's role/title. |
| `expert_image` | character | CDN URL of the expert's avatar. |
| `expert_prediction_year` | integer | Class year the expert's accuracy stats cover. |
| `expert_yearly_total_correct` | integer | Expert's correct predictions this year. |
| `expert_yearly_total_made` | integer | Expert's total predictions made this year. |
| `expert_all_time_total_correct` | integer | Expert's correct predictions all-time. |
| `expert_all_time_total_made` | integer | Expert's total predictions made all-time. |
| `prediction_page_url` | character | 247sports.com path to the prediction detail. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_target_predictions(site_key=1, year=2026, sport_key=1)
```

_Last validated n/a._

## `sports247_sport_years`

Class years for which the 247Sports RDB has data for a given sport.

**Endpoint URL:** `GET https://ipa.247sports.com/rdb/v1/sports/{sport_key}/year/`

**Valid URL:** [https://ipa.247sports.com/rdb/v1/sports/1/year/](https://ipa.247sports.com/rdb/v1/sports/1/year/)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport_key` | `sport_key` |  |  | `Y` | 247Sports sport key (1 = football, 2 = basketball). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `value` | integer | A class year for which the 247Sports RDB has data for the sport. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_sport_years(sport_key=1)
```

_Last validated n/a._

## `sports247_tags_autocomplete`

247Sports taggable-entity autocomplete (players / teams / institutions) by name prefix.

**Endpoint URL:** `GET https://ipa.247sports.com/rdb/v1/tags/autocomplete/`

**Valid URL:** [https://ipa.247sports.com/rdb/v1/tags/autocomplete/?defaultName=smith](https://ipa.247sports.com/rdb/v1/tags/autocomplete/?defaultName=smith)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `defaultName` | `default_name` |  |  | `Y` | defaultName query parameter. |
| `items` | `items` |  |  | `Y` | items query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | character | 247Sports tag id (prefixed key, e.g. Player_46151084). |
| `name` | character | Display name of the tagged entity. |
| `type` | character | Tag entity type (Player, Team, Institution, ...). |
| `annotation` | character | Disambiguating annotation (e.g. class year, position, school). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_tags_autocomplete(default_name='smith')
```

_Last validated n/a._
