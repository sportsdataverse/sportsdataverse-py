---
title: "College football — 247Sports Site Pages (247sports.com) — Season"
sidebar_label: "Season"
sidebar_position: 4
description: "College football — 247Sports Site Pages (247sports.com) — Season — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# College football — 247Sports Site Pages (247sports.com) — Season

## sports247_site_pages_season_current_expert_predictions

Current expert 'crystal ball' predictions for a season.

**Endpoint URL:** `GET https://247sports.com/Season/{season}/CurrentExpertPredictions.json`

**Valid URL:** [https://247sports.com/Season/2026-Football/CurrentExpertPredictions.json](https://247sports.com/Season/2026-Football/CurrentExpertPredictions.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season path segment in `{year}-{Sport}` form, e.g. `2026-Football`. |

### Returns {#sports247_site_pages_season_current_expert_predictions-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `key` | Int64 | Primary key of this entity (the id used in its `.json` route). |
| `player_institution` | Int64 | Nested player-institution stint the Crystal Ball prediction targets (stringified). |
| `institution` | Int64 | FK -> predicted destination Institution. |
| `user` | Int64 | 247Sports user account of the predictor (nested, stringified). |
| `updated_on` | String | Date the prediction was last updated. |
| `prediction_status` | String | Crystal-ball prediction status code. |
| `days_correct` | Float64 | Number of days the prediction has stood as correct. |
| `premium` | String |  |
| `score` | Float64 | Expert accuracy score at time of prediction. |
| `confidence` | Int64 | Expert confidence 1-10. |
| `parent` | String | Parent prediction record this entry updates (247Sports field). |
| `is_zero_zone` | String | Whether the prediction fell in 247Sports' zero zone (logged too close to the announcement to earn accuracy credit). |
| `default_name` | Int64 | Server-rendered display label for the entity. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sports247_site_pages_season_current_expert_predictions-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sports247_site_pages_season_current_expert_predictions-example}

```python
sports247_site_pages_season_current_expert_predictions(season='2026-Football')
```

## sports247_site_pages_season_recruit_interest_events

Recruit-interest timeline events for a season (offers/visits/commits).

**Endpoint URL:** `GET https://247sports.com/Season/{season}/RecruitInterestEvents.json`

**Valid URL:** [https://247sports.com/Season/2026-Football/RecruitInterestEvents.json](https://247sports.com/Season/2026-Football/RecruitInterestEvents.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season path segment in `{year}-{Sport}` form, e.g. `2026-Football`. |

### Returns {#sports247_site_pages_season_recruit_interest_events-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `key` | Int64 | Primary key of this entity (the id used in its `.json` route). |
| `institution` | Int64 | Nested 247Sports institution the interest event involves (stringified). |
| `recruitment` | Int64 | Nested 247Sports recruitment the event belongs to (stringified). |
| `recruit_interest` | Int64 | Nested recruit-interest record the event belongs to (stringified). |
| `type` | String | Record-type category (e.g. `total`, `home`, `road`). |
| `date` | String | Date of the recruiting-interest event, per 247Sports. |
| `default_name` | Int64 | Server-rendered display label for the entity. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sports247_site_pages_season_recruit_interest_events-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sports247_site_pages_season_recruit_interest_events-example}

```python
sports247_site_pages_season_recruit_interest_events(season='2026-Football')
```

## sports247_site_pages_season_recruit_interests

All recruit interests for a season (paginated).

**Endpoint URL:** `GET https://247sports.com/Season/{season}/RecruitInterests.json`

**Valid URL:** [https://247sports.com/Season/2026-Football/RecruitInterests.json](https://247sports.com/Season/2026-Football/RecruitInterests.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season path segment in `{year}-{Sport}` form, e.g. `2026-Football`. |

### Returns {#sports247_site_pages_season_recruit_interests-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `key` | Int64 | Primary key of this entity (the id used in its `.json` route). |
| `recruitment` | Int64 | FK -> parent Recruitment. |
| `player_sport` | Int64 | Nested player-sport profile the interest entry belongs to (stringified). |
| `recruit_state` | Int64 | 247Sports status of the recruit's interest entry for the school (e.g. committed, signed, decommitted). |
| `institution` | Int64 | FK -> the interested/interesting Institution. |
| `lock_prediction` | String | Crystal Ball lock-prediction value for the school on this recruitment (247Sports field). |
| `recruits_interest` | String | Recruit's stated interest level in the school, per 247Sports. |
| `primary_coach` | Float64 | Lead recruiting coach at the school for this recruit. |
| `secondary_coach` | String | Secondary recruiting coach at the school for this recruit. |
| `keeper_coach` | String | Coach designated as the keeper contact for the recruitment (247Sports field). |
| `institutions_interest` | String | School's interest level in the recruit, per 247Sports. |
| `position` | Int64 | Athlete position. |
| `position_group` | Int64 | Position group of the recruits (e.g. Offensive Line, Defensive Back). |
| `platoon` | Int64 | 247Sports platoon (side-of-ball grouping) identifier on the interest entry. |
| `offered` | String | Whether the school has extended an offer. |
| `gray_shirt` | String | Whether the offer or commitment is a grayshirt (delayed enrollment) arrangement. |
| `walk_on` | String | Whether the recruit would join the program as a walk-on. |
| `official_visit` | Float64 | Date of the recruit's official visit to the school. |
| `second_official_visit` | String | Date of the recruit's second official visit to the school. |
| `soft_commit` | String | Whether 247Sports marks the commitment as a soft commit. |
| `hard_commit` | Float64 | FK -> the RecruitInterestEvent marking a hard commit. |
| `signing_date` | Float64 | Date the recruit signed with the school. |
| `enrollment_date` | Float64 | Date the recruit enrolled at the school. |
| `decommit` | String | Date the recruit decommitted from the school, when applicable. |
| `offer` | String | Whether the school has extended a scholarship offer to the recruit. |
| `highest_recruit_interest_event` | Float64 | Nested highest-signal event on the interest timeline (e.g. commitment) (stringified). |
| `commit_status` | String | Commitment status label (e.g. Committed, Signed). |
| `default_name` | String | Server-rendered display label for the entity. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sports247_site_pages_season_recruit_interests-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sports247_site_pages_season_recruit_interests-example}

```python
sports247_site_pages_season_recruit_interests(season='2026-Football')
```

## sports247_site_pages_season_recruits

Recruit class rankings for a season (rich per-recruit rows with inlined Player).

**Endpoint URL:** `GET https://247sports.com/Season/{season}/Recruits.json`

**Valid URL:** [https://247sports.com/Season/2026-Football/Recruits.json](https://247sports.com/Season/2026-Football/Recruits.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season path segment in `{year}-{Sport}` form, e.g. `2026-Football`. |
| `Items` | `items` |  |  | `Y` | Items query parameter. |
| `Page` | `page` |  |  | `Y` | Page query parameter. |
| `Player.FullName` | `player_full_name` |  |  | `Y` | Player.FullName query parameter. |
| `Institution` | `institution` |  |  | `Y` | Institution query parameter. |

### Returns {#sports247_site_pages_season_recruits-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `key` | Int64 | Primary key of this entity (the id used in its `.json` route). |
| `player_institution` | Int64 | Nested player-institution stint behind the recruit row (stringified). |
| `year` | Int64 | Four-digit season year (e.g. 2019). |
| `announcement_date` | String | Date the recruit announced their decision. |
| `signed_institution` | Int64 | Nested institution the recruit signed with (stringified). |
| `position` | Int64 | Athlete position. |
| `institution` | Int64 | Nested institution the recruit row is scoped to (stringified). |
| `state` | Int64 | Home state of the recruit, per 247Sports. |
| `player_sport` | Int64 | Nested player-sport profile for the recruit (stringified). |
| `composite_strength` | Int64 | Composite strength points contributed to team ranking. |
| `final_choice` | Int64 | Whether this entry represents the recruit's final school choice. |
| `highest_recruit_interest_event_type` | String | Type of the highest-signal event on the recruit's interest timeline (e.g. commit, signing). |
| `highest_recruit_interest_event` | Int64 | Nested highest-signal event on the recruit's interest timeline (stringified). |
| `committed_recruit_interest` | Int64 | Nested interest record for the school the recruit committed to (stringified). |
| `committed_institution` | Int64 | FK -> committed Institution. |
| `highest_recruit_interest` | Int64 | Nested interest record carrying the recruit's highest interest signal (stringified). |
| `primary_player_position` | Int64 | Nested 247Sports record for the recruit's primary position (stringified). |
| `primary_position` | Int64 | Recruit's primary position on the 247Sports profile. |
| `default_name` | String | Server-rendered display label for the entity. |
| `commited_institution_team_image` | String | Team image asset for the committed institution (the 'commited' spelling is 247Sports' own field name). |
| `recruit_interest_count` | Int64 | Number of tracked school interests. |
| `recruit_interests_url` | String | Site URL to the recruit's interest timeline. |
| `player_key` | Int64 | Primary key of this entity (the id used in its `.json` route). |
| `player_first_name` | String |  |
| `player_last_name` | String |  |
| `player_full_name` | String |  |
| `player_height` | String |  |
| `player_weight` | Float64 |  |
| `player_bio` | String | Player biography text authored on 247Sports. |
| `player_scout_evaluation` | String | 247Sports scouting evaluation text for the player. |
| `player_birthdate` | String | Player's date of birth, per 247Sports. |
| `player_modified_user` | String | 247Sports user who last modified the player record. |
| `player_modified_date` | String | Date the player record was last modified. |
| `player_cbs_key` | Int64 | Cross-reference key into the CBS Sports id space. |
| `player_url` | String |  |
| `player_last_recruitment_player_institution` | Int64 | Nested player-institution record from the player's most recent recruitment (stringified). |
| `player_current_player_institution` | Int64 | FK -> PlayerInstitution (current school). |
| `player_twitter_contact` | Float64 | Nested 247Sports contact record for the player's Twitter/X account (stringified). |
| `player_mobile_phone_contact` | String | Player's mobile phone contact field on the 247Sports record. |
| `player_primary_player_sport` | Int64 | FK -> PlayerSport (`/PlayerSport/{id}.json`). |
| `player_primary_recruitment` | Int64 | Nested 247Sports record for the player's primary recruitment (stringified). |
| `player_default_name` | String | Server-rendered display label for the entity. |
| `player_default_asset` | Int64 | Nested 247Sports image asset for the player's headshot (stringified). |
| `player_default_asset_url` | String | URL of the player's headshot image. |
| `player_hero_asset` | String | Nested 247Sports hero (banner) image asset for the player page (stringified). |
| `player_quote_asset` | String | Nested 247Sports image asset used alongside the player's quote block (stringified). |
| `player_user` | String | 247Sports user account linked to the player profile (nested, stringified). |
| `player_pro_stat_player` | Int64 | Reference tying the profile to a professional stats player record (247Sports field). |
| `player_college_stat_player` | Int64 | Reference tying the profile to a college stats player record (247Sports field). |
| `player_bio_or_default` | String | Player bio text, falling back to a default blurb when none is authored. |
| `player_rating` | Int64 | 247Sports numeric rating (0-1 scale) for the primary sport. |
| `player_star_rating` | Int64 | Star tier (2-5) derived from the rating. |
| `player_national_rank` | Int64 | Overall national rank in the recruit's class. |
| `player_position_rank` | Int64 | Rank within position for the class. |
| `player_state_rank` | Int64 | Rank within home state for the class. |
| `player_hometown_state` | String | State of the player's hometown. |
| `player_hometown_city` | String | City of the player's hometown. |
| `player_player_high_school_name` | String | Name of the player's high school. |
| `player_primary_player_position_abbreviation` | String | Abbreviation of the player's primary position. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sports247_site_pages_season_recruits-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sports247_site_pages_season_recruits-example}

```python
sports247_site_pages_season_recruits(season='2026-Football')
```

## sports247_site_pages_season_roster_embed

Signed-class roster embed (PlayerSport rows). Accuracy can lag.

**Endpoint URL:** `GET https://247sports.com/Season/{season}/Roster/Embed.json`

**Valid URL:** [https://247sports.com/Season/2020-Football/Roster/Embed.json](https://247sports.com/Season/2020-Football/Roster/Embed.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season path segment in `{year}-{Sport}` form, e.g. `2026-Football`. |

### Returns {#sports247_site_pages_season_roster_embed-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `key` | Int64 | Primary key of this entity (the id used in its `.json` route). |
| `player` | Int64 | Player name. |
| `player_institution` | Int64 | Nested player-institution stint the player-sport profile points to (stringified). |
| `state` | Int64 | Home state of the recruit, per 247Sports. |
| `sport` | Int64 | Nested 247Sports sport for the profile (stringified). |
| `rating` | Int64 | 247Sports rating string (0-1). |
| `rating_or_default` | Int64 | 247Sports in-house rating, falling back to a default value when unrated. |
| `local_index` | Int64 | 247Sports' own industry-index value for the player, alongside the Rivals and ESPN indexes. |
| `rivals_grade` | Float64 | Rivals source grade (industry composite input). |
| `rivals_rank` | Int64 | Player's rank in the Rivals industry ranking, as tracked by 247Sports. |
| `rivals_index` | Float64 | Rivals index value for the player, as tracked by 247Sports. |
| `espn_grade` | Int64 | ESPN source grade (industry composite input). |
| `espn_rank` | Int64 | Player's rank in the ESPN industry ranking, as tracked by 247Sports. |
| `espn_index` | Float64 | ESPN index value for the player, as tracked by 247Sports. |
| `composite_strength` | Int64 | Composite strength points (team-ranking weight). |
| `composite_rating` | Float64 | 247Sports Composite rating (industry blend). |
| `composite_rating_or_default` | Float64 | 247Sports Composite rating, falling back to a default value when unrated. |
| `average_rank` | Float64 | Player's average rank across the tracked industry services. |
| `previous_recruitment` | Int64 | Nested record for the player's previous recruitment (stringified). |
| `primary` | String | Whether this is the player's primary sport. |
| `class_year_override` | String | Override of the player's recruiting class year, when 247Sports reassigns it. |
| `class_year` | String | Recruiting class year. |
| `recruitment` | Int64 | FK -> Recruitment aggregate for this player-sport. |
| `primary_institution_prediction` | Float64 | Nested leading Crystal Ball institution prediction for the player (stringified). |
| `secondary_institution_prediction` | Int64 | Nested second-place Crystal Ball institution prediction (stringified). |
| `primary_institution_prediction_percentage` | Float64 | Share of Crystal Ball predictions favoring the leading institution. |
| `show_unranked_rating` | String | 247Sports display flag to show the rating even while the player is unranked. |
| `current_player_sport_year` | Float64 | Current ranking-cycle year for the player-sport profile. |
| `unpublished_player_sport_ranking` | Float64 | Nested not-yet-published ranking row for the player (stringified). |
| `current_player_sport_ranking` | Float64 | Nested current published ranking row for the player (stringified). |
| `primary_player_position` | Int64 | Nested 247Sports record for the player's primary position (stringified). |
| `primary_position` | Int64 | Player's primary position on the 247Sports profile. |
| `primary_position_group` | Int64 | Position group the player's primary position belongs to. |
| `default_name` | String | Server-rendered display label for the entity. |
| `star_rating` | Int64 | Star tier (2-5). |
| `secondary_institution_prediction_percentage` | Float64 | Share of Crystal Ball predictions favoring the second-place institution. |
| `jersey` | String | Jersey number. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sports247_site_pages_season_roster_embed-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sports247_site_pages_season_roster_embed-example}

```python
sports247_site_pages_season_roster_embed(season='2020-Football')
```
