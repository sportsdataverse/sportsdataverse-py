---
title: "CFB — 247Sports Site Pages (247sports.com) — Season"
sidebar_label: "Season"
sidebar_position: 4
description: "CFB — 247Sports Site Pages (247sports.com) — Season — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# CFB — 247Sports Site Pages (247sports.com) — Season

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
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `player_institution` | integer | Nested player-institution stint the Crystal Ball prediction targets (stringified). |
| `institution` | integer | FK -> predicted destination Institution. |
| `user` | integer | 247Sports user account of the predictor (nested, stringified). |
| `updated_on` | character | Date the prediction was last updated. |
| `prediction_status` | character | Crystal-ball prediction status code. |
| `days_correct` | numeric | Number of days the prediction has stood as correct. |
| `premium` | character | Whether the article is premium content. |
| `score` | numeric | Expert accuracy score at time of prediction. |
| `confidence` | integer | Expert confidence 1-10. |
| `parent` | character | Parent prediction record this entry updates (247Sports field). |
| `is_zero_zone` | character | Whether the prediction fell in 247Sports' zero zone (logged too close to the announcement to earn accuracy credit). |
| `default_name` | integer | Server-rendered display label for the entity. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sports247_site_pages_season_current_expert_predictions-example}

```python
sports247_site_pages_season_current_expert_predictions(season='2026-Football')
```

_Last validated n/a._

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
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `institution` | integer | Nested 247Sports institution the interest event involves (stringified). |
| `recruitment` | integer | Nested 247Sports recruitment the event belongs to (stringified). |
| `recruit_interest` | integer | Nested recruit-interest record the event belongs to (stringified). |
| `type` | character | Record-type category (e.g. `total`, `home`, `road`). |
| `date` | character | Date of the recruiting-interest event, per 247Sports. |
| `default_name` | integer | Server-rendered display label for the entity. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sports247_site_pages_season_recruit_interest_events-example}

```python
sports247_site_pages_season_recruit_interest_events(season='2026-Football')
```

_Last validated n/a._

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
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `recruitment` | integer | FK -> parent Recruitment. |
| `player_sport` | integer | Nested player-sport profile the interest entry belongs to (stringified). |
| `recruit_state` | integer | 247Sports status of the recruit's interest entry for the school (e.g. committed, signed, decommitted). |
| `institution` | integer | FK -> the interested/interesting Institution. |
| `lock_prediction` | character | Crystal Ball lock-prediction value for the school on this recruitment (247Sports field). |
| `recruits_interest` | character | Recruit's stated interest level in the school, per 247Sports. |
| `primary_coach` | numeric | Lead recruiting coach at the school for this recruit. |
| `secondary_coach` | character | Secondary recruiting coach at the school for this recruit. |
| `keeper_coach` | character | Coach designated as the keeper contact for the recruitment (247Sports field). |
| `institutions_interest` | character | School's interest level in the recruit, per 247Sports. |
| `position` | integer | Athlete position. |
| `position_group` | integer | Position group of the recruits (e.g. Offensive Line, Defensive Back). |
| `platoon` | integer | 247Sports platoon (side-of-ball grouping) identifier on the interest entry. |
| `offered` | character | Whether the school has extended an offer. |
| `gray_shirt` | character | Whether the offer or commitment is a grayshirt (delayed enrollment) arrangement. |
| `walk_on` | character | Whether the recruit would join the program as a walk-on. |
| `official_visit` | numeric | Date of the recruit's official visit to the school. |
| `second_official_visit` | character | Date of the recruit's second official visit to the school. |
| `soft_commit` | character | Whether 247Sports marks the commitment as a soft commit. |
| `hard_commit` | numeric | FK -> the RecruitInterestEvent marking a hard commit. |
| `signing_date` | numeric | Date the recruit signed with the school. |
| `enrollment_date` | numeric | Date the recruit enrolled at the school. |
| `decommit` | character | Date the recruit decommitted from the school, when applicable. |
| `offer` | character | Whether the school has extended a scholarship offer to the recruit. |
| `highest_recruit_interest_event` | numeric | Nested highest-signal event on the interest timeline (e.g. commitment) (stringified). |
| `commit_status` | character | Commitment status label (e.g. Committed, Signed). |
| `default_name` | character | Server-rendered display label for the entity. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sports247_site_pages_season_recruit_interests-example}

```python
sports247_site_pages_season_recruit_interests(season='2026-Football')
```

_Last validated n/a._

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
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `player_institution` | integer | Nested player-institution stint behind the recruit row (stringified). |
| `year` | integer | Four-digit season year (e.g. 2019). |
| `announcement_date` | character | Date the recruit announced their decision. |
| `signed_institution` | integer | Nested institution the recruit signed with (stringified). |
| `position` | integer | Athlete position. |
| `institution` | integer | Nested institution the recruit row is scoped to (stringified). |
| `state` | integer | Home state of the recruit, per 247Sports. |
| `player_sport` | integer | Nested player-sport profile for the recruit (stringified). |
| `composite_strength` | integer | Composite strength points contributed to team ranking. |
| `final_choice` | integer | Whether this entry represents the recruit's final school choice. |
| `highest_recruit_interest_event_type` | character | Type of the highest-signal event on the recruit's interest timeline (e.g. commit, signing). |
| `highest_recruit_interest_event` | integer | Nested highest-signal event on the recruit's interest timeline (stringified). |
| `committed_recruit_interest` | integer | Nested interest record for the school the recruit committed to (stringified). |
| `committed_institution` | integer | FK -> committed Institution. |
| `highest_recruit_interest` | integer | Nested interest record carrying the recruit's highest interest signal (stringified). |
| `primary_player_position` | integer | Nested 247Sports record for the recruit's primary position (stringified). |
| `primary_position` | integer | Recruit's primary position on the 247Sports profile. |
| `default_name` | character | Server-rendered display label for the entity. |
| `commited_institution_team_image` | character | Team image asset for the committed institution (the 'commited' spelling is 247Sports' own field name). |
| `recruit_interest_count` | integer | Number of tracked school interests. |
| `recruit_interests_url` | character | Site URL to the recruit's interest timeline. |
| `player_key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `player_first_name` | character | Player's first name |
| `player_last_name` | character | Player's last name |
| `player_full_name` | character | Player full name. |
| `player_height` | character | Participant height (e.g. "6' 5\""). |
| `player_weight` | numeric | Participant weight in pounds. |
| `player_bio` | character | Player biography text authored on 247Sports. |
| `player_scout_evaluation` | character | 247Sports scouting evaluation text for the player. |
| `player_birthdate` | character | Player's date of birth, per 247Sports. |
| `player_modified_user` | character | 247Sports user who last modified the player record. |
| `player_modified_date` | character | Date the player record was last modified. |
| `player_cbs_key` | integer | Cross-reference key into the CBS Sports id space. |
| `player_url` | character | Full stats.ncaa.org url for the player page. |
| `player_last_recruitment_player_institution` | integer | Nested player-institution record from the player's most recent recruitment (stringified). |
| `player_current_player_institution` | integer | FK -> PlayerInstitution (current school). |
| `player_twitter_contact` | numeric | Nested 247Sports contact record for the player's Twitter/X account (stringified). |
| `player_mobile_phone_contact` | character | Player's mobile phone contact field on the 247Sports record. |
| `player_primary_player_sport` | integer | FK -> PlayerSport (`/PlayerSport/{id}.json`). |
| `player_primary_recruitment` | integer | Nested 247Sports record for the player's primary recruitment (stringified). |
| `player_default_name` | character | Server-rendered display label for the entity. |
| `player_default_asset` | integer | Nested 247Sports image asset for the player's headshot (stringified). |
| `player_default_asset_url` | character | URL of the player's headshot image. |
| `player_hero_asset` | character | Nested 247Sports hero (banner) image asset for the player page (stringified). |
| `player_quote_asset` | character | Nested 247Sports image asset used alongside the player's quote block (stringified). |
| `player_user` | character | 247Sports user account linked to the player profile (nested, stringified). |
| `player_pro_stat_player` | integer | Reference tying the profile to a professional stats player record (247Sports field). |
| `player_college_stat_player` | integer | Reference tying the profile to a college stats player record (247Sports field). |
| `player_bio_or_default` | character | Player bio text, falling back to a default blurb when none is authored. |
| `player_rating` | integer | 247Sports numeric rating (0-1 scale) for the primary sport. |
| `player_star_rating` | integer | Star tier (2-5) derived from the rating. |
| `player_national_rank` | integer | Overall national rank in the recruit's class. |
| `player_position_rank` | integer | Rank within position for the class. |
| `player_state_rank` | integer | Rank within home state for the class. |
| `player_hometown_state` | character | State of the player's hometown. |
| `player_hometown_city` | character | City of the player's hometown. |
| `player_player_high_school_name` | character | Name of the player's high school. |
| `player_primary_player_position_abbreviation` | character | Abbreviation of the player's primary position. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sports247_site_pages_season_recruits-example}

```python
sports247_site_pages_season_recruits(season='2026-Football')
```

_Last validated n/a._

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
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `player` | integer | Player name. |
| `player_institution` | integer | Nested player-institution stint the player-sport profile points to (stringified). |
| `state` | integer | Home state of the recruit, per 247Sports. |
| `sport` | integer | Nested 247Sports sport for the profile (stringified). |
| `rating` | integer | 247Sports rating string (0-1). |
| `rating_or_default` | integer | 247Sports in-house rating, falling back to a default value when unrated. |
| `local_index` | integer | 247Sports' own industry-index value for the player, alongside the Rivals and ESPN indexes. |
| `rivals_grade` | numeric | Rivals source grade (industry composite input). |
| `rivals_rank` | integer | Player's rank in the Rivals industry ranking, as tracked by 247Sports. |
| `rivals_index` | numeric | Rivals index value for the player, as tracked by 247Sports. |
| `espn_grade` | integer | ESPN source grade (industry composite input). |
| `espn_rank` | integer | Player's rank in the ESPN industry ranking, as tracked by 247Sports. |
| `espn_index` | numeric | ESPN index value for the player, as tracked by 247Sports. |
| `composite_strength` | integer | Composite strength points (team-ranking weight). |
| `composite_rating` | numeric | 247Sports Composite rating (industry blend). |
| `composite_rating_or_default` | numeric | 247Sports Composite rating, falling back to a default value when unrated. |
| `average_rank` | numeric | Player's average rank across the tracked industry services. |
| `previous_recruitment` | integer | Nested record for the player's previous recruitment (stringified). |
| `primary` | character | Whether this is the player's primary sport. |
| `class_year_override` | character | Override of the player's recruiting class year, when 247Sports reassigns it. |
| `class_year` | character | Recruiting class year. |
| `recruitment` | integer | FK -> Recruitment aggregate for this player-sport. |
| `primary_institution_prediction` | numeric | Nested leading Crystal Ball institution prediction for the player (stringified). |
| `secondary_institution_prediction` | integer | Nested second-place Crystal Ball institution prediction (stringified). |
| `primary_institution_prediction_percentage` | numeric | Share of Crystal Ball predictions favoring the leading institution. |
| `show_unranked_rating` | character | 247Sports display flag to show the rating even while the player is unranked. |
| `current_player_sport_year` | numeric | Current ranking-cycle year for the player-sport profile. |
| `unpublished_player_sport_ranking` | numeric | Nested not-yet-published ranking row for the player (stringified). |
| `current_player_sport_ranking` | numeric | Nested current published ranking row for the player (stringified). |
| `primary_player_position` | integer | Nested 247Sports record for the player's primary position (stringified). |
| `primary_position` | integer | Player's primary position on the 247Sports profile. |
| `primary_position_group` | integer | Position group the player's primary position belongs to. |
| `default_name` | character | Server-rendered display label for the entity. |
| `star_rating` | integer | Star tier (2-5). |
| `secondary_institution_prediction_percentage` | numeric | Share of Crystal Ball predictions favoring the second-place institution. |
| `jersey` | integer | Jersey number. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sports247_site_pages_season_roster_embed-example}

```python
sports247_site_pages_season_roster_embed(season='2020-Football')
```

_Last validated n/a._
