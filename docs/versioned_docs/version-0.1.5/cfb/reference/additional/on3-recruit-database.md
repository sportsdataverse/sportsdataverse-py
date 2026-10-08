---
title: "CFB — additional Python functions — On3 Recruit Database"
sidebar_label: "On3 Recruit Database"
sidebar_position: 4
description: "CFB — additional Python functions — On3 Recruit Database — function reference in sdv-py, the SportsDataverse Python package."
---
# CFB — additional Python functions — On3 Recruit Database

### on3_industry_player_rankings {#on3_industry_player_rankings}

`on3_industry_player_rankings(year: 'Union[int, str]', sport_slug: 'str' = 'football', page: 'Any' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

On3 Industry Comparison player rankings (**deprecated** next/data` scrape).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `year` | `Union[int, str]` |  | recruiting class year (e.g. `2026`). |
| `sport_slug` | `str` | `'football'` | On3 sport slug (default `"football"`). |
| `page` | `Any` | `None` | 1-based page number, or `None` for the first page. |
| `return_parsed` | `bool` | `True` | return a tidy frame (default); `False` returns the raw dict. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

One row per recruit (consensus On3/Rivals/247/ESPN). Zero-row frame on empty.

| col_name | type | description |
|---|---|---|
| `key` | integer |  |
| `ranking_key` | integer |  |
| `position_key` | integer |  |
| `position_abbreviation` | character | Position abbreviation (e.g. `QB`). |
| `state_key` | integer |  |
| `state_abbreviation` | character |  |
| `overall_rank` | integer | Overall recruit ranking (top recruits only; may be `NA`). |
| `consensus_overall_rank` | integer |  |
| `recruitment_key` | integer |  |
| `nearly_five_star_plus` | logical |  |
| `five_star_plus` | logical |  |
| `state_rank` | integer | State ranking. |
| `position_rank` | integer | Position ranking. |
| `consensus_state_rank` | integer |  |
| `consensus_position_rank` | integer |  |
| `nil_value` | integer |  |
| `whisper` | double |  |
| `whisper_change` | double |  |
| `valuation_change` | integer |  |
| `jersey_number` | integer |  |
| `high_school_rating` | character |  |
| `ratings` | character |  |
| `person_default_sport_key` | integer |  |
| `person_default_sport_name` | character |  |
| `person_default_sport_slug` | character |  |
| `person_default_sport_abbreviation` | character |  |
| `person_default_sport_is_rankable` | logical |  |
| `person_default_sport_is_industry_rankable` | logical |  |
| `person_default_sport_is_scoutable` | logical |  |
| `person_rating_consensus_rating` | double |  |
| `person_rating_consensus_stars` | integer |  |
| `person_rating_consensus_national_rank` | integer |  |
| `person_rating_consensus_position_rank` | integer |  |
| `person_rating_consensus_state_rank` | integer |  |
| `person_rating_key` | integer |  |
| `person_rating_rating` | integer |  |
| `person_rating_stars` | integer |  |
| `person_rating_national_rank` | integer |  |
| `person_rating_position_rank` | integer |  |
| `person_rating_state_rank` | integer |  |
| `person_rating_position_abbr` | character |  |
| `person_rating_state_abbr` | character |  |
| `person_rating_five_star_plus` | logical |  |
| `person_status_is_committed` | logical |  |
| `person_status_is_signed` | logical |  |
| `person_status_is_transfer` | logical |  |
| `person_status_is_enrolled` | logical |  |
| `person_status_commitment_date` | character |  |
| `person_status_committed_organization_key` | integer |  |
| `person_status_committed_organization_slug` | character |  |
| `person_status_committed_organization_asset_url` | character |  |
| `person_status_committed_organization_asset_key` | integer |  |
| `person_status_committed_organization_asset_domain_override` | character |  |
| `person_status_committed_organization_asset_domain` | character |  |
| `person_status_committed_organization_asset_source_override` | character |  |
| `person_status_committed_organization_asset_source` | character |  |
| `person_status_committed_organization_asset_title` | character |  |
| `person_status_committed_organization_asset_description` | character |  |
| `person_status_committed_organization_asset_caption` | character |  |
| `person_status_committed_organization_asset_category` | character |  |
| `person_status_committed_organization_asset_alt_text` | character |  |
| `person_status_committed_organization_asset_height` | integer |  |
| `person_status_committed_organization_asset_width` | integer |  |
| `person_status_committed_organization_asset_asset_type` | character |  |
| `person_status_committed_organization_asset_file_system` | character |  |
| `person_status_committed_organization_asset_path` | character |  |
| `person_status_committed_organization_asset_type` | character |  |
| `person_status_committed_organization_asset_thumbnail` | character |  |
| `person_status_committed_organization_asset_duration` | integer |  |
| `person_status_committed_organization_asset_mime_type` | character |  |
| `person_status_committed_organization_asset_credit_line` | character |  |
| `person_status_committed_organization_asset_provenance` | character |  |
| `person_status_committed_organization_asset_rights_expires_utc` | character |  |
| `person_status_committed_organization_primary_color` | character |  |
| `person_status_transferred_from_organization_asset_url` | character |  |
| `person_status_transferred_from_organization_slug` | character |  |
| `person_status_highest_interest_level` | integer |  |
| `person_status_interest_count` | integer |  |
| `person_status_recruitment_year` | integer |  |
| `person_status_sport_name` | character |  |
| `person_status_short_term_signee` | logical |  |
| `person_predictions` | character |  |
| `person_tags` | character |  |
| `person_key` | integer |  |
| `person_name` | character |  |
| `person_slug` | character |  |
| `person_high_school_name` | character |  |
| `person_high_school_key` | integer |  |
| `person_high_school_full_name` | character |  |
| `person_high_school_name_2` | character |  |
| `person_high_school_known_as` | character |  |
| `person_high_school_mascot` | character |  |
| `person_high_school_abbreviation` | character |  |
| `person_high_school_asset_url` | character |  |
| `person_high_school_default_asset_key` | integer |  |
| `person_high_school_default_asset_domain_override` | character |  |
| `person_high_school_default_asset_domain` | character |  |
| `person_high_school_default_asset_source_override` | character |  |
| `person_high_school_default_asset_source` | character |  |
| `person_high_school_default_asset_title` | character |  |
| `person_high_school_default_asset_description` | character |  |
| `person_high_school_default_asset_caption` | character |  |
| `person_high_school_default_asset_category` | character |  |
| `person_high_school_default_asset_alt_text` | character |  |
| `person_high_school_default_asset_height` | integer |  |
| `person_high_school_default_asset_width` | integer |  |
| `person_high_school_default_asset_asset_type` | character |  |
| `person_high_school_default_asset_file_system` | character |  |
| `person_high_school_default_asset_path` | character |  |
| `person_high_school_default_asset_type` | character |  |
| `person_high_school_default_asset_thumbnail` | character |  |
| `person_high_school_default_asset_duration` | integer |  |
| `person_high_school_default_asset_mime_type` | character |  |
| `person_high_school_default_asset_credit_line` | character |  |
| `person_high_school_default_asset_provenance` | character |  |
| `person_high_school_default_asset_rights_expires_utc` | character |  |
| `person_high_school_slug` | character |  |
| `person_high_school_primary_color` | character |  |
| `person_high_school_org_type` | character |  |
| `person_high_school_org_type_enum` | character |  |
| `person_high_school_division` | character |  |
| `person_high_school_site_keys` | character |  |
| `person_high_school_url_slug` | character |  |
| `person_home_town_name` | character |  |
| `person_default_asset_url` | character |  |
| `person_default_asset_key` | integer |  |
| `person_default_asset_domain_override` | character |  |
| `person_default_asset_domain` | character |  |
| `person_default_asset_source_override` | character |  |
| `person_default_asset_source` | character |  |
| `person_default_asset_title` | character |  |
| `person_default_asset_description` | character |  |
| `person_default_asset_caption` | character |  |
| `person_default_asset_category` | character |  |
| `person_default_asset_alt_text` | character |  |
| `person_default_asset_height` | integer |  |
| `person_default_asset_width` | integer |  |
| `person_default_asset_asset_type` | character |  |
| `person_default_asset_file_system` | character |  |
| `person_default_asset_path` | character |  |
| `person_default_asset_type` | character |  |
| `person_default_asset_thumbnail` | character |  |
| `person_default_asset_duration` | integer |  |
| `person_default_asset_mime_type` | character |  |
| `person_default_asset_credit_line` | character |  |
| `person_default_asset_provenance` | character |  |
| `person_default_asset_rights_expires_utc` | character |  |
| `person_early_signee` | logical |  |
| `person_early_enrollee` | logical |  |
| `person_position_abbreviation` | character |  |
| `person_height` | double |  |
| `person_formatted_height` | character |  |
| `person_weight` | integer |  |
| `person_class_year` | integer |  |
| `person_athlete_verified` | logical |  |
| `person_prospect_verified` | logical |  |
| `person_class_rank` | character |  |
| `person_recruitment_key` | integer |  |
| `person_age` | double |  |
| `college_team_key` | integer |  |
| `college_team_full_name` | character |  |
| `college_team_name` | character |  |
| `college_team_mascot` | character |  |
| `college_team_abbreviation` | character |  |
| `college_team_asset_url` | character |  |
| `college_team_asset_key` | integer |  |
| `college_team_asset_domain_override` | character |  |
| `college_team_asset_domain` | character |  |
| `college_team_asset_source_override` | character |  |
| `college_team_asset_source` | character |  |
| `college_team_asset_title` | character |  |
| `college_team_asset_description` | character |  |
| `college_team_asset_caption` | character |  |
| `college_team_asset_category` | character |  |
| `college_team_asset_alt_text` | character |  |
| `college_team_asset_height` | integer |  |
| `college_team_asset_width` | integer |  |
| `college_team_asset_asset_type` | character |  |
| `college_team_asset_file_system` | character |  |
| `college_team_asset_path` | character |  |
| `college_team_asset_type` | character |  |
| `college_team_asset_thumbnail` | character |  |
| `college_team_asset_duration` | integer |  |
| `college_team_asset_mime_type` | character |  |
| `college_team_asset_credit_line` | character |  |
| `college_team_asset_provenance` | character |  |
| `college_team_asset_rights_expires_utc` | character |  |
| `college_team_slug` | character |  |
| `college_team_url_slug` | character |  |
| `college_team_primary_color` | character |  |

**Example**

```python
from sportsdataverse.cfb import on3_players_industry_comparision  # forward RDB native
df = on3_players_industry_comparision(sport_key=1, year=2026)
print(df.shape)
```

### on3_industry_team_rankings {#on3_industry_team_rankings}

`on3_industry_team_rankings(year: 'Union[int, str]', sport_slug: 'str' = 'football', page: 'Any' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

On3 Industry Comparison team rankings (**deprecated** next/data` scrape).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `year` | `Union[int, str]` |  | recruiting class year (e.g. `2026`). |
| `sport_slug` | `str` | `'football'` | On3 sport slug (default `"football"`). |
| `page` | `Any` | `None` | 1-based page number, or `None` for the first page. |
| `return_parsed` | `bool` | `True` | return a tidy frame (default); `False` returns the raw dict. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

One row per team class (consensus ratings). Zero-row frame on empty payload.

| col_name | type | description |
|---|---|---|
| `key` | integer |  |
| `year` | integer | Four-digit season year (e.g. 2019). |
| `applied_total_rating` | double |  |
| `applied_total_consensus_rating` | double |  |
| `applied_average_rating` | double |  |
| `applied_average_consensus_rating` | double |  |
| `commits` | integer | Number of commits in the position group. |
| `applied_commits` | integer |  |
| `deductions` | double |  |
| `deductions_description` | character |  |
| `five_stars` | integer |  |
| `consensus_five_stars` | integer |  |
| `four_stars` | integer |  |
| `consensus_four_stars` | integer |  |
| `three_stars` | integer |  |
| `consensus_three_stars` | integer |  |
| `overall_rank` | double | Overall recruit ranking (top recruits only; may be `NA`). |
| `overall_consensus_rank` | double |  |
| `dispay_consensus_score` | double |  |
| `dispay_on3_score` | double |  |
| `average_nil_value` | double |  |
| `conference_rank` | double |  |
| `conference_consensus_rank` | double |  |
| `organization_key` | integer |  |
| `organization_full_name` | character |  |
| `organization_name` | character |  |
| `organization_mascot` | character |  |
| `organization_abbreviation` | character |  |
| `organization_asset_url` | character |  |
| `organization_asset_key` | integer |  |
| `organization_asset_domain_override` | character |  |
| `organization_asset_domain` | character |  |
| `organization_asset_source_override` | character |  |
| `organization_asset_source` | character |  |
| `organization_asset_title` | character |  |
| `organization_asset_description` | character |  |
| `organization_asset_caption` | character |  |
| `organization_asset_category` | character |  |
| `organization_asset_alt_text` | character |  |
| `organization_asset_height` | integer |  |
| `organization_asset_width` | integer |  |
| `organization_asset_asset_type` | character |  |
| `organization_asset_file_system` | character |  |
| `organization_asset_path` | character |  |
| `organization_asset_type` | character |  |
| `organization_asset_thumbnail` | character |  |
| `organization_asset_duration` | integer |  |
| `organization_asset_mime_type` | character |  |
| `organization_asset_credit_line` | character |  |
| `organization_asset_provenance` | character |  |
| `organization_asset_rights_expires_utc` | character |  |
| `organization_slug` | character |  |
| `organization_url_slug` | character |  |
| `organization_primary_color` | character |  |

**Example**

```python
from sportsdataverse.cfb import on3_team_ranking_consensus_team_rankings  # forward RDB native
df = on3_team_ranking_consensus_team_rankings(sport_slug="football", year=2025)
print(df.shape)
```

### on3_player_rankings {#on3_player_rankings}

`on3_player_rankings(year: 'Union[int, str]', sport_slug: 'str' = 'football', page: 'Any' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

On3 player rankings for a class year (**deprecated** next/data` scrape).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `year` | `Union[int, str]` |  | recruiting class year (e.g. `2026`). |
| `sport_slug` | `str` | `'football'` | On3 sport slug (default `"football"`). |
| `page` | `Any` | `None` | 1-based page number, or `None` for the first page. |
| `return_parsed` | `bool` | `True` | return a tidy frame (default); `False` returns the raw dict. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

One row per ranked recruit (On3 ratings). Zero-row frame on empty payload.

| col_name | type | description |
|---|---|---|
| `key` | integer | On3 numeric key of this player-ranking entry (matches person_key). |
| `ranking_key` | integer | On3 key of the ranking edition this row belongs to. |
| `position_key` | integer | On3 numeric key of the position the player is ranked at. |
| `position_abbreviation` | character | Abbreviation of the ranked position (e.g. QB, EDGE). |
| `state_key` | integer | On3 numeric key of the recruit's home state. |
| `state_abbreviation` | character | Two-letter abbreviation of the recruit's home state. |
| `overall_rank` | integer | On3 overall national rank within this ranking edition. |
| `consensus_overall_rank` | integer | Industry-consensus overall national rank (On3/Rivals/247Sports/ESPN blend). |
| `recruitment_key` | integer | On3 key of the player's active recruitment record. |
| `nearly_five_star_plus` | logical | Flag for On3's near-five-star-plus designation. |
| `five_star_plus` | logical | Flag for On3's five-star-plus designation (top of the class). |
| `state_rank` | integer | On3 rank among recruits from the same state. |
| `position_rank` | integer | On3 rank among recruits at the same position. |
| `consensus_state_rank` | integer | Industry-consensus rank within the recruit's state. |
| `consensus_position_rank` | integer | Industry-consensus rank at the recruit's position. |
| `nil_value` | integer | On3 NIL (name/image/likeness) valuation in US dollars. |
| `whisper` | double |  |
| `whisper_change` | double |  |
| `valuation_change` | integer |  |
| `jersey_number` | double | Jersey number worn by the recruit, when listed. |
| `high_school_rating` | character | High-school rating annotation as shipped by On3 (mixed-type; stringified). |
| `ratings` | character | JSON-encoded list of the player's rating rows across editions (year, type, rating, stars, ranks). |
| `person_default_sport_key` | integer | On3 numeric key of the person's primary sport. |
| `person_default_sport_name` | character | Name of the person's primary sport (e.g. Football). |
| `person_default_sport_slug` | character | URL slug of the person's primary sport. |
| `person_default_sport_abbreviation` | character | Abbreviation of the person's primary sport. |
| `person_default_sport_is_rankable` | logical | Whether On3 ranks players in this sport. |
| `person_default_sport_is_industry_rankable` | logical | Whether industry-consensus rankings exist for this sport. |
| `person_default_sport_is_scoutable` | logical | Whether On3 scouting reports exist for this sport. |
| `person_rating_consensus_rating` | double | Industry-consensus numeric rating (0-100 scale). |
| `person_rating_consensus_stars` | integer | Industry-consensus star rating (2-5). |
| `person_rating_consensus_national_rank` | integer | Industry-consensus national rank. |
| `person_rating_consensus_position_rank` | integer | Industry-consensus position rank. |
| `person_rating_consensus_state_rank` | integer | Industry-consensus state rank. |
| `person_rating_key` | integer | On3 key of the person's current rating record. |
| `person_rating_rating` | integer | On3's own numeric rating (0-100 scale). |
| `person_rating_stars` | integer | On3's own star rating (2-5). |
| `person_rating_national_rank` | integer | On3's own national rank. |
| `person_rating_position_rank` | integer | On3's own position rank. |
| `person_rating_state_rank` | integer | On3's own state rank. |
| `person_rating_position_abbr` | character | Position abbreviation attached to the current rating. |
| `person_rating_state_abbr` | character | State abbreviation attached to the current rating. |
| `person_rating_five_star_plus` | logical | Five-star-plus flag on the current On3 rating. |
| `person_status_is_committed` | logical | Whether the recruit is currently committed to a program. |
| `person_status_is_signed` | logical | Whether the recruit has signed with a program. |
| `person_status_is_transfer` | logical | Whether the entry reflects a transfer-portal recruitment. |
| `person_status_is_enrolled` | logical | Whether the recruit is enrolled at the committed program. |
| `person_status_commitment_date` | character | Timestamp of the current commitment. |
| `person_status_committed_organization_key` | integer | On3 key of the committed program. |
| `person_status_committed_organization_slug` | character | URL slug of the committed program. |
| `person_status_committed_organization_asset_url` | character | Full CDN URL of the committed program's logo asset. |
| `person_status_committed_organization_asset_key` | integer | On3 asset key of the committed program's logo asset. |
| `person_status_committed_organization_asset_domain_override` | character | CDN domain override for the committed program's logo asset (usually null). |
| `person_status_committed_organization_asset_domain` | character | CDN domain serving the committed program's logo asset. |
| `person_status_committed_organization_asset_source_override` | character | Source-path override for the committed program's logo asset (usually null). |
| `person_status_committed_organization_asset_source` | character | CDN-relative source path of the committed program's logo asset. |
| `person_status_committed_organization_asset_title` | character | Editorial title attached to the committed program's logo asset. |
| `person_status_committed_organization_asset_description` | character | Editorial description attached to the committed program's logo asset (usually null). |
| `person_status_committed_organization_asset_caption` | character | Editorial caption attached to the committed program's logo asset (usually null). |
| `person_status_committed_organization_asset_category` | character | Editorial category label of the committed program's logo asset (usually null). |
| `person_status_committed_organization_asset_alt_text` | character | Accessibility alt text of the committed program's logo asset (usually null). |
| `person_status_committed_organization_asset_height` | integer | Pixel height of the committed program's logo asset. |
| `person_status_committed_organization_asset_width` | integer | Pixel width of the committed program's logo asset. |
| `person_status_committed_organization_asset_asset_type` | character | On3 asset-type discriminator of the committed program's logo asset (e.g. Image). |
| `person_status_committed_organization_asset_file_system` | character | Storage file-system flag of the committed program's logo asset. |
| `person_status_committed_organization_asset_path` | character | Storage path of the committed program's logo asset. |
| `person_status_committed_organization_asset_type` | character | Media type field of the committed program's logo asset. |
| `person_status_committed_organization_asset_thumbnail` | character | Thumbnail variant of the committed program's logo asset (video assets; usually null). |
| `person_status_committed_organization_asset_duration` | integer | Duration of the committed program's logo asset when it is a video (usually null). |
| `person_status_committed_organization_asset_mime_type` | character | MIME type of the committed program's logo asset. |
| `person_status_committed_organization_asset_credit_line` | character |  |
| `person_status_committed_organization_asset_provenance` | character |  |
| `person_status_committed_organization_asset_rights_expires_utc` | character |  |
| `person_status_committed_organization_primary_color` | character | Primary hex color of the committed program. |
| `person_status_transferred_from_organization_asset_url` | character | Full CDN URL of the transfer-origin program's logo asset. |
| `person_status_transferred_from_organization_slug` | character | URL slug of the program the player transferred from. |
| `person_status_highest_interest_level` | integer | Highest recruiting-interest level recorded for the recruit. |
| `person_status_interest_count` | integer | Number of recorded recruiting interests. |
| `person_status_recruitment_year` | integer | Recruiting class year of the active recruitment. |
| `person_status_sport_name` | character | Sport of the active recruitment. |
| `person_status_short_term_signee` | logical | Flag for short-term signee status. |
| `person_predictions` | character | JSON-encoded On3 RPM (prediction machine) entries for the recruit. |
| `person_tags` | character | JSON-encoded editorial tags attached to the person. |
| `person_key` | integer | On3 numeric key of the person. |
| `person_name` | character | Recruit's display name. |
| `person_slug` | character | URL slug of the recruit's On3 profile. |
| `person_high_school_name` | character | High-school display name (top-level person field). |
| `person_high_school_key` | integer | On3 numeric key of the recruit's high school. |
| `person_high_school_full_name` | character | Full name of the recruit's high school. |
| `person_high_school_name_2` | character | Name field of the nested high-school object (deduplicated from person_high_school_name). |
| `person_high_school_known_as` | character | Common short name of the high school. |
| `person_high_school_mascot` | character | Mascot of the person's high school. |
| `person_high_school_abbreviation` | character | High-school abbreviation. |
| `person_high_school_asset_url` | character | Convenience CDN URL of the high-school logo (top-level school field). |
| `person_high_school_default_asset_key` | double | On3 asset key of the recruit's high-school logo asset. |
| `person_high_school_default_asset_domain_override` | double | CDN domain override for the recruit's high-school logo asset (usually null). |
| `person_high_school_default_asset_domain` | character | CDN domain serving the recruit's high-school logo asset. |
| `person_high_school_default_asset_source_override` | double | Source-path override for the recruit's high-school logo asset (usually null). |
| `person_high_school_default_asset_source` | character | CDN-relative source path of the recruit's high-school logo asset. |
| `person_high_school_default_asset_title` | character | Editorial title attached to the recruit's high-school logo asset. |
| `person_high_school_default_asset_description` | double | Editorial description attached to the recruit's high-school logo asset (usually null). |
| `person_high_school_default_asset_caption` | double | Editorial caption attached to the recruit's high-school logo asset (usually null). |
| `person_high_school_default_asset_category` | double | Editorial category label of the recruit's high-school logo asset (usually null). |
| `person_high_school_default_asset_alt_text` | double | Accessibility alt text of the recruit's high-school logo asset (usually null). |
| `person_high_school_default_asset_height` | double | Pixel height of the recruit's high-school logo asset. |
| `person_high_school_default_asset_width` | double | Pixel width of the recruit's high-school logo asset. |
| `person_high_school_default_asset_asset_type` | character | On3 asset-type discriminator of the recruit's high-school logo asset (e.g. Image). |
| `person_high_school_default_asset_file_system` | character | Storage file-system flag of the recruit's high-school logo asset. |
| `person_high_school_default_asset_path` | character | Storage path of the recruit's high-school logo asset. |
| `person_high_school_default_asset_type` | character | Media type field of the recruit's high-school logo asset. |
| `person_high_school_default_asset_thumbnail` | double | Thumbnail variant of the recruit's high-school logo asset (video assets; usually null). |
| `person_high_school_default_asset_duration` | double | Duration of the recruit's high-school logo asset when it is a video (usually null). |
| `person_high_school_default_asset_mime_type` | character | MIME type of the recruit's high-school logo asset. |
| `person_high_school_default_asset_credit_line` | double |  |
| `person_high_school_default_asset_provenance` | double |  |
| `person_high_school_default_asset_rights_expires_utc` | double |  |
| `person_high_school_slug` | character | URL slug of the high school on On3. |
| `person_high_school_primary_color` | character | Primary hex color of the high school. |
| `person_high_school_org_type` | character | Organization type label of the school (e.g. High School). |
| `person_high_school_org_type_enum` | character | Numeric enum of the school organization type. |
| `person_high_school_division` | character | Division/classification of the high school. |
| `person_high_school_site_keys` | character | JSON-encoded On3 site keys covering the school. |
| `person_high_school_url_slug` | character | URL slug variant of the school page. |
| `person_home_town_name` | character | Recruit's home town. |
| `person_default_asset_url` | character | Full CDN URL of the recruit's headshot asset. |
| `person_default_asset_key` | integer | On3 asset key of the recruit's headshot asset. |
| `person_default_asset_domain_override` | character | CDN domain override for the recruit's headshot asset (usually null). |
| `person_default_asset_domain` | character | CDN domain serving the recruit's headshot asset. |
| `person_default_asset_source_override` | character | Source-path override for the recruit's headshot asset (usually null). |
| `person_default_asset_source` | character | CDN-relative source path of the recruit's headshot asset. |
| `person_default_asset_title` | character | Editorial title attached to the recruit's headshot asset. |
| `person_default_asset_description` | character | Editorial description attached to the recruit's headshot asset (usually null). |
| `person_default_asset_caption` | character | Editorial caption attached to the recruit's headshot asset (usually null). |
| `person_default_asset_category` | character | Editorial category label of the recruit's headshot asset (usually null). |
| `person_default_asset_alt_text` | character | Accessibility alt text of the recruit's headshot asset (usually null). |
| `person_default_asset_height` | integer | Pixel height of the recruit's headshot asset. |
| `person_default_asset_width` | integer | Pixel width of the recruit's headshot asset. |
| `person_default_asset_asset_type` | character | On3 asset-type discriminator of the recruit's headshot asset (e.g. Image). |
| `person_default_asset_file_system` | character | Storage file-system flag of the recruit's headshot asset. |
| `person_default_asset_path` | character | Storage path of the recruit's headshot asset. |
| `person_default_asset_type` | character | Media type field of the recruit's headshot asset. |
| `person_default_asset_thumbnail` | character | Thumbnail variant of the recruit's headshot asset (video assets; usually null). |
| `person_default_asset_duration` | integer | Duration of the recruit's headshot asset when it is a video (usually null). |
| `person_default_asset_mime_type` | character | MIME type of the recruit's headshot asset. |
| `person_default_asset_credit_line` | character |  |
| `person_default_asset_provenance` | character |  |
| `person_default_asset_rights_expires_utc` | character |  |
| `person_early_signee` | logical | Flag for early-period signees. |
| `person_early_enrollee` | logical | Flag for early enrollees. |
| `person_position_abbreviation` | character | Position abbreviation on the person record. |
| `person_height` | double | Height of the person: a formatted string (e.g. '6-8') or inches, depending on the endpoint. |
| `person_formatted_height` | character | Human-formatted height string (e.g. 6-3.5). |
| `person_weight` | integer | Weight of the person in pounds. |
| `person_class_year` | integer | High-school graduating class year. |
| `person_athlete_verified` | logical | Whether the athlete profile is verified by On3. |
| `person_prospect_verified` | logical | Whether the prospect measurables are verified by On3. |
| `person_class_rank` | character | Rank within the recruit's class on the person record. |
| `person_recruitment_key` | integer | On3 key of the recruitment record on the person object. |
| `person_age` | double | Recruit's age, when known. |
| `college_team_key` | integer | On3 key of the recruit's college program (commits/transfers). |
| `college_team_full_name` | character | Full name of the college program. |
| `college_team_name` | character | Short name of the college program. |
| `college_team_mascot` | character | Mascot of the college program. |
| `college_team_abbreviation` | character | Abbreviation of the college program. |
| `college_team_asset_url` | character | Full CDN URL of the college program's logo asset. |
| `college_team_asset_key` | integer | On3 asset key of the college program's logo asset. |
| `college_team_asset_domain_override` | character | CDN domain override for the college program's logo asset (usually null). |
| `college_team_asset_domain` | character | CDN domain serving the college program's logo asset. |
| `college_team_asset_source_override` | character | Source-path override for the college program's logo asset (usually null). |
| `college_team_asset_source` | character | CDN-relative source path of the college program's logo asset. |
| `college_team_asset_title` | character | Editorial title attached to the college program's logo asset. |
| `college_team_asset_description` | character | Editorial description attached to the college program's logo asset (usually null). |
| `college_team_asset_caption` | character | Editorial caption attached to the college program's logo asset (usually null). |
| `college_team_asset_category` | character | Editorial category label of the college program's logo asset (usually null). |
| `college_team_asset_alt_text` | character | Accessibility alt text of the college program's logo asset (usually null). |
| `college_team_asset_height` | integer | Pixel height of the college program's logo asset. |
| `college_team_asset_width` | integer | Pixel width of the college program's logo asset. |
| `college_team_asset_asset_type` | character | On3 asset-type discriminator of the college program's logo asset (e.g. Image). |
| `college_team_asset_file_system` | character | Storage file-system flag of the college program's logo asset. |
| `college_team_asset_path` | character | Storage path of the college program's logo asset. |
| `college_team_asset_type` | character | Media type field of the college program's logo asset. |
| `college_team_asset_thumbnail` | character | Thumbnail variant of the college program's logo asset (video assets; usually null). |
| `college_team_asset_duration` | integer | Duration of the college program's logo asset when it is a video (usually null). |
| `college_team_asset_mime_type` | character | MIME type of the college program's logo asset. |
| `college_team_asset_credit_line` | character |  |
| `college_team_asset_provenance` | character |  |
| `college_team_asset_rights_expires_utc` | character |  |
| `college_team_slug` | character | URL slug of the college program on On3. |
| `college_team_url_slug` | character |  |
| `college_team_primary_color` | character | Primary hex color of the college program. |
| `person_high_school_default_asset` | double |  |

**Example**

```python
from sportsdataverse.cfb import on3_person_sport_rankings  # forward RDB native
df = on3_person_sport_rankings(sport_key=1, year=2026)
print(df.shape)
```

### on3_team_rankings {#on3_team_rankings}

`on3_team_rankings(year: 'Union[int, str]', sport_slug: 'str' = 'football', page: 'Any' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

On3 team recruiting-class rankings (**deprecated** next/data` scrape).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `year` | `Union[int, str]` |  | recruiting class year (e.g. `2026`). |
| `sport_slug` | `str` | `'football'` | On3 sport slug (default `"football"`). |
| `page` | `Any` | `None` | 1-based page number, or `None` for the first page. |
| `return_parsed` | `bool` | `True` | return a tidy frame (default); `False` returns the raw dict. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

One row per team class (On3 ratings). Zero-row frame on empty payload.

| col_name | type | description |
|---|---|---|
| `key` | integer | On3 numeric key of this team-ranking row. |
| `year` | integer | Recruiting class year of the ranking. |
| `applied_total_rating` | double | Total class rating under On3's team-ranking formula (On3 ratings). |
| `applied_total_consensus_rating` | double | Total class rating under the formula using industry-consensus ratings. |
| `applied_average_rating` | double | Average commit rating counted by the formula (On3 ratings). |
| `applied_average_consensus_rating` | double | Average commit rating counted by the formula (consensus ratings). |
| `commits` | integer | Total number of commits in the class. |
| `applied_commits` | integer | Number of commits counted toward the ranking formula. |
| `deductions` | double | Points deducted by the formula (e.g. over-signing adjustments). |
| `deductions_description` | character | Explanation of the deduction, when present. |
| `five_stars` | integer | Count of On3 five-star commits. |
| `consensus_five_stars` | integer | Count of industry-consensus five-star commits. |
| `four_stars` | integer | Count of On3 four-star commits. |
| `consensus_four_stars` | integer | Count of industry-consensus four-star commits. |
| `three_stars` | integer | Count of On3 three-star commits. |
| `consensus_three_stars` | integer | Count of industry-consensus three-star commits. |
| `overall_rank` | double | On3 national team-class rank. |
| `overall_consensus_rank` | double | Industry-consensus national team-class rank. |
| `dispay_consensus_score` | double | Display string of the consensus class score (field name misspelling is On3's). |
| `dispay_on3_score` | double | Display string of the On3 class score (field name misspelling is On3's). |
| `average_nil_value` | double | Average On3 NIL valuation across the class, in US dollars. |
| `conference_rank` | double | On3 class rank within the program's conference. |
| `conference_consensus_rank` | double | Consensus class rank within the program's conference. |
| `organization_key` | integer | On3 numeric key of the program. |
| `organization_full_name` | character | Full name of the program. |
| `organization_name` | character | Short name of the program. |
| `organization_mascot` | character | Mascot of the program. |
| `organization_abbreviation` | character | Program abbreviation. |
| `organization_asset_url` | character | Full CDN URL of the program's logo asset. |
| `organization_asset_key` | integer | On3 asset key of the program's logo asset. |
| `organization_asset_domain_override` | character | CDN domain override for the program's logo asset (usually null). |
| `organization_asset_domain` | character | CDN domain serving the program's logo asset. |
| `organization_asset_source_override` | character | Source-path override for the program's logo asset (usually null). |
| `organization_asset_source` | character | CDN-relative source path of the program's logo asset. |
| `organization_asset_title` | character | Editorial title attached to the program's logo asset. |
| `organization_asset_description` | character | Editorial description attached to the program's logo asset (usually null). |
| `organization_asset_caption` | character | Editorial caption attached to the program's logo asset (usually null). |
| `organization_asset_category` | character | Editorial category label of the program's logo asset (usually null). |
| `organization_asset_alt_text` | character | Accessibility alt text of the program's logo asset (usually null). |
| `organization_asset_height` | integer | Pixel height of the program's logo asset. |
| `organization_asset_width` | integer | Pixel width of the program's logo asset. |
| `organization_asset_asset_type` | character | On3 asset-type discriminator of the program's logo asset (e.g. Image). |
| `organization_asset_file_system` | character | Storage file-system flag of the program's logo asset. |
| `organization_asset_path` | character | Storage path of the program's logo asset. |
| `organization_asset_type` | character | Media type field of the program's logo asset. |
| `organization_asset_thumbnail` | character | Thumbnail variant of the program's logo asset (video assets; usually null). |
| `organization_asset_duration` | integer | Duration of the program's logo asset when it is a video (usually null). |
| `organization_asset_mime_type` | character | MIME type of the program's logo asset. |
| `organization_asset_credit_line` | character |  |
| `organization_asset_provenance` | character |  |
| `organization_asset_rights_expires_utc` | character |  |
| `organization_slug` | character | URL slug of the program on On3. |
| `organization_url_slug` | character |  |
| `organization_primary_color` | character | Primary hex color of the program. |

**Example**

```python
from sportsdataverse.cfb import on3_team_ranking_team_rankings  # forward RDB native
df = on3_team_ranking_team_rankings(sport_slug="football", year=2025)
print(df.shape)
```
