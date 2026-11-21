# College football — additional Python functions — On3 Recruit Database

> College football — additional Python functions — On3 Recruit Database — function reference in sdv-py, the SportsDataverse Python package.

### on3_industry_player_rankings {#on3_industry_player_rankings}

`on3_industry_player_rankings(year: 'Union[int, str]', sport_slug: 'str' = 'football', page: 'Any' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

On3 Industry Comparison player rankings (**deprecated** `_next/data` scrape).

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
| `key` | Int64 |  |
| `ranking_key` | Int64 |  |
| `position_key` | Int64 |  |
| `position_abbreviation` | String | Position abbreviation (e.g. `QB`). |
| `state_key` | Int64 |  |
| `state_abbreviation` | String |  |
| `overall_rank` | Int64 | Overall recruit ranking (top recruits only; may be `NA`). |
| `consensus_overall_rank` | Int64 |  |
| `recruitment_key` | Int64 |  |
| `nearly_five_star_plus` | Boolean |  |
| `five_star_plus` | Boolean |  |
| `state_rank` | Int64 | State ranking. |
| `position_rank` | Int64 | Position ranking. |
| `consensus_state_rank` | Int64 |  |
| `consensus_position_rank` | Int64 |  |
| `nil_value` | Int64 |  |
| `whisper` | Float64 |  |
| `whisper_change` | Float64 |  |
| `valuation_change` | Int64 |  |
| `jersey_number` | Int64 |  |
| `high_school_rating` | String |  |
| `ratings` | String |  |
| `person_default_sport_key` | Int64 |  |
| `person_default_sport_name` | String |  |
| `person_default_sport_slug` | String |  |
| `person_default_sport_abbreviation` | String |  |
| `person_default_sport_is_rankable` | Boolean |  |
| `person_default_sport_is_industry_rankable` | Boolean |  |
| `person_default_sport_is_scoutable` | Boolean |  |
| `person_rating_consensus_rating` | Float64 |  |
| `person_rating_consensus_stars` | Int64 |  |
| `person_rating_consensus_national_rank` | Int64 |  |
| `person_rating_consensus_position_rank` | Int64 |  |
| `person_rating_consensus_state_rank` | Int64 |  |
| `person_rating_key` | Int64 |  |
| `person_rating_rating` | Int64 |  |
| `person_rating_stars` | Int64 |  |
| `person_rating_national_rank` | Int64 |  |
| `person_rating_position_rank` | Int64 |  |
| `person_rating_state_rank` | Int64 |  |
| `person_rating_position_abbr` | String |  |
| `person_rating_state_abbr` | String |  |
| `person_rating_five_star_plus` | Boolean |  |
| `person_status_is_committed` | Boolean |  |
| `person_status_is_signed` | Boolean |  |
| `person_status_is_transfer` | Boolean |  |
| `person_status_is_enrolled` | Boolean |  |
| `person_status_commitment_date` | String |  |
| `person_status_committed_organization_key` | Int64 |  |
| `person_status_committed_organization_slug` | String |  |
| `person_status_committed_organization_asset_url` | String |  |
| `person_status_committed_organization_asset_key` | Int64 |  |
| `person_status_committed_organization_asset_domain_override` | String |  |
| `person_status_committed_organization_asset_domain` | String |  |
| `person_status_committed_organization_asset_source_override` | String |  |
| `person_status_committed_organization_asset_source` | String |  |
| `person_status_committed_organization_asset_title` | String |  |
| `person_status_committed_organization_asset_description` | String |  |
| `person_status_committed_organization_asset_caption` | String |  |
| `person_status_committed_organization_asset_category` | String |  |
| `person_status_committed_organization_asset_alt_text` | String |  |
| `person_status_committed_organization_asset_height` | Int64 |  |
| `person_status_committed_organization_asset_width` | Int64 |  |
| `person_status_committed_organization_asset_asset_type` | String |  |
| `person_status_committed_organization_asset_file_system` | String |  |
| `person_status_committed_organization_asset_path` | String |  |
| `person_status_committed_organization_asset_type` | String |  |
| `person_status_committed_organization_asset_thumbnail` | String |  |
| `person_status_committed_organization_asset_duration` | Int64 |  |
| `person_status_committed_organization_asset_mime_type` | String |  |
| `person_status_committed_organization_asset_credit_line` | String |  |
| `person_status_committed_organization_asset_provenance` | String |  |
| `person_status_committed_organization_asset_rights_expires_utc` | String |  |
| `person_status_committed_organization_primary_color` | String |  |
| `person_status_transferred_from_organization_asset_url` | String |  |
| `person_status_transferred_from_organization_slug` | String |  |
| `person_status_highest_interest_level` | Int64 |  |
| `person_status_interest_count` | Int64 |  |
| `person_status_recruitment_year` | Int64 |  |
| `person_status_sport_name` | String |  |
| `person_status_short_term_signee` | Boolean |  |
| `person_predictions` | String |  |
| `person_tags` | String |  |
| `person_key` | Int64 |  |
| `person_name` | String |  |
| `person_slug` | String |  |
| `person_high_school_name` | String |  |
| `person_high_school_key` | Int64 |  |
| `person_high_school_full_name` | String |  |
| `person_high_school_name_2` | String |  |
| `person_high_school_known_as` | String |  |
| `person_high_school_mascot` | String |  |
| `person_high_school_abbreviation` | String |  |
| `person_high_school_asset_url` | String |  |
| `person_high_school_default_asset_key` | Int64 |  |
| `person_high_school_default_asset_domain_override` | String |  |
| `person_high_school_default_asset_domain` | String |  |
| `person_high_school_default_asset_source_override` | String |  |
| `person_high_school_default_asset_source` | String |  |
| `person_high_school_default_asset_title` | String |  |
| `person_high_school_default_asset_description` | String |  |
| `person_high_school_default_asset_caption` | String |  |
| `person_high_school_default_asset_category` | String |  |
| `person_high_school_default_asset_alt_text` | String |  |
| `person_high_school_default_asset_height` | Int64 |  |
| `person_high_school_default_asset_width` | Int64 |  |
| `person_high_school_default_asset_asset_type` | String |  |
| `person_high_school_default_asset_file_system` | String |  |
| `person_high_school_default_asset_path` | String |  |
| `person_high_school_default_asset_type` | String |  |
| `person_high_school_default_asset_thumbnail` | String |  |
| `person_high_school_default_asset_duration` | Int64 |  |
| `person_high_school_default_asset_mime_type` | String |  |
| `person_high_school_default_asset_credit_line` | String |  |
| `person_high_school_default_asset_provenance` | String |  |
| `person_high_school_default_asset_rights_expires_utc` | String |  |
| `person_high_school_slug` | String |  |
| `person_high_school_primary_color` | String |  |
| `person_high_school_org_type` | String |  |
| `person_high_school_org_type_enum` | String |  |
| `person_high_school_division` | String |  |
| `person_high_school_site_keys` | String |  |
| `person_high_school_url_slug` | String |  |
| `person_home_town_name` | String |  |
| `person_default_asset_url` | String |  |
| `person_default_asset_key` | Int64 |  |
| `person_default_asset_domain_override` | String |  |
| `person_default_asset_domain` | String |  |
| `person_default_asset_source_override` | String |  |
| `person_default_asset_source` | String |  |
| `person_default_asset_title` | String |  |
| `person_default_asset_description` | String |  |
| `person_default_asset_caption` | String |  |
| `person_default_asset_category` | String |  |
| `person_default_asset_alt_text` | String |  |
| `person_default_asset_height` | Int64 |  |
| `person_default_asset_width` | Int64 |  |
| `person_default_asset_asset_type` | String |  |
| `person_default_asset_file_system` | String |  |
| `person_default_asset_path` | String |  |
| `person_default_asset_type` | String |  |
| `person_default_asset_thumbnail` | String |  |
| `person_default_asset_duration` | Int64 |  |
| `person_default_asset_mime_type` | String |  |
| `person_default_asset_credit_line` | String |  |
| `person_default_asset_provenance` | String |  |
| `person_default_asset_rights_expires_utc` | String |  |
| `person_early_signee` | Boolean |  |
| `person_early_enrollee` | Boolean |  |
| `person_position_abbreviation` | String |  |
| `person_height` | Float64 |  |
| `person_formatted_height` | String |  |
| `person_weight` | Int64 |  |
| `person_class_year` | Int64 |  |
| `person_athlete_verified` | Boolean |  |
| `person_prospect_verified` | Boolean |  |
| `person_class_rank` | String |  |
| `person_recruitment_key` | Int64 |  |
| `person_age` | Float64 |  |
| `college_team_key` | Int64 |  |
| `college_team_full_name` | String |  |
| `college_team_name` | String |  |
| `college_team_mascot` | String |  |
| `college_team_abbreviation` | String |  |
| `college_team_asset_url` | String |  |
| `college_team_asset_key` | Int64 |  |
| `college_team_asset_domain_override` | String |  |
| `college_team_asset_domain` | String |  |
| `college_team_asset_source_override` | String |  |
| `college_team_asset_source` | String |  |
| `college_team_asset_title` | String |  |
| `college_team_asset_description` | String |  |
| `college_team_asset_caption` | String |  |
| `college_team_asset_category` | String |  |
| `college_team_asset_alt_text` | String |  |
| `college_team_asset_height` | Int64 |  |
| `college_team_asset_width` | Int64 |  |
| `college_team_asset_asset_type` | String |  |
| `college_team_asset_file_system` | String |  |
| `college_team_asset_path` | String |  |
| `college_team_asset_type` | String |  |
| `college_team_asset_thumbnail` | String |  |
| `college_team_asset_duration` | Int64 |  |
| `college_team_asset_mime_type` | String |  |
| `college_team_asset_credit_line` | String |  |
| `college_team_asset_provenance` | String |  |
| `college_team_asset_rights_expires_utc` | String |  |
| `college_team_slug` | String |  |
| `college_team_url_slug` | String |  |
| `college_team_primary_color` | String |  |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | `year` / `sport_slug` build no On3 rankings route (a malformed value), or On3 answered 400 / 422. |
| `NoDataError` | The ranking does not exist (the page or data route answered 404). |
| `AssetFetchError` | The fetch failed (any other non-2xx, a connection failure after retries, a page with no buildId, or an empty / non-JSON data body). |

**Example**

```python
from sportsdataverse.cfb import on3_players_industry_comparision  # forward RDB native
df = on3_players_industry_comparision(sport_key=1, year=2026)
print(df.shape)
```

### on3_industry_team_rankings {#on3_industry_team_rankings}

`on3_industry_team_rankings(year: 'Union[int, str]', sport_slug: 'str' = 'football', page: 'Any' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

On3 Industry Comparison team rankings (**deprecated** `_next/data` scrape).

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
| `key` | Int64 |  |
| `year` | Int64 | Four-digit season year (e.g. 2019). |
| `applied_total_rating` | Float64 |  |
| `applied_total_consensus_rating` | Float64 |  |
| `applied_average_rating` | Float64 |  |
| `applied_average_consensus_rating` | Float64 |  |
| `commits` | Int64 | Number of commits in the position group. |
| `applied_commits` | Int64 |  |
| `deductions` | Float64 |  |
| `deductions_description` | String |  |
| `five_stars` | Int64 |  |
| `consensus_five_stars` | Int64 |  |
| `four_stars` | Int64 |  |
| `consensus_four_stars` | Int64 |  |
| `three_stars` | Int64 |  |
| `consensus_three_stars` | Int64 |  |
| `overall_rank` | Float64 | Overall recruit ranking (top recruits only; may be `NA`). |
| `overall_consensus_rank` | Float64 |  |
| `dispay_consensus_score` | Float64 |  |
| `dispay_on3_score` | Float64 |  |
| `average_nil_value` | Float64 |  |
| `conference_rank` | Float64 |  |
| `conference_consensus_rank` | Float64 |  |
| `organization_key` | Int64 |  |
| `organization_full_name` | String |  |
| `organization_name` | String |  |
| `organization_mascot` | String |  |
| `organization_abbreviation` | String |  |
| `organization_asset_url` | String |  |
| `organization_asset_key` | Int64 |  |
| `organization_asset_domain_override` | String |  |
| `organization_asset_domain` | String |  |
| `organization_asset_source_override` | String |  |
| `organization_asset_source` | String |  |
| `organization_asset_title` | String |  |
| `organization_asset_description` | String |  |
| `organization_asset_caption` | String |  |
| `organization_asset_category` | String |  |
| `organization_asset_alt_text` | String |  |
| `organization_asset_height` | Int64 |  |
| `organization_asset_width` | Int64 |  |
| `organization_asset_asset_type` | String |  |
| `organization_asset_file_system` | String |  |
| `organization_asset_path` | String |  |
| `organization_asset_type` | String |  |
| `organization_asset_thumbnail` | String |  |
| `organization_asset_duration` | Int64 |  |
| `organization_asset_mime_type` | String |  |
| `organization_asset_credit_line` | String |  |
| `organization_asset_provenance` | String |  |
| `organization_asset_rights_expires_utc` | String |  |
| `organization_slug` | String |  |
| `organization_url_slug` | String |  |
| `organization_primary_color` | String |  |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | `year` / `sport_slug` build no On3 rankings route (a malformed value), or On3 answered 400 / 422. |
| `NoDataError` | The ranking does not exist (the page or data route answered 404). |
| `AssetFetchError` | The fetch failed (any other non-2xx, a connection failure after retries, a page with no buildId, or an empty / non-JSON data body). |

**Example**

```python
from sportsdataverse.cfb import on3_team_ranking_consensus_team_rankings  # forward RDB native
df = on3_team_ranking_consensus_team_rankings(sport_slug="football", year=2025)
print(df.shape)
```

### on3_player_rankings {#on3_player_rankings}

`on3_player_rankings(year: 'Union[int, str]', sport_slug: 'str' = 'football', page: 'Any' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

On3 player rankings for a class year (**deprecated** `_next/data` scrape).

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
| `key` | Int64 | On3 numeric key of this player-ranking entry (matches person_key). |
| `ranking_key` | Int64 | On3 key of the ranking edition this row belongs to. |
| `position_key` | Int64 | On3 numeric key of the position the player is ranked at. |
| `position_abbreviation` | String | Abbreviation of the ranked position (e.g. QB, EDGE). |
| `state_key` | Int64 | On3 numeric key of the recruit's home state. |
| `state_abbreviation` | String | Two-letter abbreviation of the recruit's home state. |
| `overall_rank` | Int64 | On3 overall national rank within this ranking edition. |
| `consensus_overall_rank` | Int64 | Industry-consensus overall national rank (On3/Rivals/247Sports/ESPN blend). |
| `recruitment_key` | Int64 | On3 key of the player's active recruitment record. |
| `nearly_five_star_plus` | Boolean | Flag for On3's near-five-star-plus designation. |
| `five_star_plus` | Boolean | Flag for On3's five-star-plus designation (top of the class). |
| `state_rank` | Int64 | On3 rank among recruits from the same state. |
| `position_rank` | Int64 | On3 rank among recruits at the same position. |
| `consensus_state_rank` | Int64 | Industry-consensus rank within the recruit's state. |
| `consensus_position_rank` | Int64 | Industry-consensus rank at the recruit's position. |
| `nil_value` | Int64 | On3 NIL (name/image/likeness) valuation in US dollars. |
| `whisper` | Float64 |  |
| `whisper_change` | Float64 |  |
| `valuation_change` | Int64 |  |
| `jersey_number` | Int64 | Jersey number worn by the recruit, when listed. |
| `high_school_rating` | String | High-school rating annotation as shipped by On3 (mixed-type; stringified). |
| `ratings` | String | JSON-encoded list of the player's rating rows across editions (year, type, rating, stars, ranks). |
| `person_default_sport_key` | Int64 | On3 numeric key of the person's primary sport. |
| `person_default_sport_name` | String | Name of the person's primary sport (e.g. Football). |
| `person_default_sport_slug` | String | URL slug of the person's primary sport. |
| `person_default_sport_abbreviation` | String | Abbreviation of the person's primary sport. |
| `person_default_sport_is_rankable` | Boolean | Whether On3 ranks players in this sport. |
| `person_default_sport_is_industry_rankable` | Boolean | Whether industry-consensus rankings exist for this sport. |
| `person_default_sport_is_scoutable` | Boolean | Whether On3 scouting reports exist for this sport. |
| `person_rating_consensus_rating` | Float64 | Industry-consensus numeric rating (0-100 scale). |
| `person_rating_consensus_stars` | Int64 | Industry-consensus star rating (2-5). |
| `person_rating_consensus_national_rank` | Int64 | Industry-consensus national rank. |
| `person_rating_consensus_position_rank` | Int64 | Industry-consensus position rank. |
| `person_rating_consensus_state_rank` | Int64 | Industry-consensus state rank. |
| `person_rating_key` | Int64 | On3 key of the person's current rating record. |
| `person_rating_rating` | Int64 | On3's own numeric rating (0-100 scale). |
| `person_rating_stars` | Int64 | On3's own star rating (2-5). |
| `person_rating_national_rank` | Int64 | On3's own national rank. |
| `person_rating_position_rank` | Int64 | On3's own position rank. |
| `person_rating_state_rank` | Int64 | On3's own state rank. |
| `person_rating_position_abbr` | String | Position abbreviation attached to the current rating. |
| `person_rating_state_abbr` | String | State abbreviation attached to the current rating. |
| `person_rating_five_star_plus` | Boolean | Five-star-plus flag on the current On3 rating. |
| `person_status_is_committed` | Boolean | Whether the recruit is currently committed to a program. |
| `person_status_is_signed` | Boolean | Whether the recruit has signed with a program. |
| `person_status_is_transfer` | Boolean | Whether the entry reflects a transfer-portal recruitment. |
| `person_status_is_enrolled` | Boolean | Whether the recruit is enrolled at the committed program. |
| `person_status_commitment_date` | String | Timestamp of the current commitment. |
| `person_status_committed_organization_key` | Int64 | On3 key of the committed program. |
| `person_status_committed_organization_slug` | String | URL slug of the committed program. |
| `person_status_committed_organization_asset_url` | String | Full CDN URL of the committed program's logo asset. |
| `person_status_committed_organization_asset_key` | Int64 | On3 asset key of the committed program's logo asset. |
| `person_status_committed_organization_asset_domain_override` | String | CDN domain override for the committed program's logo asset (usually null). |
| `person_status_committed_organization_asset_domain` | String | CDN domain serving the committed program's logo asset. |
| `person_status_committed_organization_asset_source_override` | String | Source-path override for the committed program's logo asset (usually null). |
| `person_status_committed_organization_asset_source` | String | CDN-relative source path of the committed program's logo asset. |
| `person_status_committed_organization_asset_title` | String | Editorial title attached to the committed program's logo asset. |
| `person_status_committed_organization_asset_description` | String | Editorial description attached to the committed program's logo asset (usually null). |
| `person_status_committed_organization_asset_caption` | String | Editorial caption attached to the committed program's logo asset (usually null). |
| `person_status_committed_organization_asset_category` | String | Editorial category label of the committed program's logo asset (usually null). |
| `person_status_committed_organization_asset_alt_text` | String | Accessibility alt text of the committed program's logo asset (usually null). |
| `person_status_committed_organization_asset_height` | Int64 | Pixel height of the committed program's logo asset. |
| `person_status_committed_organization_asset_width` | Int64 | Pixel width of the committed program's logo asset. |
| `person_status_committed_organization_asset_asset_type` | String | On3 asset-type discriminator of the committed program's logo asset (e.g. Image). |
| `person_status_committed_organization_asset_file_system` | String | Storage file-system flag of the committed program's logo asset. |
| `person_status_committed_organization_asset_path` | String | Storage path of the committed program's logo asset. |
| `person_status_committed_organization_asset_type` | String | Media type field of the committed program's logo asset. |
| `person_status_committed_organization_asset_thumbnail` | String | Thumbnail variant of the committed program's logo asset (video assets; usually null). |
| `person_status_committed_organization_asset_duration` | Int64 | Duration of the committed program's logo asset when it is a video (usually null). |
| `person_status_committed_organization_asset_mime_type` | String | MIME type of the committed program's logo asset. |
| `person_status_committed_organization_asset_credit_line` | String |  |
| `person_status_committed_organization_asset_provenance` | String |  |
| `person_status_committed_organization_asset_rights_expires_utc` | String |  |
| `person_status_committed_organization_primary_color` | String | Primary hex color of the committed program. |
| `person_status_transferred_from_organization_asset_url` | String | Full CDN URL of the transfer-origin program's logo asset. |
| `person_status_transferred_from_organization_slug` | String | URL slug of the program the player transferred from. |
| `person_status_highest_interest_level` | Int64 | Highest recruiting-interest level recorded for the recruit. |
| `person_status_interest_count` | Int64 | Number of recorded recruiting interests. |
| `person_status_recruitment_year` | Int64 | Recruiting class year of the active recruitment. |
| `person_status_sport_name` | String | Sport of the active recruitment. |
| `person_status_short_term_signee` | Boolean | Flag for short-term signee status. |
| `person_predictions` | String | JSON-encoded On3 RPM (prediction machine) entries for the recruit. |
| `person_tags` | String | JSON-encoded editorial tags attached to the person. |
| `person_key` | Int64 | On3 numeric key of the person. |
| `person_name` | String | Recruit's display name. |
| `person_slug` | String | URL slug of the recruit's On3 profile. |
| `person_high_school_name` | String | High-school display name (top-level person field). |
| `person_high_school_key` | Int64 | On3 numeric key of the recruit's high school. |
| `person_high_school_full_name` | String | Full name of the recruit's high school. |
| `person_high_school_name_2` | String | Name field of the nested high-school object (deduplicated from person_high_school_name). |
| `person_high_school_known_as` | String | Common short name of the high school. |
| `person_high_school_mascot` | String | Mascot of the person's high school. |
| `person_high_school_abbreviation` | String | High-school abbreviation. |
| `person_high_school_asset_url` | String | Convenience CDN URL of the high-school logo (top-level school field). |
| `person_high_school_default_asset_key` | Int64 | On3 asset key of the recruit's high-school logo asset. |
| `person_high_school_default_asset_domain_override` | Float64 | CDN domain override for the recruit's high-school logo asset (usually null). |
| `person_high_school_default_asset_domain` | String | CDN domain serving the recruit's high-school logo asset. |
| `person_high_school_default_asset_source_override` | Float64 | Source-path override for the recruit's high-school logo asset (usually null). |
| `person_high_school_default_asset_source` | String | CDN-relative source path of the recruit's high-school logo asset. |
| `person_high_school_default_asset_title` | String | Editorial title attached to the recruit's high-school logo asset. |
| `person_high_school_default_asset_description` | Float64 | Editorial description attached to the recruit's high-school logo asset (usually null). |
| `person_high_school_default_asset_caption` | Float64 | Editorial caption attached to the recruit's high-school logo asset (usually null). |
| `person_high_school_default_asset_category` | Float64 | Editorial category label of the recruit's high-school logo asset (usually null). |
| `person_high_school_default_asset_alt_text` | Float64 | Accessibility alt text of the recruit's high-school logo asset (usually null). |
| `person_high_school_default_asset_height` | Int64 | Pixel height of the recruit's high-school logo asset. |
| `person_high_school_default_asset_width` | Int64 | Pixel width of the recruit's high-school logo asset. |
| `person_high_school_default_asset_asset_type` | String | On3 asset-type discriminator of the recruit's high-school logo asset (e.g. Image). |
| `person_high_school_default_asset_file_system` | String | Storage file-system flag of the recruit's high-school logo asset. |
| `person_high_school_default_asset_path` | String | Storage path of the recruit's high-school logo asset. |
| `person_high_school_default_asset_type` | String | Media type field of the recruit's high-school logo asset. |
| `person_high_school_default_asset_thumbnail` | Float64 | Thumbnail variant of the recruit's high-school logo asset (video assets; usually null). |
| `person_high_school_default_asset_duration` | Int64 | Duration of the recruit's high-school logo asset when it is a video (usually null). |
| `person_high_school_default_asset_mime_type` | String | MIME type of the recruit's high-school logo asset. |
| `person_high_school_default_asset_credit_line` | Float64 |  |
| `person_high_school_default_asset_provenance` | Float64 |  |
| `person_high_school_default_asset_rights_expires_utc` | Float64 |  |
| `person_high_school_slug` | String | URL slug of the high school on On3. |
| `person_high_school_primary_color` | String | Primary hex color of the high school. |
| `person_high_school_org_type` | String | Organization type label of the school (e.g. High School). |
| `person_high_school_org_type_enum` | String | Numeric enum of the school organization type. |
| `person_high_school_division` | String | Division/classification of the high school. |
| `person_high_school_site_keys` | String | JSON-encoded On3 site keys covering the school. |
| `person_high_school_url_slug` | String | URL slug variant of the school page. |
| `person_home_town_name` | String | Recruit's home town. |
| `person_default_asset_url` | String | Full CDN URL of the recruit's headshot asset. |
| `person_default_asset_key` | Int64 | On3 asset key of the recruit's headshot asset. |
| `person_default_asset_domain_override` | String | CDN domain override for the recruit's headshot asset (usually null). |
| `person_default_asset_domain` | String | CDN domain serving the recruit's headshot asset. |
| `person_default_asset_source_override` | String | Source-path override for the recruit's headshot asset (usually null). |
| `person_default_asset_source` | String | CDN-relative source path of the recruit's headshot asset. |
| `person_default_asset_title` | String | Editorial title attached to the recruit's headshot asset. |
| `person_default_asset_description` | String | Editorial description attached to the recruit's headshot asset (usually null). |
| `person_default_asset_caption` | String | Editorial caption attached to the recruit's headshot asset (usually null). |
| `person_default_asset_category` | String | Editorial category label of the recruit's headshot asset (usually null). |
| `person_default_asset_alt_text` | String | Accessibility alt text of the recruit's headshot asset (usually null). |
| `person_default_asset_height` | Int64 | Pixel height of the recruit's headshot asset. |
| `person_default_asset_width` | Int64 | Pixel width of the recruit's headshot asset. |
| `person_default_asset_asset_type` | String | On3 asset-type discriminator of the recruit's headshot asset (e.g. Image). |
| `person_default_asset_file_system` | String | Storage file-system flag of the recruit's headshot asset. |
| `person_default_asset_path` | String | Storage path of the recruit's headshot asset. |
| `person_default_asset_type` | String | Media type field of the recruit's headshot asset. |
| `person_default_asset_thumbnail` | String | Thumbnail variant of the recruit's headshot asset (video assets; usually null). |
| `person_default_asset_duration` | Int64 | Duration of the recruit's headshot asset when it is a video (usually null). |
| `person_default_asset_mime_type` | String | MIME type of the recruit's headshot asset. |
| `person_default_asset_credit_line` | String |  |
| `person_default_asset_provenance` | String |  |
| `person_default_asset_rights_expires_utc` | String |  |
| `person_early_signee` | Boolean | Flag for early-period signees. |
| `person_early_enrollee` | Boolean | Flag for early enrollees. |
| `person_position_abbreviation` | String | Position abbreviation on the person record. |
| `person_height` | Float64 | Height of the person: a formatted string (e.g. '6-8') or inches, depending on the endpoint. |
| `person_formatted_height` | String | Human-formatted height string (e.g. 6-3.5). |
| `person_weight` | Int64 | Weight of the person in pounds. |
| `person_class_year` | Int64 | High-school graduating class year. |
| `person_athlete_verified` | Boolean | Whether the athlete profile is verified by On3. |
| `person_prospect_verified` | Boolean | Whether the prospect measurables are verified by On3. |
| `person_class_rank` | String | Rank within the recruit's class on the person record. |
| `person_recruitment_key` | Int64 | On3 key of the recruitment record on the person object. |
| `person_age` | Float64 | Recruit's age, when known. |
| `college_team_key` | Int64 | On3 key of the recruit's college program (commits/transfers). |
| `college_team_full_name` | String | Full name of the college program. |
| `college_team_name` | String | Short name of the college program. |
| `college_team_mascot` | String | Mascot of the college program. |
| `college_team_abbreviation` | String | Abbreviation of the college program. |
| `college_team_asset_url` | String | Full CDN URL of the college program's logo asset. |
| `college_team_asset_key` | Int64 | On3 asset key of the college program's logo asset. |
| `college_team_asset_domain_override` | String | CDN domain override for the college program's logo asset (usually null). |
| `college_team_asset_domain` | String | CDN domain serving the college program's logo asset. |
| `college_team_asset_source_override` | String | Source-path override for the college program's logo asset (usually null). |
| `college_team_asset_source` | String | CDN-relative source path of the college program's logo asset. |
| `college_team_asset_title` | String | Editorial title attached to the college program's logo asset. |
| `college_team_asset_description` | String | Editorial description attached to the college program's logo asset (usually null). |
| `college_team_asset_caption` | String | Editorial caption attached to the college program's logo asset (usually null). |
| `college_team_asset_category` | String | Editorial category label of the college program's logo asset (usually null). |
| `college_team_asset_alt_text` | String | Accessibility alt text of the college program's logo asset (usually null). |
| `college_team_asset_height` | Int64 | Pixel height of the college program's logo asset. |
| `college_team_asset_width` | Int64 | Pixel width of the college program's logo asset. |
| `college_team_asset_asset_type` | String | On3 asset-type discriminator of the college program's logo asset (e.g. Image). |
| `college_team_asset_file_system` | String | Storage file-system flag of the college program's logo asset. |
| `college_team_asset_path` | String | Storage path of the college program's logo asset. |
| `college_team_asset_type` | String | Media type field of the college program's logo asset. |
| `college_team_asset_thumbnail` | String | Thumbnail variant of the college program's logo asset (video assets; usually null). |
| `college_team_asset_duration` | Int64 | Duration of the college program's logo asset when it is a video (usually null). |
| `college_team_asset_mime_type` | String | MIME type of the college program's logo asset. |
| `college_team_asset_credit_line` | String |  |
| `college_team_asset_provenance` | String |  |
| `college_team_asset_rights_expires_utc` | String |  |
| `college_team_slug` | String | URL slug of the college program on On3. |
| `college_team_url_slug` | String |  |
| `college_team_primary_color` | String | Primary hex color of the college program. |
| `person_high_school_default_asset` | Float64 |  |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | `year` / `sport_slug` build no On3 rankings route (a malformed value), or On3 answered 400 / 422. |
| `NoDataError` | The ranking does not exist (the page or data route answered 404). |
| `AssetFetchError` | The fetch failed (any other non-2xx, a connection failure after retries, a page with no buildId, or an empty / non-JSON data body). |

**Example**

```python
from sportsdataverse.cfb import on3_person_sport_rankings  # forward RDB native
df = on3_person_sport_rankings(sport_key=1, year=2026)
print(df.shape)
```

### on3_team_rankings {#on3_team_rankings}

`on3_team_rankings(year: 'Union[int, str]', sport_slug: 'str' = 'football', page: 'Any' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

On3 team recruiting-class rankings (**deprecated** `_next/data` scrape).

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
| `key` | Int64 | On3 numeric key of this team-ranking row. |
| `year` | Int64 | Recruiting class year of the ranking. |
| `applied_total_rating` | Float64 | Total class rating under On3's team-ranking formula (On3 ratings). |
| `applied_total_consensus_rating` | Float64 | Total class rating under the formula using industry-consensus ratings. |
| `applied_average_rating` | Float64 | Average commit rating counted by the formula (On3 ratings). |
| `applied_average_consensus_rating` | Float64 | Average commit rating counted by the formula (consensus ratings). |
| `commits` | Int64 | Total number of commits in the class. |
| `applied_commits` | Int64 | Number of commits counted toward the ranking formula. |
| `deductions` | Int64 | Points deducted by the formula (e.g. over-signing adjustments). |
| `deductions_description` | String | Explanation of the deduction, when present. |
| `five_stars` | Int64 | Count of On3 five-star commits. |
| `consensus_five_stars` | Int64 | Count of industry-consensus five-star commits. |
| `four_stars` | Int64 | Count of On3 four-star commits. |
| `consensus_four_stars` | Int64 | Count of industry-consensus four-star commits. |
| `three_stars` | Int64 | Count of On3 three-star commits. |
| `consensus_three_stars` | Int64 | Count of industry-consensus three-star commits. |
| `overall_rank` | Int64 | On3 national team-class rank. |
| `overall_consensus_rank` | Int64 | Industry-consensus national team-class rank. |
| `dispay_consensus_score` | Float64 | Display string of the consensus class score (field name misspelling is On3's). |
| `dispay_on3_score` | Float64 | Display string of the On3 class score (field name misspelling is On3's). |
| `average_nil_value` | Float64 | Average On3 NIL valuation across the class, in US dollars. |
| `conference_rank` | Int64 | On3 class rank within the program's conference. |
| `conference_consensus_rank` | Int64 | Consensus class rank within the program's conference. |
| `organization_key` | Int64 | On3 numeric key of the program. |
| `organization_full_name` | String | Full name of the program. |
| `organization_name` | String | Short name of the program. |
| `organization_mascot` | String | Mascot of the program. |
| `organization_abbreviation` | String | Program abbreviation. |
| `organization_asset_url` | String | Full CDN URL of the program's logo asset. |
| `organization_asset_key` | Int64 | On3 asset key of the program's logo asset. |
| `organization_asset_domain_override` | String | CDN domain override for the program's logo asset (usually null). |
| `organization_asset_domain` | String | CDN domain serving the program's logo asset. |
| `organization_asset_source_override` | String | Source-path override for the program's logo asset (usually null). |
| `organization_asset_source` | String | CDN-relative source path of the program's logo asset. |
| `organization_asset_title` | String | Editorial title attached to the program's logo asset. |
| `organization_asset_description` | String | Editorial description attached to the program's logo asset (usually null). |
| `organization_asset_caption` | String | Editorial caption attached to the program's logo asset (usually null). |
| `organization_asset_category` | String | Editorial category label of the program's logo asset (usually null). |
| `organization_asset_alt_text` | String | Accessibility alt text of the program's logo asset (usually null). |
| `organization_asset_height` | Int64 | Pixel height of the program's logo asset. |
| `organization_asset_width` | Int64 | Pixel width of the program's logo asset. |
| `organization_asset_asset_type` | String | On3 asset-type discriminator of the program's logo asset (e.g. Image). |
| `organization_asset_file_system` | String | Storage file-system flag of the program's logo asset. |
| `organization_asset_path` | String | Storage path of the program's logo asset. |
| `organization_asset_type` | String | Media type field of the program's logo asset. |
| `organization_asset_thumbnail` | String | Thumbnail variant of the program's logo asset (video assets; usually null). |
| `organization_asset_duration` | Int64 | Duration of the program's logo asset when it is a video (usually null). |
| `organization_asset_mime_type` | String | MIME type of the program's logo asset. |
| `organization_asset_credit_line` | String |  |
| `organization_asset_provenance` | String |  |
| `organization_asset_rights_expires_utc` | String |  |
| `organization_slug` | String | URL slug of the program on On3. |
| `organization_url_slug` | String |  |
| `organization_primary_color` | String | Primary hex color of the program. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | `year` / `sport_slug` build no On3 rankings route (a malformed value), or On3 answered 400 / 422. |
| `NoDataError` | The ranking does not exist (the page or data route answered 404). |
| `AssetFetchError` | The fetch failed (any other non-2xx, a connection failure after retries, a page with no buildId, or an empty / non-JSON data body). |

**Example**

```python
from sportsdataverse.cfb import on3_team_ranking_team_rankings  # forward RDB native
df = on3_team_ranking_team_rankings(sport_slug="football", year=2025)
print(df.shape)
```
