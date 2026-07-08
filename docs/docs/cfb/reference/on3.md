---
title: CFB — On3 Recruiting (on3.com)
sidebar_label: On3 Recruiting (on3.com)
sidebar_position: 10
---
# CFB — On3 Recruiting (on3.com)

`sportsdataverse.cfb` — 4 endpoints.

## `on3_player_rankings`

On3 player rankings for a recruiting class year — On3's own ratings (rankingType=player).

**Endpoint URL:** `GET https://www.on3.com/rivals/rankings/player/{sport_slug}/{year}.json`

**Valid URL:** [https://www.on3.com/rivals/rankings/player/football/2026.json](https://www.on3.com/rivals/rankings/player/football/2026.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | year path parameter. |
| `sport_slug` | `sport_slug` |  |  | `Y` | On3 sport slug (e.g. football, basketball). |
| `page` | `page` |  |  | `Y` | page query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
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
| `jersey_number` | integer | Jersey number worn by the recruit, when listed. |
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
| `person_high_school_mascot` | character | High-school mascot. |
| `person_high_school_abbreviation` | character | High-school abbreviation. |
| `person_high_school_asset_url` | character | Convenience CDN URL of the high-school logo (top-level school field). |
| `person_high_school_default_asset_key` | integer | On3 asset key of the recruit's high-school logo asset. |
| `person_high_school_default_asset_domain_override` | character | CDN domain override for the recruit's high-school logo asset (usually null). |
| `person_high_school_default_asset_domain` | character | CDN domain serving the recruit's high-school logo asset. |
| `person_high_school_default_asset_source_override` | character | Source-path override for the recruit's high-school logo asset (usually null). |
| `person_high_school_default_asset_source` | character | CDN-relative source path of the recruit's high-school logo asset. |
| `person_high_school_default_asset_title` | character | Editorial title attached to the recruit's high-school logo asset. |
| `person_high_school_default_asset_description` | character | Editorial description attached to the recruit's high-school logo asset (usually null). |
| `person_high_school_default_asset_caption` | character | Editorial caption attached to the recruit's high-school logo asset (usually null). |
| `person_high_school_default_asset_category` | character | Editorial category label of the recruit's high-school logo asset (usually null). |
| `person_high_school_default_asset_alt_text` | character | Accessibility alt text of the recruit's high-school logo asset (usually null). |
| `person_high_school_default_asset_height` | integer | Pixel height of the recruit's high-school logo asset. |
| `person_high_school_default_asset_width` | integer | Pixel width of the recruit's high-school logo asset. |
| `person_high_school_default_asset_asset_type` | character | On3 asset-type discriminator of the recruit's high-school logo asset (e.g. Image). |
| `person_high_school_default_asset_file_system` | character | Storage file-system flag of the recruit's high-school logo asset. |
| `person_high_school_default_asset_path` | character | Storage path of the recruit's high-school logo asset. |
| `person_high_school_default_asset_type` | character | Media type field of the recruit's high-school logo asset. |
| `person_high_school_default_asset_thumbnail` | character | Thumbnail variant of the recruit's high-school logo asset (video assets; usually null). |
| `person_high_school_default_asset_duration` | integer | Duration of the recruit's high-school logo asset when it is a video (usually null). |
| `person_high_school_default_asset_mime_type` | character | MIME type of the recruit's high-school logo asset. |
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
| `person_early_signee` | logical | Flag for early-period signees. |
| `person_early_enrollee` | logical | Flag for early enrollees. |
| `person_position_abbreviation` | character | Position abbreviation on the person record. |
| `person_height` | double | Height in inches. |
| `person_formatted_height` | character | Human-formatted height string (e.g. 6-3.5). |
| `person_weight` | integer | Weight in pounds. |
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
| `college_team_slug` | character | URL slug of the college program on On3. |
| `college_team_primary_color` | character | Primary hex color of the college program. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_player_rankings(year=2026, sport_slug='football')
```

_Last validated n/a._

## `on3_industry_player_rankings`

On3 Industry Comparison player rankings (consensus across On3/Rivals/247/ESPN; rankingType=industry-player).

**Endpoint URL:** `GET https://www.on3.com/rivals/rankings/industry-player/{sport_slug}/{year}.json`

**Valid URL:** [https://www.on3.com/rivals/rankings/industry-player/football/2026.json](https://www.on3.com/rivals/rankings/industry-player/football/2026.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | year path parameter. |
| `sport_slug` | `sport_slug` |  |  | `Y` | On3 sport slug (e.g. football, basketball). |
| `page` | `page` |  |  | `Y` | page query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
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
| `jersey_number` | integer | Jersey number worn by the recruit, when listed. |
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
| `person_high_school_mascot` | character | High-school mascot. |
| `person_high_school_abbreviation` | character | High-school abbreviation. |
| `person_high_school_asset_url` | character | Convenience CDN URL of the high-school logo (top-level school field). |
| `person_high_school_default_asset_key` | integer | On3 asset key of the recruit's high-school logo asset. |
| `person_high_school_default_asset_domain_override` | character | CDN domain override for the recruit's high-school logo asset (usually null). |
| `person_high_school_default_asset_domain` | character | CDN domain serving the recruit's high-school logo asset. |
| `person_high_school_default_asset_source_override` | character | Source-path override for the recruit's high-school logo asset (usually null). |
| `person_high_school_default_asset_source` | character | CDN-relative source path of the recruit's high-school logo asset. |
| `person_high_school_default_asset_title` | character | Editorial title attached to the recruit's high-school logo asset. |
| `person_high_school_default_asset_description` | character | Editorial description attached to the recruit's high-school logo asset (usually null). |
| `person_high_school_default_asset_caption` | character | Editorial caption attached to the recruit's high-school logo asset (usually null). |
| `person_high_school_default_asset_category` | character | Editorial category label of the recruit's high-school logo asset (usually null). |
| `person_high_school_default_asset_alt_text` | character | Accessibility alt text of the recruit's high-school logo asset (usually null). |
| `person_high_school_default_asset_height` | integer | Pixel height of the recruit's high-school logo asset. |
| `person_high_school_default_asset_width` | integer | Pixel width of the recruit's high-school logo asset. |
| `person_high_school_default_asset_asset_type` | character | On3 asset-type discriminator of the recruit's high-school logo asset (e.g. Image). |
| `person_high_school_default_asset_file_system` | character | Storage file-system flag of the recruit's high-school logo asset. |
| `person_high_school_default_asset_path` | character | Storage path of the recruit's high-school logo asset. |
| `person_high_school_default_asset_type` | character | Media type field of the recruit's high-school logo asset. |
| `person_high_school_default_asset_thumbnail` | character | Thumbnail variant of the recruit's high-school logo asset (video assets; usually null). |
| `person_high_school_default_asset_duration` | integer | Duration of the recruit's high-school logo asset when it is a video (usually null). |
| `person_high_school_default_asset_mime_type` | character | MIME type of the recruit's high-school logo asset. |
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
| `person_early_signee` | logical | Flag for early-period signees. |
| `person_early_enrollee` | logical | Flag for early enrollees. |
| `person_position_abbreviation` | character | Position abbreviation on the person record. |
| `person_height` | double | Height in inches. |
| `person_formatted_height` | character | Human-formatted height string (e.g. 6-3.5). |
| `person_weight` | integer | Weight in pounds. |
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
| `college_team_slug` | character | URL slug of the college program on On3. |
| `college_team_primary_color` | character | Primary hex color of the college program. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_industry_player_rankings(year=2026, sport_slug='football')
```

_Last validated n/a._

## `on3_team_rankings`

On3 team recruiting-class rankings for a class year (rankingType=team).

**Endpoint URL:** `GET https://www.on3.com/rivals/rankings/team/{sport_slug}/{year}.json`

**Valid URL:** [https://www.on3.com/rivals/rankings/team/football/2026.json](https://www.on3.com/rivals/rankings/team/football/2026.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | year path parameter. |
| `sport_slug` | `sport_slug` |  |  | `Y` | On3 sport slug (e.g. football, basketball). |
| `page` | `page` |  |  | `Y` | page query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
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
| `deductions` | integer | Points deducted by the formula (e.g. over-signing adjustments). |
| `deductions_description` | character | Explanation of the deduction, when present. |
| `five_stars` | integer | Count of On3 five-star commits. |
| `consensus_five_stars` | integer | Count of industry-consensus five-star commits. |
| `four_stars` | integer | Count of On3 four-star commits. |
| `consensus_four_stars` | integer | Count of industry-consensus four-star commits. |
| `three_stars` | integer | Count of On3 three-star commits. |
| `consensus_three_stars` | integer | Count of industry-consensus three-star commits. |
| `overall_rank` | integer | On3 national team-class rank. |
| `overall_consensus_rank` | integer | Industry-consensus national team-class rank. |
| `dispay_consensus_score` | double | Display string of the consensus class score (field name misspelling is On3's). |
| `dispay_on3_score` | double | Display string of the On3 class score (field name misspelling is On3's). |
| `average_nil_value` | double | Average On3 NIL valuation across the class, in US dollars. |
| `conference_rank` | integer | On3 class rank within the program's conference. |
| `conference_consensus_rank` | integer | Consensus class rank within the program's conference. |
| `organization_key` | integer | On3 numeric key of the program. |
| `organization_full_name` | character | Full name of the program. |
| `organization_name` | character | Short name of the program. |
| `organization_mascot` | character | Program mascot. |
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
| `organization_slug` | character | URL slug of the program on On3. |
| `organization_primary_color` | character | Primary hex color of the program. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_team_rankings(year=2026, sport_slug='football')
```

_Last validated n/a._

## `on3_industry_team_rankings`

On3 Industry Comparison team recruiting-class rankings (rankingType=industry-team).

**Endpoint URL:** `GET https://www.on3.com/rivals/rankings/industry-team/{sport_slug}/{year}.json`

**Valid URL:** [https://www.on3.com/rivals/rankings/industry-team/football/2026.json](https://www.on3.com/rivals/rankings/industry-team/football/2026.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | year path parameter. |
| `sport_slug` | `sport_slug` |  |  | `Y` | On3 sport slug (e.g. football, basketball). |
| `page` | `page` |  |  | `Y` | page query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
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
| `deductions` | integer | Points deducted by the formula (e.g. over-signing adjustments). |
| `deductions_description` | character | Explanation of the deduction, when present. |
| `five_stars` | integer | Count of On3 five-star commits. |
| `consensus_five_stars` | integer | Count of industry-consensus five-star commits. |
| `four_stars` | integer | Count of On3 four-star commits. |
| `consensus_four_stars` | integer | Count of industry-consensus four-star commits. |
| `three_stars` | integer | Count of On3 three-star commits. |
| `consensus_three_stars` | integer | Count of industry-consensus three-star commits. |
| `overall_rank` | integer | On3 national team-class rank. |
| `overall_consensus_rank` | integer | Industry-consensus national team-class rank. |
| `dispay_consensus_score` | double | Display string of the consensus class score (field name misspelling is On3's). |
| `dispay_on3_score` | double | Display string of the On3 class score (field name misspelling is On3's). |
| `average_nil_value` | double | Average On3 NIL valuation across the class, in US dollars. |
| `conference_rank` | integer | On3 class rank within the program's conference. |
| `conference_consensus_rank` | integer | Consensus class rank within the program's conference. |
| `organization_key` | integer | On3 numeric key of the program. |
| `organization_full_name` | character | Full name of the program. |
| `organization_name` | character | Short name of the program. |
| `organization_mascot` | character | Program mascot. |
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
| `organization_slug` | character | URL slug of the program on On3. |
| `organization_primary_color` | character | Primary hex color of the program. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_industry_team_rankings(year=2026, sport_slug='football')
```

_Last validated n/a._
