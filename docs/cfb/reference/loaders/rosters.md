# CFB dataset loaders — Rosters

> CFB dataset loaders — Rosters — function reference in sdv-py, the SportsDataverse Python package.

## load_cfb_rosters

Release: [espn_cfb_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_rosters) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_rosters/cfb_rosters_{season}.parquet`
### Returns {#load_cfb_rosters-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season the roster row describes. One row per athlete per team per season, so a transfer appears once under each team. |
| `team_id` | Int64 | ESPN team identifier, the join key to cfb_teams, cfb_schedules and the pbp datasets. |
| `athlete_id` | Int64 | ESPN athlete identifier, stable across seasons and teams -- the key for following a player through a career or a transfer. |
| `division` | String | NCAA classification of the athlete's team that season (fbs, fcs, d2, d3, d2_d3, naia), resolved from ESPN's group tree by the same map the teams dataset uses. |
| `position_id` | Int64 | ESPN's numeric position identifier, resolved against the league's 74-entry position reference rather than left as a bare URL. |
| `position` | String | Human-readable position, e.g. Quarterback or Nose Tackle. Resolved from the position reference, which is why this is populated rather than href-only as on the older published assets. |
| `position_abbreviation` | String | Short position code, e.g. QB or NT -- the form most box-score and depth-chart joins expect. |
| `position_name` | String | ESPN's canonical position name from the reference table; usually matches position, and kept so a caller can see the raw reference value. |
| `position_leaf` | Boolean | Whether the position sits at the bottom of ESPN's position tree. False means it is a grouping such as Offensive Line rather than a specific role. |
| `position_parent_id` | Int64 | Identifier of the broader position group above this one, letting you roll specific roles up to offense/defense buckets. |
| `games_rostered` | Int64 | Number of that season's games whose roster block listed the athlete. The dataset is compiled per game, so this doubles as an availability signal -- a low count means a late arrival, injury or departure. |
| `athlete_uid` | String | ESPN's fully-qualified resource key for the athlete. Traceability back to the source payload; athlete_id is the practical key. |
| `athlete_guid` | String | Opaque ESPN global identifier for the athlete record. |
| `athlete_type` | String | ESPN's athlete category, effectively always the football player type in this dataset. |
| `first_name` | String | Athlete's given name as ESPN records it. |
| `middle_name` | String | Athlete's middle name where ESPN carries one; null for most rows. |
| `last_name` | String | Athlete's family name as ESPN records it. Beware suffixes -- ESPN sometimes folds Jr./III into this field. |
| `full_name` | String | ESPN's assembled full name for the athlete. |
| `display_name` | String | Name ESPN shows in most surfaces, generally first plus last. |
| `athlete_display_name` | String | ESPN's display name taken from the athlete record itself, which can differ in punctuation or suffix handling from display_name. |
| `short_name` | String | Abbreviated name ESPN uses where space is tight, typically first initial plus surname. |
| `nickname` | String | Alternate short label ESPN carries for the athlete; sparsely populated. |
| `slug` | String | URL fragment ESPN uses for the athlete on espn.com. |
| `jersey` | String | Jersey number as text, because ESPN publishes it that way and numbers can carry a leading zero. Cast before doing arithmetic. |
| `jersey_right` | String | Right-aligned jersey rendering ESPN publishes for tabular display; cosmetic rather than analytic. |
| `weight` | Float64 | Listed weight in pounds. Null where ESPN has no figure, which is common outside Division I. |
| `display_weight` | String | Weight as ESPN renders it for display, e.g. 215 lbs. |
| `height` | Float64 | Listed height in inches. Null where ESPN has no figure. |
| `display_height` | String | Height as ESPN renders it for display, e.g. 6' 3\". |
| `age` | Float64 | Athlete's age as of the payload capture, not as of any particular game. Treat it as approximate. |
| `date_of_birth` | String | Birth date ESPN publishes, as text. Sparsely populated and worth validating before use. |
| `hand_type` | String | Handedness ESPN records, mostly meaningful for quarterbacks and kickers. |
| `hand_abbreviation` | String | Short form of the handedness value, e.g. R or L. |
| `hand_display_value` | String | Handedness as ESPN renders it for display. |
| `linked` | Boolean | ESPN's flag for whether the athlete record links to a fuller profile. |
| `active` | Boolean | ESPN's active flag on the athlete record. It reflects capture time, not the season, so do not use it as a season-eligibility filter. |
| `alternate_ids_sdr` | String | ESPN's secondary SDR identifier for the athlete, occasionally needed to reconcile against other ESPN feeds. |
| `birth_place_city` | String | City ESPN lists as the athlete's birthplace. |
| `birth_place_state` | String | State or province of the athlete's birthplace. |
| `birth_place_country` | String | Country of the athlete's birthplace. |
| `birth_country_alternate_id` | String | ESPN's alternate identifier for the birth country. |
| `birth_country_abbreviation` | String | Abbreviated birth-country code as ESPN publishes it. |
| `citizenship` | String | Citizenship ESPN records for the athlete; sparsely populated and mostly present for international players. |
| `flag_href` | String | URL of the flag image ESPN pairs with the athlete's citizenship. |
| `flag_alt` | String | Alt text ESPN publishes for that flag image. |
| `flag_rel` | String | Relationship hints ESPN attaches to the flag image, stringified from a list. |
| `headshot_href` | String | URL of the athlete's headshot on ESPN's CDN. Null for most non-FBS players. |
| `headshot_alt` | String | Alt text ESPN publishes for the headshot image. |
| `experience_years` | Float64 | Years of collegiate experience ESPN credits the athlete with, which is closer to class standing than to seasons played. |
| `experience_display_value` | String | Experience as ESPN renders it, e.g. Sophomore or Freshman. |
| `experience_abbreviation` | String | Short form of the experience value, e.g. SO or FR. |
| `status_id` | String | Identifier of the athlete's roster status in ESPN's status vocabulary. |
| `status_name` | String | Roster status as ESPN names it, e.g. Active. Reflects capture time rather than a point in the season. |
| `status_type` | String | ESPN's coarse grouping for the status value. |
| `status_abbreviation` | String | Short form of the roster status. |
| `draft_display_text` | String | ESPN's rendered draft summary for the athlete where one exists, e.g. round and pick. Populated only for players who reached the NFL draft. |
| `draft_round` | String | Round the athlete was drafted in, as text. Null for the overwhelming majority of rows. |
| `draft_year` | String | Year the athlete was drafted, as text. |
| `draft_selection` | String | Overall selection number in that draft, as text. |
| `draft_team_href` | String | URL of the drafting team's ESPN resource, from which the team can be resolved. |
| `team_guid` | String | Opaque ESPN global identifier of the athlete's team, denormalised onto the roster row. |
| `team_uid` | String | ESPN's fully-qualified resource key for the team. |
| `team_slug` | String | URL fragment ESPN uses for the team. |
| `team_location` | String | School or city ESPN attaches to the team, denormalised for convenience. |
| `team_name` | String | Team nickname as ESPN stores it, e.g. Crimson Tide. |
| `team_nickname` | String | ESPN's alternate short label for the team, often the school shorthand. |
| `team_abbreviation` | String | Short team code ESPN displays, e.g. ALA. Not unique across divisions, so join on team_id. |
| `team_display_name` | String | Full team name as ESPN renders it, location plus nickname. |
| `team_short_display_name` | String | Condensed team label ESPN uses where space is tight. |
| `team_color` | String | Primary team colour as a hex string without the leading hash, denormalised from the team record. |
| `team_alternate_color` | String | Secondary team colour as a hex string without the leading hash. |
| `team_alternate_ids_sdr` | String | ESPN's secondary SDR identifier for the team. |
| `is_active` | Boolean | Whether ESPN marks the team as active. Denormalised from the team record and reflects capture time. |
| `is_all_star` | Boolean | Whether the team is one of the all-star or exhibition squads ESPN files alongside real programs. Filter these out for a true roster count. |
| `logo_href` | String | URL of the team's primary logo on ESPN's CDN. |
| `logo_dark_href` | String | URL of the dark-background variant of the team logo. |
| `athlete_href` | String | ESPN API URL of the athlete resource, useful for fetching the fuller profile. |
| `position_href` | String | ESPN API URL of the position resource. Retained for provenance -- the resolved position, position_name and position_abbreviation columns are what you should read. |
| `cfbd_recruit_ids` | String | CollegeFootballData recruiting-profile ids matched to this athlete, as a JSON array string such as "[62208]". The one field here with no ESPN counterpart at all, and the link from a roster row into the recruiting datasets; a lone 0 entry means no recruiting profile was matched. |
| `cfbd_home_city` | String | Hometown city from CollegeFootballData. This is HOMETOWN, not birthplace -- ESPN's birth_place_city is a different fact and the two disagree often enough that they are shipped side by side rather than coalesced. |
| `cfbd_home_state` | String | Hometown state or province from CollegeFootballData, two-letter for United States addresses. Hometown, not birthplace -- see cfbd_home_city. |
| `cfbd_home_country` | String | Hometown country from CollegeFootballData, overwhelmingly USA. Hometown, not birthplace -- see cfbd_home_city. |
| `cfbd_home_latitude` | String | Latitude of the hometown as geocoded by CollegeFootballData, carried as a string exactly as published. Pair with cfbd_home_longitude to map recruiting footprints or compute distance from campus. |
| `cfbd_home_longitude` | String | Longitude of the hometown as geocoded by CollegeFootballData, carried as a string exactly as published. Negative across the United States; pair with cfbd_home_latitude. |
| `cfbd_home_county_fips` | String | Five-digit FIPS code of the hometown county from CollegeFootballData, the join key to United States census and county-level demographic tables. |

```python
load_cfb_rosters(seasons=2024)
```

## load_cfb_rosters_cfbd

Release: [cfbfastR-data](https://github.com/sportsdataverse/cfbfastR-data) · asset `https://raw.githubusercontent.com/sportsdataverse/cfbfastR-data/main/rosters/parquet/cfb_rosters_{season}.parquet`
### Returns {#load_cfb_rosters_cfbd-returns}

| col_name | type | description |
|---|---|---|
| `athlete_id` | String | ESPN athlete id. |
| `first_name` | String | Athlete first name. |
| `last_name` | String | Athlete last name. |
| `team` | String | Team name. |
| `weight` | Int32 | Listed weight (lbs). |
| `height` | Int32 | Listed height (inches). |
| `jersey` | Int32 | Jersey number. |
| `year` | Int32 | Four-digit season year (e.g. 2019). |
| `position` | String | Athlete position. |
| `home_city` | String | Hometown of the athlete. |
| `home_state` | String | Hometown state of the athlete. |
| `home_country` | String | Hometown country of the athlete. |
| `home_latitude` | Float64 | Hometown latitude. |
| `home_longitude` | Float64 | Hometown longitude. |
| `home_county_fips` | String | Hometown FIPS code. |
| `recruit_ids` | List(Int32) | List of recruiting-database profile ids matched to the player; real ids run in the six-figure range and a lone 0 entry means no recruiting profile was matched. |
| `headshot_url` | String | Player ESPN headshot url. |
| `season` | Int32 | Season (4-digit year). |

```python
load_cfb_rosters_cfbd(seasons=2024)
```
