---
title: CFB — ESPN site API (v2)
sidebar_label: ESPN site API (v2)
sidebar_position: 20
---
# CFB — ESPN site API (v2)

`sportsdataverse.cfb` — 25 endpoints.

## `espn_cfb_scoreboard`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard?dates=20240115](https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard?dates=20240115)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `dates` | `dates` |  |  | `Y` | Date or date range filter (YYYYMMDD or YYYYMMDD-YYYYMMDD). |
| `week` | `week` |  |  | `Y` | Week number within the season (football). |
| `seasontype` | `season_type` |  |  | `Y` | Season phase: 1=preseason, 2=regular season, 3=postseason. |
| `groups` | `groups` |  |  | `Y` | Conference or group id filter (e.g. an ESPN conference id). |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_id` | character | ESPN event id. |
| `uid` | character | ESPN global unique identifier. |
| `date` | character | Match start timestamp (ISO 8601, UTC). |
| `name` | character | Full event name (e.g. 'Team A at Team B'). |
| `short_name` | character | Abbreviated event name (e.g. 'TA @ TB'). |
| `season_year` | integer | Season year string ('YYYY-YY' format). |
| `season_type` | integer | ESPN season type (2 = regular, 3 = postseason). |
| `season_slug` | character | Season slug. |
| `status_type_id` | character | Unique identifier for status type. |
| `status_type_name` | character | Status type name. |
| `status_type_state` | character | Status state (pre/in/post). |
| `status_type_completed` | logical | Whether the game is complete. |
| `status_type_description` | character | Status type description. |
| `status_type_detail` | character | Status type detail. |
| `status_type_short_detail` | character | Status type short detail. |
| `status_clock` | double | Game clock in seconds. |
| `status_display_clock` | character | Status display clock. |
| `status_period` | integer | Current period. |
| `neutral_site` | logical | Whether the match is played at a neutral venue. |
| `conference_competition` | logical | Conference competition. |
| `attendance` | integer | Reported attendance at the game. |
| `venue_id` | character | Referencing venue id. |
| `venue_full_name` | character | Venue full name. |
| `venue_city` | character | City where the venue is located. |
| `venue_state` | character | State (or province/country) where the venue is located. |
| `venue_indoor` | logical | Whether the home venue is indoors. |
| `broadcast` | character | Broadcast network short name. |
| `note` | character | Game note or headline. |
| `home_id` | character | Home team referencing id. |
| `home_name` | character | Home team display name. |
| `home_abbreviation` | character | Home team's abbreviation. |
| `home_display_name` | character | Home team display name. |
| `home_location` | character | Home team's location. |
| `home_color` | character | Home team primary color hex. |
| `home_alternate_color` | character | Color code (hex) for home alternate. |
| `home_logo` | character | Home team logo URL. |
| `home_score` | character | Home team's score. For cricket, the innings string (e.g. '161/5 (18/20 ov, target 156)'). |
| `home_winner` | logical | Whether the home team won. |
| `home_rank` | integer | Home team rank (if ranked). |
| `away_id` | character | Away team referencing id. |
| `away_name` | character | Away team display name. |
| `away_abbreviation` | character | Away team's abbreviation. |
| `away_display_name` | character | Away team display name. |
| `away_location` | character | Away team's location. |
| `away_color` | character | Away team primary color hex. |
| `away_alternate_color` | character | Color code (hex) for away alternate. |
| `away_logo` | character | Away team logo URL. |
| `away_score` | character | Away team's score. For cricket, the innings string. |
| `away_winner` | logical | Whether the away team won. |
| `away_rank` | integer | Away team rank (if ranked). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_cfb_scoreboard(dates='20240115')
```

_Last validated n/a._

## `espn_cfb_summary`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/summary`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/summary](https://site.api.espn.com/apis/site/v2/sports/football/college-football/summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event` | `event_id` |  |  | `Y` | event query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
**boxscore_player**

| col_name | type | description |
|---|---|---|
| `team_id` | character | Team id. |
| `team_abbreviation` | character | Team abbreviation. |
| `team_display_name` | character | Team display name. |
| `team_location` | character | Team location. |
| `athlete_id` | character | Athlete id. |
| `athlete_display_name` | character | Athlete display name. |
| `athlete_short_name` | character | Athlete short name. |
| `athlete_jersey` | character | Athlete jersey. |
| `athlete_position` | character | Athlete position. |
| `starter` | character | Starter. |
| `active` | character | Active. |
| `did_not_play` | character | Did not play. |
| `ejected` | character | Ejected. |
| `reason` | character | Reason. |
| `completions/passing_attempts` | character |  |
| `passing_yards` | character | Numeric yards by the passer_player_name, including yards gained in pass plays with laterals. This should equal official passing statistics. |
| `yards_per_pass_attempt` | character |  |
| `passing_touchdowns` | character |  |
| `interceptions` | character | Passing interceptions. |
| `adj_qbr` | character |  |
| `rushing_attempts` | character | Team rushing attempts. |
| `rushing_yards` | character | Team rushing yards. |
| `yards_per_rush_attempt` | character | Team yards per rush attempt. |
| `rushing_touchdowns` | character |  |
| `long_rushing` | character |  |
| `receptions` | character | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `receiving_yards` | character | Numeric yards by the receiver_player_name, excluding yards gained in pass plays with laterals. This should equal official receiving statistics but could miss yards gained in pass plays with laterals. Please see the description of `lateral_receiver_player_name` for further information. |
| `yards_per_reception` | character |  |
| `receiving_touchdowns` | character |  |
| `long_reception` | character |  |
| `fumbles` | character |  |
| `fumbles_lost` | character | Fumbles lost. |
| `fumbles_recovered` | character | Team fumbles recovered. |
| `total_tackles` | character |  |
| `solo_tackles` | character |  |
| `sacks` | character | Team sacks. |
| `tackles_for_loss` | character | Team tackles for a loss. |
| `passes_defended` | character |  |
| `hurries` | character |  |
| `defensive_touchdowns` | character |  |
| `kick_returns` | character | Number of kick returns. |
| `kick_return_yards` | character | Team kick return yards. |
| `yards_per_kick_return` | character |  |
| `long_kick_return` | character |  |
| `kick_return_touchdowns` | character |  |
| `punt_returns` | character | Number of punt returns. |
| `punt_return_yards` | character | Team punt return yards. |
| `yards_per_punt_return` | character |  |
| `long_punt_return` | character |  |
| `punt_return_touchdowns` | character |  |
| `field_goals_made/field_goal_attempts` | character |  |
| `field_goal_pct` | character | Field goal percentage (0-1). |
| `long_field_goal_made` | character |  |
| `extra_points_made/extra_point_attempts` | character |  |
| `total_kicking_points` | character |  |
| `punts` | character |  |
| `punt_yards` | character |  |
| `gross_avg_punt_yards` | character |  |
| `touchbacks` | character |  |
| `punts_inside20` | character |  |
| `long_punt` | character |  |

**boxscore_team**

| col_name | type | description |
|---|---|---|
| `team_id` | character | Team id. |
| `team_abbreviation` | character | Team abbreviation. |
| `team_display_name` | character | Team display name. |
| `home_away` | character | Home away. |
| `display_order` | integer | Display order. |
| `stat_name` | character | Stat name. |
| `stat_label` | character | Stat label. |
| `stat_display_value` | character | Stat display value. |
| `stat_value` | character | Stat value. |

**winprobability**

| col_name | type | description |
|---|---|---|
| `home_win_percentage` | double | Home win percentage. |
| `tie_percentage` | double | Tie percentage. |
| `play_id` | character | Play id. |

**leaders**

| col_name | type | description |
|---|---|---|
| `team_id` | character | Team id. |
| `team_abbreviation` | character | Team abbreviation. |
| `category_name` | character | Category name. |
| `category_display_name` | character | Category display name. |
| `athlete_id` | character | Athlete id. |
| `athlete_display_name` | character | Athlete display name. |
| `athlete_position` | character | Athlete position. |
| `value` | double | Value. |
| `display_value` | character | Display value. |
| `main_stat` | character | Main stat. |
| `summary` | character | Summary. |

**game_info**

| col_name | type | description |
|---|---|---|
| `attendance` | integer | Attendance. |
| `venue_id` | character | Venue id. |
| `venue_guid` | character | Venue guid. |
| `venue_full_name` | character | Venue full name. |
| `venue_address_city` | character | Venue address city. |
| `venue_address_state` | character | Venue address state. |
| `venue_address_zip_code` | character |  |
| `venue_address_country` | character |  |
| `venue_grass` | logical | Venue grass. |

**header**

| col_name | type | description |
|---|---|---|
| `id` | character | Id. |
| `uid` | character | Uid. |
| `time_valid` | logical | Time valid. |
| `competitions` | character | Competitions. |
| `links` | character | Links. |
| `week` | integer | Game week of the season. |
| `game_note` | character |  |
| `season_year` | integer | Season year. |
| `season_current` | logical | Season current. |
| `season_type` | integer | Season type. |
| `league_id` | character | League id. |
| `league_uid` | character | League uid. |
| `league_name` | character | League name. |
| `league_abbreviation` | character | League abbreviation. |
| `league_midsize_name` | character |  |
| `league_slug` | character | League slug. |
| `league_is_tournament` | logical | League is tournament. |
| `league_links` | character | League links. |
| `league_logos` | character | League logos. |

**standings**

| col_name | type | description |
|---|---|---|
| `group_header` | character | Group header. |
| `conference_header` | character | Conference header. |
| `division_header` | character | Division header. |
| `team_id` | character | Team id. |
| `team_uid` | character | Team uid. |
| `team_location` | character | Team location. |
| `overall` | character | Overall draft pick number. |
| `vs. conf.` | character |  |

**broadcasts**

| col_name | type | description |
|---|---|---|
| `station` | character | Broadcast station / network name (e.g. `ESPN+`). |
| `station_key` | character |  |
| `lang` | character | Broadcast language code. |
| `region` | character | Broadcast region code. |
| `is_national` | logical |  |
| `type_id` | character | Type id. |
| `type_short_name` | character | Broadcast type short name (e.g. "TV"). |
| `type_long_name` | character | Broadcast type long name (e.g. "Television"). |
| `type_slug` | character | Broadcast-type slug (e.g. `streaming`, `tv`). |
| `market_id` | character | ESPN futures-market identifier. |
| `market_type` | character | Geographic market type (e.g. `National`). |
| `media_call_letters` | character | Broadcast call letters for the outlet. |
| `media_name` | character | ESPN media name for the outlet. |
| `media_short_name` | character | Short ESPN media name for the outlet. |

**format**

| col_name | type | description |
|---|---|---|
| `regulation_periods` | integer | Regulation periods. |
| `regulation_display_name` | character | Regulation display name. |
| `regulation_slug` | character | Regulation slug. |
| `regulation_clock` | double | Regulation clock. |
| `overtime_display_name` | character | Overtime display name. |
| `overtime_slug` | character | Overtime slug. |
| `sudden_death_periods` | integer |  |

**article**

| col_name | type | description |
|---|---|---|
| `id` | integer | Id. |
| `now_id` | character | Now id. |
| `content_key` | character | Content key. |
| `data_source_identifier` | character | Data source identifier. |
| `publishedkey` | character | Publishedkey. |
| `type` | character | Type. |
| `game_id` | character | Game id. |
| `headline` | character | Headline. |
| `description` | character | Description. |
| `link_text` | character | Link text. |
| `categorized` | character | Categorized. |
| `originally_posted` | character | Originally posted. |
| `last_modified` | character | Last modified. |
| `published` | character | Published. |
| `section` | character | Section. |
| `source` | character | Source. |
| `images` | character | Images. |
| `video` | character | Video. |
| `categories` | character | Categories. |
| `keywords` | character | Keywords. |
| `story` | character | Story. |
| `premium` | logical | Premium. |
| `is_live_blog` | logical | Is live blog. |
| `allow_comments` | logical | Allow comments. |
| `allow_search` | logical | Allow search. |
| `allow_content_reactions` | logical | Allow content reactions. |
| `links_web_href` | character | Links web href. |
| `links_mobile_href` | character | Links mobile href. |
| `links_api_self_href` | character | Links api self href. |
| `links_app_sportscenter_href` | character | Links app sportscenter href. |

**news**

| col_name | type | description |
|---|---|---|
| `id` | integer | Id. |
| `now_id` | character | Now id. |
| `content_key` | character | Content key. |
| `data_source_identifier` | character | Data source identifier. |
| `type` | character | Type. |
| `headline` | character | Headline. |
| `description` | character | Description. |
| `last_modified` | character | Last modified. |
| `published` | character | Published. |
| `images` | character | Images. |
| `categories` | character | Categories. |
| `premium` | logical | Premium. |
| `links_web_href` | character | Links web href. |
| `links_web_self_href` | character |  |
| `links_web_self_dsi_href` | character |  |
| `links_api_self_href` | character | Links api self href. |
| `links_api_artwork_href` | character |  |
| `links_sportscenter_href` | character |  |
| `byline` | character | Byline. |
| `links_mobile_href` | character | Links mobile href. |
| `links_app_sportscenter_href` | character | Links app sportscenter href. |

**drives**

| col_name | type | description |
|---|---|---|
| `id` | character | Id. |
| `description` | character | Description. |
| `yards` | integer | Total yards gained on the drive. |
| `is_score` | logical | `TRUE` if the drive resulted in a score. |
| `offensive_plays` | integer | Number of offensive plays on the drive. |
| `result` | character | Drive result code (e.g. `PUNT`, `TD`). |
| `short_display_result` | character | Short drive-result label. |
| `display_result` | character | Drive-result label (e.g. `Punt`, `Touchdown`). |
| `plays` | character | Total qualifying passing plays included in the WEPA calculation. |
| `team_id` | character | Team id. |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `team_abbreviation` | character | Team abbreviation. |
| `team_display_name` | character | Team display name. |
| `team_short_display_name` | character | Short team display name; `team_detail = TRUE` only. |
| `team_logos` | character | Team logos. |
| `start_period_type` | character | Period type at the start of the drive (e.g. `quarter`). |
| `start_period_number` | integer |  |
| `start_clock_display_value` | character |  |
| `start_yard_line` | integer | Yard line at the start of the play. |
| `start_text` | character | Field-position text at the start of the drive. |
| `end_period_type` | character | Period type at the end of the drive (e.g. `quarter`). |
| `end_period_number` | integer |  |
| `end_clock_display_value` | character |  |
| `end_yard_line` | integer | Yard line at the end of the play. |
| `end_text` | character | Field-position text at the end of the drive. |
| `time_elapsed_display_value` | character |  |

**drive_plays**

| col_name | type | description |
|---|---|---|
| `drive_id` | character | CFBD drive identifier the play belongs to. |
| `drive_sequence` | integer |  |
| `id` | character | Id. |
| `sequence_number` | character | Sequence number. |
| `text` | character | Text. |
| `away_score` | integer | Away score. |
| `home_score` | integer | Home score. |
| `scoring_play` | logical | Scoring play. |
| `priority` | logical | `TRUE` if ESPN flags the play as a priority highlight. |
| `modified` | character | ISO timestamp the play record was last modified. |
| `wallclock` | character | Wallclock. |
| `team_participants` | character |  |
| `is_penalty` | logical | `TRUE` if the play was a penalty. |
| `stat_yardage` | integer | Yards gained or lost on the play. |
| `is_turnover` | logical | `TRUE` if the play was a turnover. |
| `type_id` | character | Type id. |
| `type_text` | character | Type text. |
| `type_abbreviation` | character | Play-type abbreviation (e.g. `RUSH`, `TD`). |
| `period_number` | integer | Period number. |
| `clock_display_value` | character | Clock display value. |
| `start_down` | integer | Down at the start of the play. |
| `start_distance` | integer | Yards to go at the start of the play. |
| `start_yard_line` | integer | Yard line at the start of the play. |
| `start_yards_to_endzone` | integer | Yards to the end zone at the start of the play. |
| `start_team_id` | character | ESPN team id in possession at the start of the play. |
| `end_down` | integer | Down at the end of the play. |
| `end_distance` | integer | Yards to go at the end of the play. |
| `end_yard_line` | integer | Yard line at the end of the play. |
| `end_yards_to_endzone` | integer | Yards to the end zone at the end of the play. |
| `end_down_distance_text` | character | Down-and-distance text at the end of the play. |
| `end_short_down_distance_text` | character | Short down-and-distance text at the end of the play. |
| `end_possession_text` | character | Field-position text at the end of the play. |
| `end_team_id` | character | ESPN team id in possession at the end of the play. |
| `start_down_distance_text` | character | Down-and-distance text at the start of the play. |
| `start_short_down_distance_text` | character | Short down-and-distance text at the start of the play. |
| `start_possession_text` | character | Field-position text at the start of the play. |
| `scoring_type_name` | character | Scoring-type key on a scoring play (e.g. `touchdown`). |
| `scoring_type_display_name` | character | Human-readable scoring-type name. |
| `scoring_type_abbreviation` | character | Scoring-type abbreviation (e.g. `TD`, `FG`). |
| `point_after_attempt_id` | double | Point-after-attempt id on a scoring play. |
| `point_after_attempt_text` | character | Point-after-attempt text (e.g. `Extra Point Good`). |
| `point_after_attempt_abbreviation` | character | Point-after-attempt abbreviation. |
| `point_after_attempt_value` | double | Points added by the point-after attempt. |
| `media_id` | character | ESPN media id for the outlet. |

**scoring_plays**

| col_name | type | description |
|---|---|---|
| `id` | character | Id. |
| `text` | character | Text. |
| `away_score` | integer | Away score. |
| `home_score` | integer | Home score. |
| `type_id` | character | Type id. |
| `type_text` | character | Type text. |
| `type_abbreviation` | character | Play-type abbreviation (e.g. `RUSH`, `TD`). |
| `period_number` | integer | Period number. |
| `clock_value` | double | Clock value in seconds. |
| `clock_display_value` | character | Clock display value. |
| `team_id` | character | Team id. |
| `team_uid` | character | Team uid. |
| `team_display_name` | character | Team display name. |
| `team_abbreviation` | character | Team abbreviation. |
| `team_links` | character | Team links. |
| `team_logo` | character | Team logo. |
| `team_logos` | character | Team logos. |
| `scoring_type_name` | character | Scoring-type key on a scoring play (e.g. `touchdown`). |
| `scoring_type_display_name` | character | Human-readable scoring-type name. |
| `scoring_type_abbreviation` | character | Scoring-type abbreviation (e.g. `TD`, `FG`). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_cfb_summary()
```

_Last validated n/a._

## `espn_cfb_calendar`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/calendar`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/calendar](https://site.api.espn.com/apis/site/v2/sports/football/college-football/calendar)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_cfb_calendar()
```

_Last validated n/a._

## `espn_cfb_news`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/news`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/news](https://site.api.espn.com/apis/site/v2/sports/football/college-football/news)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | ESPN numeric identifier for the article. |
| `now_id` | character | ESPN 'now' feed id. |
| `content_key` | character | Internal content key. |
| `data_source_identifier` | character | Source-system identifier. |
| `type` | character | Article type (Story, Media, HeadlineNews, etc.). |
| `headline` | character | Article headline. |
| `description` | character | Article summary/description. |
| `last_modified` | character | Last-modified timestamp (ISO 8601). |
| `published` | character | Publish timestamp (ISO 8601). |
| `images` | character | Article images (list, stringified). |
| `categories` | character | Article categories (list, stringified). |
| `premium` | logical | Whether the article is premium/paywalled. |
| `byline` | character | Author byline string as published by ESPN. |
| `links_web_href` | character | Web article URL. |
| `links_mobile_href` | character | Mobile article URL. |
| `links_api_self_href` | character | ESPN API canonical self-link for the article resource. |
| `links_app_sportscenter_href` | character | SportsCenter app deep link. |
| `links_web_self_href` | character |  |
| `links_web_self_dsi_href` | character |  |
| `links_api_artwork_href` | character |  |
| `links_sportscenter_href` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_cfb_news()
```

_Last validated n/a._

## `espn_cfb_injuries`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/injuries`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/injuries](https://site.api.espn.com/apis/site/v2/sports/football/college-football/injuries)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | character | ESPN numeric identifier for the athlete. |
| `display_name` | character | Athlete's full display name as shown on ESPN. |
| `injuries` | character | Injury entries for the athlete (list of dicts, stringified): status, type, details, dates. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_cfb_injuries()
```

_Last validated n/a._

## `espn_cfb_transactions`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/transactions`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/transactions](https://site.api.espn.com/apis/site/v2/sports/football/college-football/transactions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_cfb_transactions()
```

_Last validated n/a._

## `espn_cfb_conferences`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/groups`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/groups](https://site.api.espn.com/apis/site/v2/sports/football/college-football/groups)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_groups`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_cfb_conferences()
```

_Last validated n/a._

## `espn_cfb_statistics_league`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/statistics`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/statistics](https://site.api.espn.com/apis/site/v2/sports/football/college-football/statistics)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_cfb_statistics_league()
```

_Last validated n/a._

## `espn_cfb_draft`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/draft`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/draft](https://site.api.espn.com/apis/site/v2/sports/football/college-football/draft)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_cfb_draft()
```

_Last validated n/a._

## `espn_cfb_teams_site`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams](https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `team_abbreviation` | character | Short team abbreviation (e.g. "BOS"). |
| `team_alternate_color` | character | Secondary team color as a hex string (no leading '#'). |
| `team_color` | character | Primary team color as a hex string (no leading '#'). |
| `team_display_name` | character | Full team display name (location + nickname). |
| `team_id` | character | ESPN team id (stable join key across ESPN endpoints). |
| `team_is_active` | logical | Whether the team is currently active. |
| `team_is_all_star` | logical | Whether the entry is an all-star squad rather than a franchise. |
| `team_location` | character | Team location / city (e.g. "Boston"). |
| `team_logos` | character | Pipe-delimited logo image URLs. |
| `team_name` | character | Team nickname/mascot (e.g. "Celtics"). |
| `team_nickname` | character | Team nickname as ESPN labels it (often equals team_name). |
| `team_short_display_name` | character | Abbreviated display name for compact UIs. |
| `team_slug` | character | URL slug used in ESPN web paths. |
| `team_uid` | character | ESPN global UID (encodes sport/league/team). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_cfb_teams_site()
```

_Last validated n/a._

## `espn_cfb_team`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/{team_id}`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4](https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_cfb_team(team_id='4')
```

_Last validated n/a._

## `espn_cfb_team_roster`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/{team_id}/roster`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/roster](https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/roster)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `position_group` | character | Position group of the recruits (e.g. Offensive Line, Defensive Back). |
| `id` | character | Id. |
| `uid` | character | Uid. |
| `guid` | character | Guid. |
| `first_name` | character | First name. |
| `last_name` | character | Last name. |
| `full_name` | character | Full name. |
| `display_name` | character | Display name. |
| `short_name` | character | Short name. |
| `weight` | double | Weight. |
| `display_weight` | character | Display weight. |
| `height` | double | Height. |
| `display_height` | character | Display height. |
| `links` | character | Links. |
| `slug` | character | Slug. |
| `jersey` | character | Jersey. |
| `injuries` | character | Injuries. |
| `alternate_ids_sdr` | character | Alternate ids sdr. |
| `birth_place_city` | character | Birth place city. |
| `birth_place_state` | character | Birth place state. |
| `birth_place_country` | character | Birth place country. |
| `birth_place_display_text` | character | Birth place display text. |
| `birth_country_alternate_id` | character |  |
| `birth_country_abbreviation` | character | Birth country abbreviation. |
| `college_id` | character | College id. |
| `college_guid` | character | College guid. |
| `college_mascot` | character | College mascot. |
| `college_name` | character | College name. |
| `college_short_name` | character | College short name. |
| `college_abbrev` | character | College abbrev. |
| `college_logos` | character | College logos. |
| `headshot_href` | character | Headshot href. |
| `headshot_alt` | character | Headshot alt. |
| `flag_href` | character |  |
| `flag_alt` | character |  |
| `flag_rel` | character |  |
| `position_id` | character | Position id. |
| `position_name` | character | Position name. |
| `position_display_name` | character | Position display name. |
| `position_abbreviation` | character | Position abbreviation. |
| `position_leaf` | logical | Position leaf. |
| `position_parent_id` | character | ESPN id of the parent position; `position_detail = TRUE` only. |
| `position_parent_name` | character | Parent position name. |
| `position_parent_display_name` | character | Parent position display name. |
| `position_parent_abbreviation` | character | Parent position abbreviation. |
| `position_parent_leaf` | logical | Whether parent position is leaf. |
| `experience_years` | integer | Experience years. |
| `experience_display_value` | character | Experience display value. |
| `experience_abbreviation` | character | Experience abbreviation. |
| `status_id` | character | Status id. |
| `status_name` | character | Status name. |
| `status_type` | character | Status type. |
| `status_abbreviation` | character | Status abbreviation. |
| `hand_type` | character | Hand type. |
| `hand_abbreviation` | character | Hand abbreviation. |
| `hand_display_value` | character | Hand display value. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_cfb_team_roster(team_id='4')
```

_Last validated n/a._

## `espn_cfb_team_schedule`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/{team_id}/schedule`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/schedule](https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/schedule)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_team_schedule`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_cfb_team_schedule(team_id='4')
```

_Last validated n/a._

## `espn_cfb_team_record`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/{team_id}/record`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/record](https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/record)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_cfb_team_record(team_id='4')
```

_Last validated n/a._

## `espn_cfb_team_depthcharts`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/{team_id}/depthcharts`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/depthcharts](https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/depthcharts)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_cfb_team_depthcharts(team_id='4')
```

_Last validated n/a._

## `espn_cfb_team_injuries`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/{team_id}/injuries`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/injuries](https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/injuries)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | character | ESPN numeric identifier for the athlete. |
| `display_name` | character | Athlete's full display name as shown on ESPN. |
| `injuries` | character | Injury entries for the athlete (list of dicts, stringified): status, type, details, dates. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_cfb_team_injuries(team_id='4')
```

_Last validated n/a._

## `espn_cfb_team_transactions`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/{team_id}/transactions`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/transactions](https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/transactions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_cfb_team_transactions(team_id='4')
```

_Last validated n/a._

## `espn_cfb_team_history`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/{team_id}/history`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/history](https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/history)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_cfb_team_history(team_id='4')
```

_Last validated n/a._

## `espn_cfb_team_news`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/{team_id}/news`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/news](https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/news)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | ESPN numeric identifier for the article. |
| `now_id` | character | ESPN 'now' feed id. |
| `content_key` | character | Internal content key. |
| `data_source_identifier` | character | Source-system identifier. |
| `type` | character | Article type (Story, Media, HeadlineNews, etc.). |
| `headline` | character | Article headline. |
| `description` | character | Article summary/description. |
| `last_modified` | character | Last-modified timestamp (ISO 8601). |
| `published` | character | Publish timestamp (ISO 8601). |
| `images` | character | Article images (list, stringified). |
| `categories` | character | Article categories (list, stringified). |
| `premium` | logical | Whether the article is premium/paywalled. |
| `byline` | character | Author byline string as published by ESPN. |
| `links_web_href` | character | Web article URL. |
| `links_mobile_href` | character | Mobile article URL. |
| `links_api_self_href` | character | ESPN API canonical self-link for the article resource. |
| `links_app_sportscenter_href` | character | SportsCenter app deep link. |
| `links_web_self_href` | character |  |
| `links_web_self_dsi_href` | character |  |
| `links_api_artwork_href` | character |  |
| `links_sportscenter_href` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_cfb_team_news(team_id='4')
```

_Last validated n/a._

## `espn_cfb_team_leaders`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/{team_id}/leaders`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/leaders](https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_cfb_team_leaders(team_id='4')
```

_Last validated n/a._

## `espn_cfb_player_info`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/athletes/{athlete_id}`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/athletes/4239](https://site.api.espn.com/apis/site/v2/sports/football/college-football/athletes/4239)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_cfb_player_info(athlete_id='4239')
```

_Last validated n/a._

## `espn_cfb_player_bio`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/athletes/{athlete_id}/bio`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/athletes/4239/bio](https://site.api.espn.com/apis/site/v2/sports/football/college-football/athletes/4239/bio)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_cfb_player_bio(athlete_id='4239')
```

_Last validated n/a._

## `espn_cfb_player_news`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/athletes/{athlete_id}/news`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/athletes/4239/news](https://site.api.espn.com/apis/site/v2/sports/football/college-football/athletes/4239/news)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | ESPN numeric identifier for the article. |
| `now_id` | character | ESPN 'now' feed id. |
| `content_key` | character | Internal content key. |
| `data_source_identifier` | character | Source-system identifier. |
| `type` | character | Article type (Story, Media, HeadlineNews, etc.). |
| `headline` | character | Article headline. |
| `description` | character | Article summary/description. |
| `last_modified` | character | Last-modified timestamp (ISO 8601). |
| `published` | character | Publish timestamp (ISO 8601). |
| `images` | character | Article images (list, stringified). |
| `categories` | character | Article categories (list, stringified). |
| `premium` | logical | Whether the article is premium/paywalled. |
| `byline` | character | Author byline string as published by ESPN. |
| `links_web_href` | character | Web article URL. |
| `links_mobile_href` | character | Mobile article URL. |
| `links_api_self_href` | character | ESPN API canonical self-link for the article resource. |
| `links_app_sportscenter_href` | character | SportsCenter app deep link. |
| `links_web_self_href` | character |  |
| `links_web_self_dsi_href` | character |  |
| `links_api_artwork_href` | character |  |
| `links_sportscenter_href` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_cfb_player_news(athlete_id='4239')
```

_Last validated n/a._

## `espn_cfb_standings`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/v2/sports/football/college-football/standings`

**Valid URL:** [https://site.api.espn.com/apis/v2/sports/football/college-football/standings](https://site.api.espn.com/apis/v2/sports/football/college-football/standings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `group` | `group` |  |  | `Y` | Conference or group id filter (e.g. an ESPN conference id). |
| `type` | `standings_type` |  |  | `Y` | Standings variant (e.g. 'by-division' or 'by-conference'). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `group_name` | character | Group name. |
| `group_abbreviation` | character | Group abbreviation. |
| `team_id` | character | Team id. |
| `team_name` | character | Team name. |
| `team_abbreviation` | character | Team abbreviation. |
| `team_display_name` | character | Team display name. |
| `team_location` | character | Team location. |
| `team_logo` | character | Team logo. |
| `games_behind` | double | Games behind. |
| `league_win_percent` | double | League win percent. |
| `playoff_seed` | double | Playoff seed. |
| `point_differential` | double | Point differential. |
| `points_against` | double | Points against. |
| `points_for` | double | Points for. |
| `streak` | double | Streak. |
| `wins` | double | Wins. |
| `division_losses` | double |  |
| `division_ties` | double |  |
| `division_wins` | double |  |
| `overall` | character | Overall. |
| `home` | character | Home. |
| `away` | character | Away team name. |
| `vs. conf.` | character | Vs. conf.. |
| `vs ap top 25` | character |  |
| `vs usa ranked teams` | character |  |
| `vs division` | double |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_cfb_standings()
```

_Last validated n/a._

## `espn_cfb_rankings`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/rankings`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/rankings](https://site.api.espn.com/apis/site/v2/sports/football/college-football/rankings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_cfb_rankings()
```

_Last validated n/a._
