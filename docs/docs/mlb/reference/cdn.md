---
title: MLB — ESPN CDN API (cdn.espn.com)
sidebar_label: ESPN CDN API (cdn.espn.com)
description: "MLB — ESPN CDN API (cdn.espn.com) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 24
toc_max_heading_level: 2
---
# MLB — ESPN CDN API (cdn.espn.com)

`sportsdataverse.mlb` — 4 endpoints.

## espn_mlb_cdn_playbyplay

One game's espn.com play-by-play page data. The gamepackageJSON block is a Site v2 summary (header, boxscore, plays or drives, win probability, ...), so the parsed result is the same dict of frames that parse_summary returns.

**Endpoint URL:** `GET https://cdn.espn.com/core/mlb/playbyplay`

**Valid URL:** [https://cdn.espn.com/core/mlb/playbyplay?xhr=1&gameId=401696358](https://cdn.espn.com/core/mlb/playbyplay?xhr=1&gameId=401696358)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameId` | `game_id` |  | `Y` |  | ESPN game (event) id. |

### Returns {#espn_mlb_cdn_playbyplay-returns}

**`return_parsed=True`** (default) — a dict of `polars.DataFrame`s keyed by summary section (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

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
| `starter` | logical | Starter. |
| `active` | logical | Active. |
| `did_not_play` | character | Did not play. |
| `ejected` | character | Ejected. |
| `reason` | character | Reason. |
| `hits_at_bats` | character | Hitting performance ratio expressed as hits divided by at-bats for the player in the box score. |
| `at_bats` | character | At bats. |
| `runs` | character | Runs scored. |
| `hits` | character | Hits. |
| `rb_is` | character | Runs batted in and other secondary hitting statistics for the player in the box score. |
| `home_runs` | character | Home runs. |
| `walks` | character | Number of base-on-balls (walks) recorded by the player in the box score. |
| `strikeouts` | character | Number of strikeouts recorded by the player, either as a batter or pitcher, in the box score. |
| `pitches` | character | Total number of pitches thrown or faced by the player in the box score. |
| `avg` | character | Batting average. |
| `on_base_pct` | character | Percentage of plate appearances in which the player reached base safely in the game. |
| `slug_avg` | character | Slugging average reflecting the total bases per at-bat recorded by the player in the box score. |
| `full_innings.part_innings` | character | Innings pitched expressed as full innings and fractional partial innings for the pitcher in the box score. |
| `earned_runs` | character | Earned runs allowed. |
| `pitches_strikes` | character | Combined count of pitches thrown and strikes recorded for the pitcher in the box score. |
| `era` | character | Earned run average. |

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

**plays**

| col_name | type | description |
|---|---|---|
| `id` | character | Id. |
| `sequence_number` | character | Sequence number. |
| `text` | character | Text. |
| `away_score` | integer | Away score. |
| `home_score` | integer | Home score. |
| `scoring_play` | logical | Scoring play. |
| `score_value` | integer | Score value. |
| `wallclock` | character | Wallclock. |
| `at_bat_id` | character | Identifier of the at-bat the play belongs to. |
| `summary_type` | character | Play summary type. |
| `outs` | integer | Outs in the inning after the play. |
| `type_id` | character | Type id. |
| `type_text` | character | Type text. |
| `type_type` | character | Play type category. |
| `period_type` | character | Period type ('inning'). |
| `period_number` | integer | Period number. |
| `period_display_value` | character | Period display value. |
| `team_id` | character | Team id. |
| `pitch_count_balls` | integer | Balls in the count when the pitch was thrown. |
| `pitch_count_strikes` | integer | Strikes in the count when the pitch was thrown. |
| `result_count_balls` | integer | Balls in the count after the pitch. |
| `result_count_strikes` | integer | Strikes in the count after the pitch. |
| `participants` | character | Participants. |
| `bat_order` | double | Spot in the batting order (1-9; NA if not applicable). |
| `type_alternative_text` | character | Alternative play type text. |
| `bats_type` | character | Bats type. |
| `bats_abbreviation` | character | Bats abbreviation. |
| `bats_display_value` | character | Bats display value. |
| `at_bat_pitch_number` | double | Pitch number within the at-bat. |
| `pitch_velocity` | double | Pitch velocity (mph). |
| `trajectory` | character | Batted-ball trajectory. |
| `type_abbreviation` | character | Play type abbreviation. |
| `pitch_coordinate_x` | double | Pitch location x-coordinate. |
| `pitch_coordinate_y` | double | Pitch location y-coordinate. |
| `pitch_type_id` | character | Pitch type identifier. |
| `pitch_type_text` | character | Pitch type description (e.g. 'Four-seam FB'). |
| `pitch_type_abbreviation` | character | Pitch type abbreviation. |
| `hit_coordinate_x` | double | Batted-ball location x-coordinate. |
| `hit_coordinate_y` | double | Batted-ball location y-coordinate. |
| `alternative_play` | character | Alternative play flag. |
| `alternative_type_id` | character | Alternative play type id. |
| `alternative_type_text` | character | Alternative play type text. |
| `alternative_type_abbreviation` | character | Alternative play type abbreviation. |
| `alternative_type_alternative_text` | character | Alternative type alternative text. |
| `alternative_type_type` | character | Alternative play type category. |
| `on_first_athlete_id` | character | Athlete id of the runner on first base. |
| `on_second_athlete_id` | character | Athlete id of the runner on second base. |
| `on_third_athlete_id` | character | Athlete id of the runner on third base. |

**winprobability**

| col_name | type | description |
|---|---|---|
| `home_win_percentage` | double | Home win percentage. |
| `tie_percentage` | double | Tie percentage. |
| `play_id` | character | Play id. |

**game_info**

| col_name | type | description |
|---|---|---|
| `attendance` | integer | Attendance. |
| `venue_id` | character | Venue id. |
| `venue_full_name` | character | Venue full name. |
| `venue_short_name` | character | Venue short name. |
| `venue_address_city` | character | Venue address city. |
| `venue_address_state` | character | Venue address state. |
| `venue_address_zip_code` | character | Postal zip code of the venue where the game was played. |

**officials**

| col_name | type | description |
|---|---|---|
| `display_name` | character | Display name. |
| `order` | integer | Order. |
| `position_name` | character | Position name. |
| `position_display_name` | character | Position display name. |
| `position_id` | character | Position id. |

**header**

| col_name | type | description |
|---|---|---|
| `id` | character | Id. |
| `uid` | character | Uid. |
| `time_valid` | logical | Time valid. |
| `competitions` | character | Competitions. |
| `links` | character | Links. |
| `game_note` | character | Optional editorial note or context annotation attached to the game in the header. |
| `season_year` | integer | Season year. |
| `season_current` | logical | Season current. |
| `season_type` | integer | Season type. |
| `league_id` | character | League id. |
| `league_uid` | character | League uid. |
| `league_name` | character | League name. |
| `league_abbreviation` | character | League abbreviation. |
| `league_midsize_name` | character | Medium-length display name for the league or competition as shown in the game header. |
| `league_slug` | character | League slug. |
| `league_is_tournament` | logical | League is tournament. |
| `league_links` | character | League links. |
| `league_logos` | character | League logos. |

**season_series**

| col_name | type | description |
|---|---|---|
| `type` | character | Type. |
| `title` | character | Title. |
| `description` | character | Description. |
| `summary` | character | Summary. |
| `completed` | logical | Completed. |
| `total_competitions` | integer | Total competitions. |
| `series_score` | character | Series score. |
| `events` | character | Events. |
| `series_label` | character | Series label. |
| `short_summary` | character | Short summary. |
| `round` | character | Draft round number. |

**standings**

| col_name | type | description |
|---|---|---|
| `group_header` | character | Group header. |
| `conference_header` | character | Conference header. |
| `division_header` | character | Division header. |
| `team_id` | character | Team id. |
| `team_uid` | character | Team uid. |
| `team_location` | character | Team location. |
| `games_behind` | character | Games behind. |
| `losses` | character | Losses. |
| `streak` | character | Streak. |
| `win_percent` | character | Win percent. |
| `wins` | character | Wins. |

**broadcasts**

| col_name | type | description |
|---|---|---|
| `station` | character | Station full name (e.g. "FanDuel Sports Network Detroit"). |
| `station_key` | character | Machine-readable key identifying the broadcasting station airing the game. |
| `lang` | character | Broadcast language (e.g. "en"). |
| `region` | character | Region label. |
| `is_national` | logical | Boolean flag indicating whether the broadcast is a nationally distributed feed. |
| `type_id` | character | Type id. |
| `type_short_name` | character | Broadcast type short name (e.g. "TV"). |
| `type_long_name` | character | Broadcast type long name (e.g. "Television"). |
| `type_slug` | character | Broadcast-type slug (e.g. `streaming`, `tv`). |
| `market_id` | character | ESPN futures-market identifier. |
| `market_type` | character | Market type code (`winLeague`, `winConference`, `winDivision`, ...). |
| `media_call_letters` | character | Broadcast call letters for the outlet. |
| `media_name` | character | ESPN media name for the outlet. |
| `media_short_name` | character | Short ESPN media name for the outlet. |

**format**

| col_name | type | description |
|---|---|---|
| `regulation_periods` | integer | Regulation periods. |
| `regulation_display_name` | character | Regulation display name. |
| `regulation_slug` | character | Regulation slug. |

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

**injuries**

| col_name | type | description |
|---|---|---|
| `injuries` | character | Injuries. |
| `team_id` | character | Team id. |
| `team_uid` | character | Team uid. |
| `team_display_name` | character | Team display name. |
| `team_abbreviation` | character | Team abbreviation. |
| `team_links` | character | Team links. |
| `team_logo` | character | Team logo. |
| `team_logos` | character | Team logos. |

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
| `links_mobile_href` | character | Links mobile href. |
| `links_api_self_href` | character | Links api self href. |
| `links_app_sportscenter_href` | character | Links app sportscenter href. |
| `links_web_self_href` | character | URL for the canonical web page of the associated article or editorial content. |
| `links_web_self_dsi_href` | character | Data-source-identified URL for the web page of the associated article content. |
| `links_api_artwork_href` | character | API endpoint URL for artwork or imagery associated with the article. |
| `links_sportscenter_href` | character | URL for the article's page on ESPN's SportsCenter platform. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_cdn_playbyplay-example}

```python
espn_mlb_cdn_playbyplay(game_id='401696358')
```

_Last validated n/a._

## espn_mlb_cdn_boxscore

One game's espn.com box-score page data, parsed like a Site v2 summary. For football the drives and scoring plays are only on the playbyplay page.

**Endpoint URL:** `GET https://cdn.espn.com/core/mlb/boxscore`

**Valid URL:** [https://cdn.espn.com/core/mlb/boxscore?xhr=1&gameId=401696358](https://cdn.espn.com/core/mlb/boxscore?xhr=1&gameId=401696358)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameId` | `game_id` |  | `Y` |  | ESPN game (event) id. |

### Returns {#espn_mlb_cdn_boxscore-returns}

**`return_parsed=True`** (default) — a dict of `polars.DataFrame`s keyed by summary section (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

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
| `starter` | logical | Starter. |
| `active` | logical | Active. |
| `did_not_play` | character | Did not play. |
| `ejected` | character | Ejected. |
| `reason` | character | Reason. |
| `hits_at_bats` | character | Hitting performance ratio expressed as hits divided by at-bats for the player in the box score. |
| `at_bats` | character | At bats. |
| `runs` | character | Runs scored. |
| `hits` | character | Hits. |
| `rb_is` | character | Runs batted in and other secondary hitting statistics for the player in the box score. |
| `home_runs` | character | Home runs. |
| `walks` | character | Number of base-on-balls (walks) recorded by the player in the box score. |
| `strikeouts` | character | Number of strikeouts recorded by the player, either as a batter or pitcher, in the box score. |
| `pitches` | character | Total number of pitches thrown or faced by the player in the box score. |
| `avg` | character | Batting average. |
| `on_base_pct` | character | Percentage of plate appearances in which the player reached base safely in the game. |
| `slug_avg` | character | Slugging average reflecting the total bases per at-bat recorded by the player in the box score. |
| `full_innings.part_innings` | character | Innings pitched expressed as full innings and fractional partial innings for the pitcher in the box score. |
| `earned_runs` | character | Earned runs allowed. |
| `pitches_strikes` | character | Combined count of pitches thrown and strikes recorded for the pitcher in the box score. |
| `era` | character | Earned run average. |

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

**plays**

| col_name | type | description |
|---|---|---|
| `id` | character | Id. |
| `sequence_number` | character | Sequence number. |
| `text` | character | Text. |
| `away_score` | integer | Away score. |
| `home_score` | integer | Home score. |
| `scoring_play` | logical | Scoring play. |
| `score_value` | integer | Score value. |
| `wallclock` | character | Wallclock. |
| `at_bat_id` | character | Identifier of the at-bat the play belongs to. |
| `summary_type` | character | Play summary type. |
| `outs` | integer | Outs in the inning after the play. |
| `type_id` | character | Type id. |
| `type_text` | character | Type text. |
| `type_type` | character | Play type category. |
| `period_type` | character | Period type ('inning'). |
| `period_number` | integer | Period number. |
| `period_display_value` | character | Period display value. |
| `team_id` | character | Team id. |
| `pitch_count_balls` | integer | Balls in the count when the pitch was thrown. |
| `pitch_count_strikes` | integer | Strikes in the count when the pitch was thrown. |
| `result_count_balls` | integer | Balls in the count after the pitch. |
| `result_count_strikes` | integer | Strikes in the count after the pitch. |
| `participants` | character | Participants. |
| `bat_order` | double | Spot in the batting order (1-9; NA if not applicable). |
| `type_alternative_text` | character | Alternative play type text. |
| `bats_type` | character | Bats type. |
| `bats_abbreviation` | character | Bats abbreviation. |
| `bats_display_value` | character | Bats display value. |
| `at_bat_pitch_number` | double | Pitch number within the at-bat. |
| `pitch_velocity` | double | Pitch velocity (mph). |
| `trajectory` | character | Batted-ball trajectory. |
| `type_abbreviation` | character | Play type abbreviation. |
| `pitch_coordinate_x` | double | Pitch location x-coordinate. |
| `pitch_coordinate_y` | double | Pitch location y-coordinate. |
| `pitch_type_id` | character | Pitch type identifier. |
| `pitch_type_text` | character | Pitch type description (e.g. 'Four-seam FB'). |
| `pitch_type_abbreviation` | character | Pitch type abbreviation. |
| `hit_coordinate_x` | double | Batted-ball location x-coordinate. |
| `hit_coordinate_y` | double | Batted-ball location y-coordinate. |
| `alternative_play` | character | Alternative play flag. |
| `alternative_type_id` | character | Alternative play type id. |
| `alternative_type_text` | character | Alternative play type text. |
| `alternative_type_abbreviation` | character | Alternative play type abbreviation. |
| `alternative_type_alternative_text` | character | Alternative type alternative text. |
| `alternative_type_type` | character | Alternative play type category. |
| `on_first_athlete_id` | character | Athlete id of the runner on first base. |
| `on_second_athlete_id` | character | Athlete id of the runner on second base. |
| `on_third_athlete_id` | character | Athlete id of the runner on third base. |

**winprobability**

| col_name | type | description |
|---|---|---|
| `home_win_percentage` | double | Home win percentage. |
| `tie_percentage` | double | Tie percentage. |
| `play_id` | character | Play id. |

**game_info**

| col_name | type | description |
|---|---|---|
| `attendance` | integer | Attendance. |
| `venue_id` | character | Venue id. |
| `venue_full_name` | character | Venue full name. |
| `venue_short_name` | character | Venue short name. |
| `venue_address_city` | character | Venue address city. |
| `venue_address_state` | character | Venue address state. |
| `venue_address_zip_code` | character | Postal zip code of the venue where the game was played. |

**officials**

| col_name | type | description |
|---|---|---|
| `display_name` | character | Display name. |
| `order` | integer | Order. |
| `position_name` | character | Position name. |
| `position_display_name` | character | Position display name. |
| `position_id` | character | Position id. |

**header**

| col_name | type | description |
|---|---|---|
| `id` | character | Id. |
| `uid` | character | Uid. |
| `time_valid` | logical | Time valid. |
| `competitions` | character | Competitions. |
| `links` | character | Links. |
| `game_note` | character | Optional editorial note or context annotation attached to the game in the header. |
| `season_year` | integer | Season year. |
| `season_current` | logical | Season current. |
| `season_type` | integer | Season type. |
| `league_id` | character | League id. |
| `league_uid` | character | League uid. |
| `league_name` | character | League name. |
| `league_abbreviation` | character | League abbreviation. |
| `league_midsize_name` | character | Medium-length display name for the league or competition as shown in the game header. |
| `league_slug` | character | League slug. |
| `league_is_tournament` | logical | League is tournament. |
| `league_links` | character | League links. |
| `league_logos` | character | League logos. |

**season_series**

| col_name | type | description |
|---|---|---|
| `type` | character | Type. |
| `title` | character | Title. |
| `description` | character | Description. |
| `summary` | character | Summary. |
| `completed` | logical | Completed. |
| `total_competitions` | integer | Total competitions. |
| `series_score` | character | Series score. |
| `events` | character | Events. |
| `series_label` | character | Series label. |
| `short_summary` | character | Short summary. |
| `round` | character | Draft round number. |

**standings**

| col_name | type | description |
|---|---|---|
| `group_header` | character | Group header. |
| `conference_header` | character | Conference header. |
| `division_header` | character | Division header. |
| `team_id` | character | Team id. |
| `team_uid` | character | Team uid. |
| `team_location` | character | Team location. |
| `games_behind` | character | Games behind. |
| `losses` | character | Losses. |
| `streak` | character | Streak. |
| `win_percent` | character | Win percent. |
| `wins` | character | Wins. |

**broadcasts**

| col_name | type | description |
|---|---|---|
| `station` | character | Station full name (e.g. "FanDuel Sports Network Detroit"). |
| `station_key` | character | Machine-readable key identifying the broadcasting station airing the game. |
| `lang` | character | Broadcast language (e.g. "en"). |
| `region` | character | Region label. |
| `is_national` | logical | Boolean flag indicating whether the broadcast is a nationally distributed feed. |
| `type_id` | character | Type id. |
| `type_short_name` | character | Broadcast type short name (e.g. "TV"). |
| `type_long_name` | character | Broadcast type long name (e.g. "Television"). |
| `type_slug` | character | Broadcast-type slug (e.g. `streaming`, `tv`). |
| `market_id` | character | ESPN futures-market identifier. |
| `market_type` | character | Market type code (`winLeague`, `winConference`, `winDivision`, ...). |
| `media_call_letters` | character | Broadcast call letters for the outlet. |
| `media_name` | character | ESPN media name for the outlet. |
| `media_short_name` | character | Short ESPN media name for the outlet. |

**format**

| col_name | type | description |
|---|---|---|
| `regulation_periods` | integer | Regulation periods. |
| `regulation_display_name` | character | Regulation display name. |
| `regulation_slug` | character | Regulation slug. |

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

**injuries**

| col_name | type | description |
|---|---|---|
| `injuries` | character | Injuries. |
| `team_id` | character | Team id. |
| `team_uid` | character | Team uid. |
| `team_display_name` | character | Team display name. |
| `team_abbreviation` | character | Team abbreviation. |
| `team_links` | character | Team links. |
| `team_logo` | character | Team logo. |
| `team_logos` | character | Team logos. |

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
| `links_mobile_href` | character | Links mobile href. |
| `links_api_self_href` | character | Links api self href. |
| `links_app_sportscenter_href` | character | Links app sportscenter href. |
| `links_web_self_href` | character | URL for the canonical web page of the associated article or editorial content. |
| `links_web_self_dsi_href` | character | Data-source-identified URL for the web page of the associated article content. |
| `links_api_artwork_href` | character | API endpoint URL for artwork or imagery associated with the article. |
| `links_sportscenter_href` | character | URL for the article's page on ESPN's SportsCenter platform. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_cdn_boxscore-example}

```python
espn_mlb_cdn_boxscore(game_id='401696358')
```

_Last validated n/a._

## espn_mlb_cdn_schedule

espn.com schedule page data, one row per game: up to 7 days starting at `date` (mbb and wbb: that day only; cfb and nfl: one week).

**Endpoint URL:** `GET https://cdn.espn.com/core/mlb/schedule`

**Valid URL:** [https://cdn.espn.com/core/mlb/schedule?xhr=1&date=20250415](https://cdn.espn.com/core/mlb/schedule?xhr=1&date=20250415)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `date` | `date` |  |  | `Y` | Single date (YYYYMMDD). Ignored by cfb and nfl, which are week-oriented. Defaults to today. |
| `week` | `week` |  |  | `Y` | Week number (cfb and nfl). |
| `year` | `season` |  |  | `Y` | Season year that `week` belongs to (cfb and nfl). |
| `seasontype` | `season_type` |  |  | `Y` | Season phase for `week`: 1=preseason, 2=regular season, 3=postseason (cfb and nfl). |

### Returns {#espn_mlb_cdn_schedule-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | character | ESPN event id. |
| `uid` | character | ESPN UID string. |
| `date` | character | Match start timestamp (ISO 8601, UTC). |
| `name` | character | Full event name (e.g. 'Team A at Team B'). |
| `short_name` | character | Abbreviated event name (e.g. 'TA @ TB'). |
| `season_year` | integer | Integer season year ESPN assigns the event (e.g. 2025 for the 2025-26 season). |
| `season_type` | integer | ESPN season-type id of the event's season: 1 preseason, 2 regular season, 3 postseason, 4 offseason for the US leagues; soccer competitions carry their own competition-specific ids (e.g. 13481). |
| `season_slug` | character | Season slug. |
| `status_type_id` | character | Unique identifier for status type. |
| `status_type_name` | character | Status type name. |
| `status_type_state` | character | Status state (pre/in/post). |
| `status_type_completed` | logical | Whether the game is complete. |
| `status_type_description` | character | Status type description. |
| `status_type_detail` | character | Status type detail. |
| `status_type_short_detail` | character | Status type short detail. |
| `status_clock` | double | Game clock in seconds as ESPN reports it: time remaining in the period for clock sports, elapsed seconds for soccer (e.g. 5400.0 at full time); 0.0 once a game has ended. |
| `status_display_clock` | character | Status display clock. |
| `status_period` | integer | Current or final period number (quarter, half, inning or period, depending on the sport). |
| `neutral_site` | logical | Whether the match is played at a neutral venue. |
| `conference_competition` | logical | Conference competition. |
| `attendance` | integer | Reported attendance (NA on the redesigned page). |
| `venue_id` | character | MLBAM venue ID. |
| `venue_full_name` | character | Venue full name. |
| `venue_city` | character | Venue city. |
| `venue_state` | character | Venue state / province. |
| `venue_indoor` | logical | Whether the home venue is indoors. |
| `broadcast` | character | Broadcast information string. |
| `note` | character | Event note text from the competition (e.g. a series or game label such as 'World Series - Game 1', or a shootout result); an empty string when there is none. |
| `home_id` | character | Unique identifier for home. |
| `home_name` | character | Home team display name. |
| `home_abbreviation` | character | Home team's abbreviation. |
| `home_display_name` | character | Home team display name. |
| `home_location` | character | Home team's location. |
| `home_color` | character | Home team primary color hex. |
| `home_alternate_color` | character | Color code (hex) for home alternate. |
| `home_logo` | character | Home team logo URL. |
| `home_score` | character | Home team's score. For cricket, the innings string (e.g. '161/5 (18/20 ov, target 156)'). |
| `home_winner` | logical | Whether the home team won. |
| `home_rank` | character | Home team rank (if ranked). |
| `away_id` | character | Unique identifier for away. |
| `away_name` | character | Away team display name. |
| `away_abbreviation` | character | Away team's abbreviation. |
| `away_display_name` | character | Away team display name. |
| `away_location` | character | Away team's location. |
| `away_color` | character | Away team primary color hex. |
| `away_alternate_color` | character | Color code (hex) for away alternate. |
| `away_logo` | character | Away team logo URL. |
| `away_score` | character | Away team's score. For cricket, the innings string. |
| `away_winner` | logical | Whether the away team won. |
| `away_rank` | character | Away team rank (if ranked). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_cdn_schedule-example}

```python
espn_mlb_cdn_schedule(date='20250415')
```

_Last validated n/a._

## espn_mlb_cdn_scoreboard

espn.com scoreboard page data for one day (one week for football), one row per game. The page's sbData block is a Site v2 scoreboard payload.

**Endpoint URL:** `GET https://cdn.espn.com/core/mlb/scoreboard`

**Valid URL:** [https://cdn.espn.com/core/mlb/scoreboard?xhr=1&date=20250415](https://cdn.espn.com/core/mlb/scoreboard?xhr=1&date=20250415)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `date` | `date` |  |  | `Y` | Single date (YYYYMMDD). Ignored by cfb and nfl, which are week-oriented. Defaults to today. |
| `week` | `week` |  |  | `Y` | Week number (cfb and nfl). |
| `year` | `season` |  |  | `Y` | Season year that `week` belongs to (cfb and nfl). |
| `seasontype` | `season_type` |  |  | `Y` | Season phase for `week`: 1=preseason, 2=regular season, 3=postseason (cfb and nfl). |

### Returns {#espn_mlb_cdn_scoreboard-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | character | ESPN event id. |
| `uid` | character | ESPN UID string. |
| `date` | character | Match start timestamp (ISO 8601, UTC). |
| `name` | character | Full event name (e.g. 'Team A at Team B'). |
| `short_name` | character | Abbreviated event name (e.g. 'TA @ TB'). |
| `season_year` | integer | Integer season year ESPN assigns the event (e.g. 2025 for the 2025-26 season). |
| `season_type` | integer | ESPN season-type id of the event's season: 1 preseason, 2 regular season, 3 postseason, 4 offseason for the US leagues; soccer competitions carry their own competition-specific ids (e.g. 13481). |
| `season_slug` | character | Season slug. |
| `status_type_id` | character | Unique identifier for status type. |
| `status_type_name` | character | Status type name. |
| `status_type_state` | character | Status state (pre/in/post). |
| `status_type_completed` | logical | Whether the game is complete. |
| `status_type_description` | character | Status type description. |
| `status_type_detail` | character | Status type detail. |
| `status_type_short_detail` | character | Status type short detail. |
| `status_clock` | double | Game clock in seconds as ESPN reports it: time remaining in the period for clock sports, elapsed seconds for soccer (e.g. 5400.0 at full time); 0.0 once a game has ended. |
| `status_display_clock` | character | Status display clock. |
| `status_period` | integer | Current or final period number (quarter, half, inning or period, depending on the sport). |
| `neutral_site` | logical | Whether the match is played at a neutral venue. |
| `conference_competition` | logical | Conference competition. |
| `attendance` | integer | Reported attendance (NA on the redesigned page). |
| `venue_id` | character | MLBAM venue ID. |
| `venue_full_name` | character | Venue full name. |
| `venue_city` | character | Venue city. |
| `venue_state` | character | Venue state / province. |
| `venue_indoor` | logical | Whether the home venue is indoors. |
| `broadcast` | character | Broadcast information string. |
| `note` | character | Event note text from the competition (e.g. a series or game label such as 'World Series - Game 1', or a shootout result); an empty string when there is none. |
| `home_id` | character | Unique identifier for home. |
| `home_name` | character | Home team display name. |
| `home_abbreviation` | character | Home team's abbreviation. |
| `home_display_name` | character | Home team display name. |
| `home_location` | character | Home team's location. |
| `home_color` | character | Home team primary color hex. |
| `home_alternate_color` | character | Color code (hex) for home alternate. |
| `home_logo` | character | Home team logo URL. |
| `home_score` | character | Home team's score. For cricket, the innings string (e.g. '161/5 (18/20 ov, target 156)'). |
| `home_winner` | logical | Whether the home team won. |
| `home_rank` | character | Home team rank (if ranked). |
| `away_id` | character | Unique identifier for away. |
| `away_name` | character | Away team display name. |
| `away_abbreviation` | character | Away team's abbreviation. |
| `away_display_name` | character | Away team display name. |
| `away_location` | character | Away team's location. |
| `away_color` | character | Away team primary color hex. |
| `away_alternate_color` | character | Color code (hex) for away alternate. |
| `away_logo` | character | Away team logo URL. |
| `away_score` | character | Away team's score. For cricket, the innings string. |
| `away_winner` | logical | Whether the away team won. |
| `away_rank` | character | Away team rank (if ranked). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_cdn_scoreboard-example}

```python
espn_mlb_cdn_scoreboard(date='20250415')
```

_Last validated n/a._
