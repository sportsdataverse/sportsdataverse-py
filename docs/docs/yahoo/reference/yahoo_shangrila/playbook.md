---
title: "YAHOO — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Playbook"
sidebar_label: "Playbook"
sidebar_position: 5
description: "YAHOO — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Playbook — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# YAHOO — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Playbook

## yahoo_playbook_boxscore

Yahoo shangrila persisted query `playbookBoxscore` -> tables: football_positions, football_stat_types, games

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookBoxscore`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookBoxscore](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookBoxscore)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameId` | `game_id` |  |  | `Y` | gameId query parameter. |
| `standingsSeasonPhases` | `standings_season_phases` |  |  | `Y` | standingsSeasonPhases query parameter. |
| `imageHeight` | `image_height` |  |  | `Y` | imageHeight query parameter. |
| `imageWidth` | `image_width` |  |  | `Y` | imageWidth query parameter. |
| `isBaseball` | `is_baseball` |  |  | `Y` | isBaseball query parameter. |
| `isFootball` | `is_football` |  |  | `Y` | isFootball query parameter. |
| `isProBasketball` | `is_pro_basketball` |  |  | `Y` | isProBasketball query parameter. |
| `isCollegeBasketball` | `is_college_basketball` |  |  | `Y` | isCollegeBasketball query parameter. |
| `isHockey` | `is_hockey` |  |  | `Y` | isHockey query parameter. |
| `isSoccer` | `is_soccer` |  |  | `Y` | isSoccer query parameter. |
| `eventState` | `event_state` |  |  | `Y` | eventState query parameter. |

### Returns {#yahoo_playbook_boxscore-returns}

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**football_positions**

| col_name | type | description |
|---|---|---|
| `position_id` | character |  |
| `name` | character |  |
| `abbreviation` | character |  |

**football_stat_types**

| col_name | type | description |
|---|---|---|
| `stat_id` | character | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `abbreviation` | character |  |
| `display_name` | character |  |
| `short_name` | character |  |
| `context_agnostic_abbreviation` | character | Abbreviation for the statistic that still reads correctly outside its category (e.g., "PassYds" where the in-category abbreviation is only "Yds"). |
| `sort_order` | character |  |

**games**

| col_name | type | description |
|---|---|---|
| `game_id` | character |  |
| `display_name` | character |  |
| `display_result` | character |  |
| `sport_name` | character |  |
| `league_name` | character |  |
| `league_full_name` | character | Full league name (e.g., "NCAA Football"). |
| `league_display_short` | character | Short league label used in navigation and compact UI (e.g., "NCAA FB"). |
| `league_short_name` | character |  |
| `league_sport` | character | Sport the league belongs to (e.g., "football"). |
| `league_alias` | character | JSON-encoded Yahoo alias object for the league, carrying its site URL and path. |
| `alias_url` | character | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `special_event_type` | character | Marker identifying a special framing for the game, such as a bowl game or neutral-site showcase. |
| `away_team_id` | character |  |
| `basic_away_team_abbreviation` | character | Short abbreviation for the away team used in compact displays, as carried on the boxscore's lightweight team node. |
| `basic_away_team_display_name` | character | Display name of the away team as shown on the scoreboard, as carried on the boxscore's lightweight team node. |
| `basic_away_team_team_id` | character | Yahoo composite team id of the away team (e.g., "ncaaf.t.29"), as carried on the boxscore's lightweight team node. |
| `basic_away_team_team_logo_white` | character | JSON-encoded image node for the away team's white knockout logo, used on dark backgrounds, as carried on the boxscore's lightweight team node. |
| `basic_away_team_team_logo` | character | JSON-encoded image node for the away team's standard logo, as carried on the boxscore's lightweight team node. |
| `basic_away_team_playoff_seeds` | character | JSON-encoded list of the away team's playoff-seed entries for the season, as carried on the boxscore's lightweight team node. |
| `basic_away_team_nickname` | character | Nickname or mascot of the away team, as carried on the boxscore's lightweight team node. |
| `basic_away_team_primary_color` | character | Primary brand color of the away team, as a hex RGB string without the leading hash, as carried on the boxscore's lightweight team node. |
| `basic_away_team_secondary_color` | character | Secondary brand color of the away team, as a hex RGB string without the leading hash, as carried on the boxscore's lightweight team node. |
| `away_team_full_name` | character |  |
| `away_team_team_id` | character | Yahoo composite team id of the away team (e.g., "ncaaf.t.29"). |
| `away_team_primary_color` | character | Primary brand color of the away team, as a hex RGB string without the leading hash. |
| `away_team_secondary_color` | character | Secondary brand color of the away team, as a hex RGB string without the leading hash. |
| `away_team_display_name` | character |  |
| `away_team_abbreviation` | character |  |
| `away_team_location` | character |  |
| `away_team_alias` | character | JSON-encoded Yahoo alias object for the away team, carrying its site URL and path. |
| `away_team_nickname` | character |  |
| `away_team_last_games` | character | JSON-encoded list of the away team's most recently completed games. |
| `away_team_team_logo_white` | character | JSON-encoded image node for the away team's white knockout logo, used on dark backgrounds. |
| `away_team_team_logo` | character | JSON-encoded image node for the away team's standard logo. |
| `away_team_team_logo_white_large` | character | JSON-encoded image node for the large-format white knockout away-team logo. |
| `away_team_team_logo_large` | character | JSON-encoded image node for the large-format away-team logo. |
| `away_team_team_standings` | character | JSON-encoded standings node for the away team, carrying its record, position and streak. |
| `away_team_gametime_ticket_url` | character | Gametime affiliate ticket-purchase URL for the away team's games. |
| `away_team_injured_players` | character | JSON-encoded list of away-team players currently carrying an injury designation. |
| `away_team_rank_polls` | character | JSON-encoded list of the poll rankings the away team currently holds. |
| `away_team_playoff_seeds` | character | JSON-encoded list of the away team's playoff-seed entries for the season. |
| `away_team_record` | character |  |
| `home_team_id` | character |  |
| `basic_home_team_abbreviation` | character | Short abbreviation for the home team used in compact displays, as carried on the boxscore's lightweight team node. |
| `basic_home_team_display_name` | character | Display name of the home team as shown on the scoreboard, as carried on the boxscore's lightweight team node. |
| `basic_home_team_team_id` | character | Yahoo composite team id of the home team (e.g., "ncaaf.t.29"), as carried on the boxscore's lightweight team node. |
| `basic_home_team_team_logo_white` | character | JSON-encoded image node for the home team's white knockout logo, used on dark backgrounds, as carried on the boxscore's lightweight team node. |
| `basic_home_team_team_logo` | character | JSON-encoded image node for the home team's standard logo, as carried on the boxscore's lightweight team node. |
| `basic_home_team_playoff_seeds` | character | JSON-encoded list of the home team's playoff-seed entries for the season, as carried on the boxscore's lightweight team node. |
| `basic_home_team_nickname` | character | Nickname or mascot of the home team, as carried on the boxscore's lightweight team node. |
| `basic_home_team_primary_color` | character | Primary brand color of the home team, as a hex RGB string without the leading hash, as carried on the boxscore's lightweight team node. |
| `basic_home_team_secondary_color` | character | Secondary brand color of the home team, as a hex RGB string without the leading hash, as carried on the boxscore's lightweight team node. |
| `home_team_full_name` | character |  |
| `home_team_team_id` | character | Yahoo composite team id of the home team (e.g., "ncaaf.t.29"). |
| `home_team_primary_color` | character | Primary brand color of the home team, as a hex RGB string without the leading hash. |
| `home_team_secondary_color` | character | Secondary brand color of the home team, as a hex RGB string without the leading hash. |
| `home_team_display_name` | character |  |
| `home_team_abbreviation` | character |  |
| `home_team_location` | character |  |
| `home_team_alias` | character | JSON-encoded Yahoo alias object for the home team, carrying its site URL and path. |
| `home_team_nickname` | character |  |
| `home_team_last_games` | character | JSON-encoded list of the home team's most recently completed games. |
| `home_team_team_logo_white` | character | JSON-encoded image node for the home team's white knockout logo, used on dark backgrounds. |
| `home_team_team_logo` | character | JSON-encoded image node for the home team's standard logo. |
| `home_team_team_logo_white_large` | character | JSON-encoded image node for the large-format white knockout home-team logo. |
| `home_team_team_logo_large` | character | JSON-encoded image node for the large-format home-team logo. |
| `home_team_team_standings` | character | JSON-encoded standings node for the home team, carrying its record, position and streak. |
| `home_team_gametime_ticket_url` | character | Gametime affiliate ticket-purchase URL for the home team's games. |
| `home_team_injured_players` | character | JSON-encoded list of home-team players currently carrying an injury designation. |
| `home_team_rank_polls` | character | JSON-encoded list of the poll rankings the home team currently holds. |
| `home_team_playoff_seeds` | character | JSON-encoded list of the home team's playoff-seed entries for the season. |
| `home_team_record` | character |  |
| `away_score` | integer |  |
| `home_score` | integer |  |
| `start_time` | character |  |
| `start_date` | character |  |
| `if_necessary` | character |  |
| `status` | character |  |
| `status_display_name` | character | Short game or event status as shown on the scoreboard (e.g., "Final", "12:00 pm ET"). |
| `full_status_display_name` | character | Long-form game status label including overtime and date context (e.g., "Final/OT"). |
| `season` | integer |  |
| `season_phase` | character | Phase of the season the game falls in (e.g., "season.phase.season"). |
| `time_left` | character |  |
| `is_halftime` | logical | Flag indicating that the game is currently stopped at halftime. |
| `tournament_id` | character |  |
| `week` | integer |  |
| `playoff_series` | character | JSON-encoded playoff-series node the game belongs to. |
| `winning_team_id` | character | Composite Yahoo team id of the side that won the game (e.g., "ncaaf.t.29"). |
| `broadcast_channels` | character | JSON-encoded list of the channels broadcasting the event. |
| `news_break_subtext` | character | Secondary line of the news-break banner attached to the game. |
| `news_break_title` | character | Headline of the news-break banner attached to the game. |
| `news_break_url` | character | URL of the article behind the game's news-break banner. |
| `news_break_uuid` | character | Yahoo content UUID of the article behind the game's news-break banner. |
| `brief` | character | Short editorial blurb summarizing the game's state or result. |
| `event_extended_display_name` | character | Long-form event title used for marquee games, such as a bowl or rivalry name. |
| `game_odds_summary_pregame_odds_display` | character | Pregame line formatted for display (e.g., "MICH -6.5"). |
| `game_odds_summary_favorite_id` | character | Yahoo composite team id of the pregame betting favorite. |
| `game_odds_summary_underdog_team_predicted_score` | character | Projected final score for the betting underdog implied by the pregame line. |
| `game_odds_summary_favorite_team_predicted_score` | character | Projected final score for the betting favorite implied by the pregame line. |
| `bets` | character | JSON-encoded list of the betting markets offered on the event (spread, moneyline and total). |
| `partial_game_bets` | character | JSON-encoded list of in-game betting markets covering only part of the game, such as halves or quarters. |
| `venue_display_name` | character | Name of the venue hosting the event. |
| `venue_city` | character |  |
| `venue_cover_type` | character | Whether the venue is open-air, domed or fitted with a retractable roof. |
| `venue_state` | character |  |
| `venue_venue_id` | character | Yahoo identifier of the venue hosting the event. |
| `venue_country` | character | Country the venue is located in. |
| `tv_coverage` | character | Network carrying the game, as a short broadcast abbreviation (e.g., "CBS", "ESPN"). |
| `weather` | character |  |
| `regular_season_series_series_score` | character | Formatted head-to-head record for the regular-season series between the two teams (e.g., "1-1"). |
| `regular_season_series_games` | character | JSON-encoded list of the games making up the regular-season series between the two teams. |
| `regular_season_series_first_home_team_id` | character | Yahoo team id of the side that hosted the first meeting of the regular-season series. |
| `regular_season_series_first_home_team_wins` | character | Wins recorded across the regular-season series by the side that hosted the first meeting. |
| `regular_season_series_first_away_team_id` | character | Yahoo team id of the side that visited in the first meeting of the regular-season series. |
| `regular_season_series_first_away_team_wins` | character | Wins recorded across the regular-season series by the side that visited in the first meeting. |
| `regular_season_series_winning_team_id` | character | Yahoo team id of the side leading, or having won, the regular-season series. |
| `game_win_probability_time_line_win_probability_timeline` | character | JSON-encoded series of win-probability observations across the course of the game. |
| `current_period_short_display_name` | character | Abbreviated label for the period in progress (e.g., "4th"). |
| `current_period_period` | character | Ordinal number of the period currently in progress within the game. |
| `current_period_display_name` | character | Full label for the period in progress (e.g., "4th Quarter"). |
| `current_period_overtime` | character | Flag indicating that the period in progress is an overtime period. |
| `regulation_periods` | character |  |
| `away_team_lineup` | character | JSON-encoded starting lineup fielded by the away team. |
| `home_team_lineup` | character | JSON-encoded starting lineup fielded by the home team. |
| `game_ticket_price` | character | Lowest available ticket price for the game from the Gametime affiliate feed, in US dollars. |
| `gametime_ticket_url` | character | Gametime affiliate ticket-purchase URL for the event or team. |
| `game_coverage` | character | JSON-encoded node describing which live feeds Yahoo carries for the game. |
| `down` | character |  |
| `distance` | character |  |
| `field_position` | character | Ball spot expressed on Yahoo's 0-100 field scale, measured toward the offense's target goal line. |
| `field_position_display_name` | character | Ball spot rendered the way a scoreboard shows it (e.g., "MICH 35"). |
| `home_timeouts_remaining` | integer |  |
| `away_timeouts_remaining` | integer |  |
| `timeouts_granted` | integer | Number of timeouts granted so far in the current period. |
| `team_possessing_ball` | character | Yahoo team id of the side currently possessing the ball. |
| `away_line_score` | character | JSON-encoded per-period scoring line for the away team. |
| `home_line_score` | character | JSON-encoded per-period scoring line for the home team. |
| `first_play` | character | JSON-encoded first play of the game or of the current period. |
| `last_play` | character |  |
| `recap_videos` | character | JSON-encoded list of recap videos published for the game. |
| `play_by_play` | character | JSON-encoded data-island pointer to the game's play-by-play collection in the same editorial payload. |
| `drives` | character | JSON-encoded data-island pointer to the game's drive collection; football only. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_playbook_boxscore-example}

```python
yahoo_playbook_boxscore()
```

_Last validated n/a._

## yahoo_playbook_boxscore_poll

Yahoo shangrila persisted query `playbookBoxscorePoll` -> tables: football_positions, football_stat_types, games

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookBoxscorePoll`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookBoxscorePoll](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookBoxscorePoll)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameId` | `game_id` |  |  | `Y` | gameId query parameter. |
| `standingsSeasonPhases` | `standings_season_phases` |  |  | `Y` | standingsSeasonPhases query parameter. |
| `isBaseball` | `is_baseball` |  |  | `Y` | isBaseball query parameter. |
| `isFootball` | `is_football` |  |  | `Y` | isFootball query parameter. |
| `isProBasketball` | `is_pro_basketball` |  |  | `Y` | isProBasketball query parameter. |
| `isCollegeBasketball` | `is_college_basketball` |  |  | `Y` | isCollegeBasketball query parameter. |
| `isHockey` | `is_hockey` |  |  | `Y` | isHockey query parameter. |
| `isSoccer` | `is_soccer` |  |  | `Y` | isSoccer query parameter. |
| `eventState` | `event_state` |  |  | `Y` | eventState query parameter. |

### Returns {#yahoo_playbook_boxscore_poll-returns}

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**football_positions**

| col_name | type | description |
|---|---|---|
| `position_id` | character |  |
| `name` | character |  |
| `abbreviation` | character |  |

**football_stat_types**

| col_name | type | description |
|---|---|---|
| `stat_id` | character | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `abbreviation` | character |  |
| `display_name` | character |  |
| `short_name` | character |  |
| `context_agnostic_abbreviation` | character | Abbreviation for the statistic that still reads correctly outside its category (e.g., "PassYds" where the in-category abbreviation is only "Yds"). |
| `sort_order` | character |  |

**games**

| col_name | type | description |
|---|---|---|
| `game_id` | character |  |
| `display_name` | character |  |
| `season` | integer |  |
| `league_name` | character |  |
| `league_full_name` | character | Full league name (e.g., "NCAA Football"). |
| `sport_name` | character |  |
| `alias_url` | character | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `away_score` | integer |  |
| `basic_away_team_abbreviation` | character | Short abbreviation for the away team used in compact displays, as carried on the boxscore's lightweight team node. |
| `basic_away_team_display_name` | character | Display name of the away team as shown on the scoreboard, as carried on the boxscore's lightweight team node. |
| `basic_away_team_team_id` | character | Yahoo composite team id of the away team (e.g., "ncaaf.t.29"), as carried on the boxscore's lightweight team node. |
| `basic_away_team_team_logo_white` | character | JSON-encoded image node for the away team's white knockout logo, used on dark backgrounds, as carried on the boxscore's lightweight team node. |
| `basic_away_team_team_logo` | character | JSON-encoded image node for the away team's standard logo, as carried on the boxscore's lightweight team node. |
| `basic_away_team_playoff_seeds` | character | JSON-encoded list of the away team's playoff-seed entries for the season, as carried on the boxscore's lightweight team node. |
| `basic_away_team_nickname` | character | Nickname or mascot of the away team, as carried on the boxscore's lightweight team node. |
| `basic_away_team_primary_color` | character | Primary brand color of the away team, as a hex RGB string without the leading hash, as carried on the boxscore's lightweight team node. |
| `basic_away_team_secondary_color` | character | Secondary brand color of the away team, as a hex RGB string without the leading hash, as carried on the boxscore's lightweight team node. |
| `away_team_full_name` | character |  |
| `away_team_team_id` | character | Yahoo composite team id of the away team (e.g., "ncaaf.t.29"). |
| `away_team_primary_color` | character | Primary brand color of the away team, as a hex RGB string without the leading hash. |
| `away_team_secondary_color` | character | Secondary brand color of the away team, as a hex RGB string without the leading hash. |
| `away_team_display_name` | character |  |
| `away_team_abbreviation` | character |  |
| `away_team_location` | character |  |
| `away_team_alias` | character | JSON-encoded Yahoo alias object for the away team, carrying its site URL and path. |
| `away_team_nickname` | character |  |
| `away_team_team_logo_white` | character | JSON-encoded image node for the away team's white knockout logo, used on dark backgrounds. |
| `away_team_team_logo` | character | JSON-encoded image node for the away team's standard logo. |
| `away_team_team_logo_white_large` | character | JSON-encoded image node for the large-format white knockout away-team logo. |
| `away_team_team_logo_large` | character | JSON-encoded image node for the large-format away-team logo. |
| `away_team_team_standings` | character | JSON-encoded standings node for the away team, carrying its record, position and streak. |
| `away_team_rank_polls` | character | JSON-encoded list of the poll rankings the away team currently holds. |
| `away_team_playoff_seeds` | character | JSON-encoded list of the away team's playoff-seed entries for the season. |
| `away_team_record` | character |  |
| `away_team_id` | character |  |
| `bets` | character | JSON-encoded list of the betting markets offered on the event (spread, moneyline and total). |
| `partial_game_bets` | character | JSON-encoded list of in-game betting markets covering only part of the game, such as halves or quarters. |
| `brief` | character | Short editorial blurb summarizing the game's state or result. |
| `broadcast_channels` | character | JSON-encoded list of the channels broadcasting the event. |
| `home_score` | integer |  |
| `home_team_full_name` | character |  |
| `home_team_team_id` | character | Yahoo composite team id of the home team (e.g., "ncaaf.t.29"). |
| `home_team_primary_color` | character | Primary brand color of the home team, as a hex RGB string without the leading hash. |
| `home_team_secondary_color` | character | Secondary brand color of the home team, as a hex RGB string without the leading hash. |
| `home_team_display_name` | character |  |
| `home_team_abbreviation` | character |  |
| `home_team_location` | character |  |
| `home_team_alias` | character | JSON-encoded Yahoo alias object for the home team, carrying its site URL and path. |
| `home_team_nickname` | character |  |
| `home_team_team_logo_white` | character | JSON-encoded image node for the home team's white knockout logo, used on dark backgrounds. |
| `home_team_team_logo` | character | JSON-encoded image node for the home team's standard logo. |
| `home_team_team_logo_white_large` | character | JSON-encoded image node for the large-format white knockout home-team logo. |
| `home_team_team_logo_large` | character | JSON-encoded image node for the large-format home-team logo. |
| `home_team_team_standings` | character | JSON-encoded standings node for the home team, carrying its record, position and streak. |
| `home_team_rank_polls` | character | JSON-encoded list of the poll rankings the home team currently holds. |
| `home_team_playoff_seeds` | character | JSON-encoded list of the home team's playoff-seed entries for the season. |
| `home_team_record` | character |  |
| `home_team_id` | character |  |
| `basic_home_team_abbreviation` | character | Short abbreviation for the home team used in compact displays, as carried on the boxscore's lightweight team node. |
| `basic_home_team_display_name` | character | Display name of the home team as shown on the scoreboard, as carried on the boxscore's lightweight team node. |
| `basic_home_team_team_id` | character | Yahoo composite team id of the home team (e.g., "ncaaf.t.29"), as carried on the boxscore's lightweight team node. |
| `basic_home_team_team_logo_white` | character | JSON-encoded image node for the home team's white knockout logo, used on dark backgrounds, as carried on the boxscore's lightweight team node. |
| `basic_home_team_team_logo` | character | JSON-encoded image node for the home team's standard logo, as carried on the boxscore's lightweight team node. |
| `basic_home_team_playoff_seeds` | character | JSON-encoded list of the home team's playoff-seed entries for the season, as carried on the boxscore's lightweight team node. |
| `basic_home_team_nickname` | character | Nickname or mascot of the home team, as carried on the boxscore's lightweight team node. |
| `basic_home_team_primary_color` | character | Primary brand color of the home team, as a hex RGB string without the leading hash, as carried on the boxscore's lightweight team node. |
| `basic_home_team_secondary_color` | character | Secondary brand color of the home team, as a hex RGB string without the leading hash, as carried on the boxscore's lightweight team node. |
| `start_date` | character |  |
| `start_time` | character |  |
| `status` | character |  |
| `status_display_name` | character | Short game or event status as shown on the scoreboard (e.g., "Final", "12:00 pm ET"). |
| `full_status_display_name` | character | Long-form game status label including overtime and date context (e.g., "Final/OT"). |
| `time_left` | character |  |
| `venue_display_name` | character | Name of the venue hosting the event. |
| `venue_city` | character |  |
| `venue_cover_type` | character | Whether the venue is open-air, domed or fitted with a retractable roof. |
| `venue_state` | character |  |
| `venue_venue_id` | character | Yahoo identifier of the venue hosting the event. |
| `venue_country` | character | Country the venue is located in. |
| `weather` | character |  |
| `game_win_probability_time_line_win_probability_timeline` | character | JSON-encoded series of win-probability observations across the course of the game. |
| `playoff_series` | character | JSON-encoded playoff-series node the game belongs to. |
| `regular_season_series_games` | character | JSON-encoded list of the games making up the regular-season series between the two teams. |
| `is_halftime` | logical | Flag indicating that the game is currently stopped at halftime. |
| `regulation_periods` | character |  |
| `current_period_short_display_name` | character | Abbreviated label for the period in progress (e.g., "4th"). |
| `current_period_period` | character | Ordinal number of the period currently in progress within the game. |
| `current_period_display_name` | character | Full label for the period in progress (e.g., "4th Quarter"). |
| `current_period_overtime` | character | Flag indicating that the period in progress is an overtime period. |
| `away_team_lineup` | character | JSON-encoded starting lineup fielded by the away team. |
| `home_team_lineup` | character | JSON-encoded starting lineup fielded by the home team. |
| `winning_team_id` | character | Composite Yahoo team id of the side that won the game (e.g., "ncaaf.t.29"). |
| `game_coverage` | character | JSON-encoded node describing which live feeds Yahoo carries for the game. |
| `away_line_score` | character | JSON-encoded per-period scoring line for the away team. |
| `away_timeouts_remaining` | integer |  |
| `down` | character |  |
| `distance` | character |  |
| `field_position_display_name` | character | Ball spot rendered the way a scoreboard shows it (e.g., "MICH 35"). |
| `field_position` | character | Ball spot expressed on Yahoo's 0-100 field scale, measured toward the offense's target goal line. |
| `home_line_score` | character | JSON-encoded per-period scoring line for the home team. |
| `home_timeouts_remaining` | integer |  |
| `team_possessing_ball` | character | Yahoo team id of the side currently possessing the ball. |
| `drives` | character | JSON-encoded data-island pointer to the game's drive collection; football only. |
| `first_play` | character | JSON-encoded first play of the game or of the current period. |
| `play_by_play` | character | JSON-encoded data-island pointer to the game's play-by-play collection in the same editorial payload. |
| `timeouts_granted` | integer | Number of timeouts granted so far in the current period. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_playbook_boxscore_poll-example}

```python
yahoo_playbook_boxscore_poll()
```

_Last validated n/a._

## yahoo_playbook_boxscore_social_share

Yahoo shangrila persisted query `playbookBoxscoreSocialShare` -> one row per `games` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookBoxscoreSocialShare`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookBoxscoreSocialShare](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookBoxscoreSocialShare)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameId` | `game_id` |  |  | `Y` | gameId query parameter. |

### Returns {#yahoo_playbook_boxscore_social_share-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | character |  |
| `away_team_active` | character |  |
| `away_team_primary_color` | character | Primary brand color of the away team, as a hex RGB string without the leading hash. |
| `away_team_secondary_color` | character | Secondary brand color of the away team, as a hex RGB string without the leading hash. |
| `away_team_team_logo_white` | character | JSON-encoded image node for the away team's white knockout logo, used on dark backgrounds. |
| `away_team_team_logo` | character | JSON-encoded image node for the away team's standard logo. |
| `home_team_active` | character |  |
| `home_team_primary_color` | character | Primary brand color of the home team, as a hex RGB string without the leading hash. |
| `home_team_secondary_color` | character | Secondary brand color of the home team, as a hex RGB string without the leading hash. |
| `home_team_team_logo_white` | character | JSON-encoded image node for the home team's white knockout logo, used on dark backgrounds. |
| `home_team_team_logo` | character | JSON-encoded image node for the home team's standard logo. |
| `league_league_logo` | character | JSON-encoded image node for the league's logo. |
| `start_time` | character |  |
| `start_date` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_playbook_boxscore_social_share-example}

```python
yahoo_playbook_boxscore_social_share()
```

_Last validated n/a._

## yahoo_playbook_combat_match

Yahoo shangrila persisted query `playbookCombatMatch` -> one row per `games` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookCombatMatch`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookCombatMatch](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookCombatMatch)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameId` | `game_id` |  |  | `Y` | gameId query parameter. |
| `imageHeight` | `image_height` |  |  | `Y` | imageHeight query parameter. |
| `imageWidth` | `image_width` |  |  | `Y` | imageWidth query parameter. |
| `headshotHeight` | `headshot_height` |  |  | `Y` | headshotHeight query parameter. |
| `headshotWidth` | `headshot_width` |  |  | `Y` | headshotWidth query parameter. |

### Returns {#yahoo_playbook_combat_match-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_yahoo_shangrila`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_playbook_combat_match-example}

```python
yahoo_playbook_combat_match()
```

_Last validated n/a._

## yahoo_playbook_game

Yahoo shangrila persisted query `playbookGame` -> one row per `games` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookGame`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookGame](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookGame)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameId` | `game_id` |  |  | `Y` | gameId query parameter. |
| `imageHeight` | `image_height` |  |  | `Y` | imageHeight query parameter. |
| `imageWidth` | `image_width` |  |  | `Y` | imageWidth query parameter. |

### Returns {#yahoo_playbook_game-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | character |  |
| `display_name` | character |  |
| `league_name` | character |  |
| `league_full_name` | character | Full league name (e.g., "NCAA Football"). |
| `league_display_short` | character | Short league label used in navigation and compact UI (e.g., "NCAA FB"). |
| `league_short_name` | character |  |
| `league_sport` | character | Sport the league belongs to (e.g., "football"). |
| `league_alias` | character | JSON-encoded Yahoo alias object for the league, carrying its site URL and path. |
| `alias_url` | character | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `away_team_id` | character |  |
| `away_team_full_name` | character |  |
| `away_team_team_id` | character | Yahoo composite team id of the away team (e.g., "ncaaf.t.29"). |
| `away_team_primary_color` | character | Primary brand color of the away team, as a hex RGB string without the leading hash. |
| `away_team_secondary_color` | character | Secondary brand color of the away team, as a hex RGB string without the leading hash. |
| `away_team_display_name` | character |  |
| `away_team_abbreviation` | character |  |
| `away_team_location` | character |  |
| `away_team_alias` | character | JSON-encoded Yahoo alias object for the away team, carrying its site URL and path. |
| `away_team_nickname` | character |  |
| `away_team_last_games` | character | JSON-encoded list of the away team's most recently completed games. |
| `away_team_team_logo_white` | character | JSON-encoded image node for the away team's white knockout logo, used on dark backgrounds. |
| `away_team_team_logo` | character | JSON-encoded image node for the away team's standard logo. |
| `away_team_team_standings` | character | JSON-encoded standings node for the away team, carrying its record, position and streak. |
| `away_team_players` | character | JSON-encoded roster of away-team players attached to the game. |
| `away_team_rank_polls` | character | JSON-encoded list of the poll rankings the away team currently holds. |
| `away_team_playoff_seeds` | character | JSON-encoded list of the away team's playoff-seed entries for the season. |
| `home_team_id` | character |  |
| `home_team_full_name` | character |  |
| `home_team_team_id` | character | Yahoo composite team id of the home team (e.g., "ncaaf.t.29"). |
| `home_team_primary_color` | character | Primary brand color of the home team, as a hex RGB string without the leading hash. |
| `home_team_secondary_color` | character | Secondary brand color of the home team, as a hex RGB string without the leading hash. |
| `home_team_display_name` | character |  |
| `home_team_abbreviation` | character |  |
| `home_team_location` | character |  |
| `home_team_alias` | character | JSON-encoded Yahoo alias object for the home team, carrying its site URL and path. |
| `home_team_nickname` | character |  |
| `home_team_last_games` | character | JSON-encoded list of the home team's most recently completed games. |
| `home_team_team_logo_white` | character | JSON-encoded image node for the home team's white knockout logo, used on dark backgrounds. |
| `home_team_team_logo` | character | JSON-encoded image node for the home team's standard logo. |
| `home_team_team_standings` | character | JSON-encoded standings node for the home team, carrying its record, position and streak. |
| `home_team_players` | character | JSON-encoded roster of home-team players attached to the game. |
| `home_team_rank_polls` | character | JSON-encoded list of the poll rankings the home team currently holds. |
| `home_team_playoff_seeds` | character | JSON-encoded list of the home team's playoff-seed entries for the season. |
| `away_score` | integer |  |
| `home_score` | integer |  |
| `start_time` | character |  |
| `start_date` | character |  |
| `if_necessary` | character |  |
| `status` | character |  |
| `status_display_name` | character | Short game or event status as shown on the scoreboard (e.g., "Final", "12:00 pm ET"). |
| `season` | integer |  |
| `season_phase` | character | Phase of the season the game falls in (e.g., "season.phase.season"). |
| `time_left` | character |  |
| `tournament_id` | character |  |
| `playoff_series` | character | JSON-encoded playoff-series node the game belongs to. |
| `winning_team_id` | character | Composite Yahoo team id of the side that won the game (e.g., "ncaaf.t.29"). |
| `broadcast_channels` | character | JSON-encoded list of the channels broadcasting the event. |
| `news_break_subtext` | character | Secondary line of the news-break banner attached to the game. |
| `news_break_title` | character | Headline of the news-break banner attached to the game. |
| `news_break_url` | character | URL of the article behind the game's news-break banner. |
| `news_break_uuid` | character | Yahoo content UUID of the article behind the game's news-break banner. |
| `brief` | character | Short editorial blurb summarizing the game's state or result. |
| `bets` | character | JSON-encoded list of the betting markets offered on the event (spread, moneyline and total). |
| `venue_display_name` | character | Name of the venue hosting the event. |
| `venue_city` | character |  |
| `venue_cover_type` | character | Whether the venue is open-air, domed or fitted with a retractable roof. |
| `venue_state` | character |  |
| `venue_venue_id` | character | Yahoo identifier of the venue hosting the event. |
| `venue_country` | character | Country the venue is located in. |
| `tv_coverage` | character | Network carrying the game, as a short broadcast abbreviation (e.g., "CBS", "ESPN"). |
| `weather` | character |  |
| `away_line_score` | character | JSON-encoded per-period scoring line for the away team. |
| `current_period_period` | character | Ordinal number of the period currently in progress within the game. |
| `field_position` | character | Ball spot expressed on Yahoo's 0-100 field scale, measured toward the offense's target goal line. |
| `field_position_display_name` | character | Ball spot rendered the way a scoreboard shows it (e.g., "MICH 35"). |
| `home_line_score` | character | JSON-encoded per-period scoring line for the home team. |
| `home_timeouts_remaining` | integer |  |
| `away_timeouts_remaining` | integer |  |
| `last_play` | character |  |
| `game_stat_leaders` | character | JSON-encoded pointer to the per-category statistical leaders for the game. |
| `team_possessing_ball` | character | Yahoo team id of the side currently possessing the ball. |
| `recap_videos` | character | JSON-encoded list of recap videos published for the game. |
| `week` | integer |  |
| `play_by_play` | character | JSON-encoded data-island pointer to the game's play-by-play collection in the same editorial payload. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_playbook_game-example}

```python
yahoo_playbook_game()
```

_Last validated n/a._

## yahoo_playbook_game_odds_poll

Yahoo shangrila persisted query `playbookGameOddsPoll` -> one row per `games` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookGameOddsPoll`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookGameOddsPoll](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookGameOddsPoll)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameId` | `game_id` |  |  | `Y` | gameId query parameter. |
| `eventState` | `event_state` |  |  | `Y` | eventState query parameter. |

### Returns {#yahoo_playbook_game_odds_poll-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | character |  |
| `bets` | character | JSON-encoded list of the betting markets offered on the event (spread, moneyline and total). |
| `partial_game_bets` | character | JSON-encoded list of in-game betting markets covering only part of the game, such as halves or quarters. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_playbook_game_odds_poll-example}

```python
yahoo_playbook_game_odds_poll()
```

_Last validated n/a._

## yahoo_playbook_golf_tournament

Yahoo shangrila persisted query `playbookGolfTournament` (response body not captured; shape unknown)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookGolfTournament`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookGolfTournament](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookGolfTournament)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameId` | `game_id` |  |  | `Y` | gameId query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `statIds` | `stat_ids` |  |  | `Y` | statIds query parameter. |
| `showHoleResults` | `show_hole_results` |  |  | `Y` | showHoleResults query parameter. |

### Returns {#yahoo_playbook_golf_tournament-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_yahoo_shangrila`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_playbook_golf_tournament-example}

```python
yahoo_playbook_golf_tournament()
```

_Last validated n/a._

## yahoo_playbook_league_odds

Yahoo shangrila persisted query `playbookLeagueOdds` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookLeagueOdds`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookLeagueOdds](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookLeagueOdds)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | league query parameter. |
| `dates` | `dates` |  |  | `Y` | Date or date range filter (YYYYMMDD or YYYYMMDD-YYYYMMDD). |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `startTimeFilter` | `start_time_filter` |  |  | `Y` | startTimeFilter query parameter. |
| `rangeStartDate` | `range_start_date` |  |  | `Y` | rangeStartDate query parameter. |
| `rangeEndDate` | `range_end_date` |  |  | `Y` | rangeEndDate query parameter. |

### Returns {#yahoo_playbook_league_odds-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `ncaaf_games` | character | JSON-encoded list of NCAAF game nodes carrying the pick or odds distribution for the slate. |
| `conferences` | character | JSON-encoded list of the league's conference nodes, each carrying an id, a name and its member teams. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_playbook_league_odds-example}

```python
yahoo_playbook_league_odds()
```

_Last validated n/a._

## yahoo_playbook_player

Yahoo shangrila persisted query `playbookPlayer` -> one row per `players` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookPlayer`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookPlayer](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookPlayer)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `playerId` | `player_id` |  |  | `Y` | playerId query parameter. |
| `seasonPhases` | `season_phases` |  |  | `Y` | seasonPhases query parameter. |

### Returns {#yahoo_playbook_player-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | character |  |
| `alias_url` | character | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `alias_subpages` | character | JSON-encoded list of subpage aliases (roster, schedule, stats) available beneath the entity's Yahoo page. |
| `sport_name` | character |  |
| `first_name` | character |  |
| `last_name` | character |  |
| `display_name` | character |  |
| `college` | character |  |
| `birth_state` | character |  |
| `birth_city` | character |  |
| `birth_country` | character |  |
| `birth_date` | character |  |
| `height` | integer |  |
| `display_height` | character |  |
| `weight` | integer |  |
| `status` | character |  |
| `active` | logical |  |
| `suggested_headshot` | character | JSON-encoded image node for the headshot Yahoo recommends for this player. |
| `uniform_number` | character | Jersey number the player wears for the team. |
| `positions` | character |  |
| `team_id` | character |  |
| `team_team_id` | character |  |
| `team_display_name` | character |  |
| `team_full_name` | character |  |
| `team_alias` | character | JSON-encoded alias object for the entity's team, carrying its Yahoo page URL and path. |
| `team_team_logo` | character | JSON-encoded image node for the team's standard logo. |
| `team_team_logo_white` | character | JSON-encoded image node for the team's white knockout logo. |
| `team_primary_color` | character | Primary brand color of the entity's team, as a hex RGB string without the leading hash. |
| `team_secondary_color` | character | Secondary brand color of the entity's team, as a hex RGB string without the leading hash. |
| `draft_position` | character | Round and pick at which the player was drafted, left null for undrafted players. |
| `player_seasons` | character | JSON-encoded list of the seasons for which Yahoo carries data on this player. |
| `header_stats_passing` | character | JSON-encoded headline passing statistics shown at the top of the player's page. |
| `season_stats_passing` | character | JSON-encoded full-season passing statistics for the player. |
| `header_stats_rushing` | character | JSON-encoded headline rushing statistics shown at the top of the player's page. |
| `season_stats_rushing` | character | JSON-encoded full-season rushing statistics for the player. |
| `header_stats_receiving` | character | JSON-encoded headline receiving statistics shown at the top of the player's page. |
| `season_stats_receiving` | character | JSON-encoded full-season receiving statistics for the player. |
| `header_stats_defense` | character | JSON-encoded headline defensive statistics shown at the top of the player's page. |
| `season_stats_defense` | character | JSON-encoded full-season defensive statistics for the player. |
| `header_stats_kicking` | character | JSON-encoded headline kicking statistics shown at the top of the player's page. |
| `season_stats_kicking` | character | JSON-encoded full-season kicking statistics for the player. |
| `header_stats_punting` | character | JSON-encoded headline punting statistics shown at the top of the player's page. |
| `season_stats_punting` | character | JSON-encoded full-season punting statistics for the player. |
| `earnings` | character | Prize money the player has earned over the covered period, in US dollars. |
| `first_year` | character | First season in which the player appeared in this league. |
| `last_year` | integer | Most recent season in which the player appeared in this league. |
| `injury` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_playbook_player-example}

```python
yahoo_playbook_player()
```

_Last validated n/a._

## yahoo_playbook_player_social_share

Yahoo shangrila persisted query `playbookPlayerSocialShare` -> one row per `players` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookPlayerSocialShare`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookPlayerSocialShare](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookPlayerSocialShare)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `playerId` | `player_id` |  |  | `Y` | playerId query parameter. |

### Returns {#yahoo_playbook_player_social_share-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `sport_name` | character |  |
| `short_display_name` | character |  |
| `suggested_headshot` | character | JSON-encoded image node for the headshot Yahoo recommends for this player. |
| `team_primary_color` | character | Primary brand color of the entity's team, as a hex RGB string without the leading hash. |
| `team_secondary_color` | character | Secondary brand color of the entity's team, as a hex RGB string without the leading hash. |
| `team_league` | character | JSON-encoded league node for the entity's team. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_playbook_player_social_share-example}

```python
yahoo_playbook_player_social_share()
```

_Last validated n/a._

## yahoo_playbook_race

Yahoo shangrila persisted query `playbookRace` -> one row per `games` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookRace`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookRace](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookRace)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameId` | `game_id` |  |  | `Y` | gameId query parameter. |
| `playerImageHeight` | `player_image_height` |  |  | `Y` | playerImageHeight query parameter. |
| `playerImageWidth` | `player_image_width` |  |  | `Y` | playerImageWidth query parameter. |

### Returns {#yahoo_playbook_race-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_yahoo_shangrila`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_playbook_race-example}

```python
yahoo_playbook_race()
```

_Last validated n/a._

## yahoo_playbook_team

Yahoo shangrila persisted query `playbookTeam` -> tables: teams, leagues

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookTeam`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookTeam](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookTeam)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `teamId` | `team_id` |  |  | `Y` | teamId query parameter. |
| `imageHeight` | `image_height` |  |  | `Y` | imageHeight query parameter. |
| `imageWidth` | `image_width` |  |  | `Y` | imageWidth query parameter. |
| `leagueShortName` | `league_short_name` |  |  | `Y` | leagueShortName query parameter. |
| `disableConference` | `disable_conference` |  |  | `Y` | disableConference query parameter. |
| `disableDivision` | `disable_division` |  |  | `Y` | disableDivision query parameter. |

### Returns {#yahoo_playbook_team-returns}

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**teams**

| col_name | type | description |
|---|---|---|
| `sport_sport_id` | character | Yahoo identifier of the sport the league belongs to. |
| `sport_name` | character |  |
| `league_name` | character |  |
| `league_short_name` | character |  |
| `league_current_season_phase` | character | Phase the league's season is currently in (e.g., "season.phase.season"). |
| `team_id` | character |  |
| `conference_id` | integer |  |
| `full_name` | character |  |
| `display_name` | character |  |
| `location` | character |  |
| `nickname` | character |  |
| `primary_color` | character |  |
| `secondary_color` | character |  |
| `team_logo_white_url` | character | Absolute URL of the team's white knockout logo, the variant used on dark backgrounds. |
| `team_logo_url` | character | Absolute URL of the team's standard logo image on Yahoo's image CDN. |
| `alias_navigation_links` | character | JSON-encoded map of navigation links (scores, standings, teams) hanging off the entity's Yahoo alias. |
| `alias_url` | character | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `alias_path` | character | Site-relative path portion of the entity's Yahoo alias (e.g., "/ncaaf/teams/tcu/"). |
| `last_games` | character | JSON-encoded list of the team's most recently completed games, used for form and streak displays. |
| `next_games` | character | JSON-encoded list of the team's next scheduled games. |
| `division_name` | character |  |
| `division_teams` | character | JSON-encoded list of the teams that make up the division. |
| `conference_short_name` | character |  |
| `conference_name` | character |  |
| `conference_conference_id` | character | Yahoo numeric identifier of the conference carried on the team's conference node. |
| `conference_team_standings` | character | JSON-encoded standings rows for every team in the conference. |
| `conference_abbreviation` | character |  |
| `team_standings_team` | character | JSON-encoded team node the standings row describes. |
| `team_standings_conference_id` | character | Yahoo numeric conference id for the team's standings row. |
| `team_standings_conference` | character | JSON-encoded conference node the standings row sits under. |
| `team_standings_display_name` | character | Display name of the team on its standings row. |
| `team_standings_full_name` | character | Full name of the team on its standings row. |
| `team_standings_position` | character | Rank of the team within the standings grouping it is listed in. |
| `team_standings_sequence` | character | Tie-break ordering value Yahoo uses to sequence teams holding identical records. |
| `team_standings_team_record` | character | Formatted overall record for the team (e.g., "8-2"). |
| `team_standings_conference_position` | character | Rank of the team within its conference standings. |
| `team_standings_points_for` | character | Points the team has scored over the standings period. |
| `team_standings_points_against` | character | Points the team has allowed over the standings period. |
| `team_standings_clinched_playoff` | character | Flag indicating that the team has clinched a playoff berth. |
| `team_standings_clinched_division` | character | Flag indicating that the team has clinched its division. |
| `team_standings_streak_display` | character | Formatted current streak for the team (e.g., "W3", "L2"). |
| `gametime_ticket_url` | character | Gametime affiliate ticket-purchase URL for the event or team. |
| `football_team_season_stats` | character | JSON-encoded team-level season statistics for the team's football side. |
| `football_player_season_stats` | character | JSON-encoded per-player season statistics for the team's football roster. |
| `injured_players` | character | JSON-encoded list of the team's players currently carrying an injury designation. |
| `transactions` | character | JSON-encoded list of the team's roster transactions over the requested window. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_playbook_team-example}

```python
yahoo_playbook_team()
```

_Last validated n/a._

## yahoo_playbook_team_basic

Yahoo shangrila persisted query `playbookTeamBasic` -> one row per `teams` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookTeamBasic`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookTeamBasic](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookTeamBasic)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `teamId` | `team_id` |  |  | `Y` | teamId query parameter. |
| `imageHeight` | `image_height` |  |  | `Y` | imageHeight query parameter. |
| `imageWidth` | `image_width` |  |  | `Y` | imageWidth query parameter. |

### Returns {#yahoo_playbook_team_basic-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `sport_sport_id` | character | Yahoo identifier of the sport the league belongs to. |
| `sport_name` | character |  |
| `league_name` | character |  |
| `league_short_name` | character |  |
| `league_current_season_phase` | character | Phase the league's season is currently in (e.g., "season.phase.season"). |
| `team_id` | character |  |
| `conference_id` | integer |  |
| `full_name` | character |  |
| `display_name` | character |  |
| `location` | character |  |
| `nickname` | character |  |
| `primary_color` | character |  |
| `secondary_color` | character |  |
| `team_logo_white_url` | character | Absolute URL of the team's white knockout logo, the variant used on dark backgrounds. |
| `team_logo_url` | character | Absolute URL of the team's standard logo image on Yahoo's image CDN. |
| `alias_navigation_links` | character | JSON-encoded map of navigation links (scores, standings, teams) hanging off the entity's Yahoo alias. |
| `alias_url` | character | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `alias_path` | character | Site-relative path portion of the entity's Yahoo alias (e.g., "/ncaaf/teams/tcu/"). |
| `last_games` | character | JSON-encoded list of the team's most recently completed games, used for form and streak displays. |
| `next_games` | character | JSON-encoded list of the team's next scheduled games. |
| `division_name` | character |  |
| `conference_short_name` | character |  |
| `conference_name` | character |  |
| `conference_conference_id` | character | Yahoo numeric identifier of the conference carried on the team's conference node. |
| `conference_abbreviation` | character |  |
| `team_standings_team` | character | JSON-encoded team node the standings row describes. |
| `team_standings_conference_id` | character | Yahoo numeric conference id for the team's standings row. |
| `team_standings_conference` | character | JSON-encoded conference node the standings row sits under. |
| `team_standings_display_name` | character | Display name of the team on its standings row. |
| `team_standings_full_name` | character | Full name of the team on its standings row. |
| `team_standings_position` | character | Rank of the team within the standings grouping it is listed in. |
| `team_standings_sequence` | character | Tie-break ordering value Yahoo uses to sequence teams holding identical records. |
| `team_standings_team_record` | character | Formatted overall record for the team (e.g., "8-2"). |
| `team_standings_conference_position` | character | Rank of the team within its conference standings. |
| `team_standings_points_for` | character | Points the team has scored over the standings period. |
| `team_standings_points_against` | character | Points the team has allowed over the standings period. |
| `team_standings_clinched_playoff` | character | Flag indicating that the team has clinched a playoff berth. |
| `team_standings_clinched_division` | character | Flag indicating that the team has clinched its division. |
| `team_standings_streak_display` | character | Formatted current streak for the team (e.g., "W3", "L2"). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_playbook_team_basic-example}

```python
yahoo_playbook_team_basic()
```

_Last validated n/a._

## yahoo_playbook_team_social_share

Yahoo shangrila persisted query `playbookTeamSocialShare` -> one row per `teams` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookTeamSocialShare`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookTeamSocialShare](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookTeamSocialShare)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `teamId` | `team_id` |  |  | `Y` | teamId query parameter. |

### Returns {#yahoo_playbook_team_social_share-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `sport_sport_id` | character | Yahoo identifier of the sport the league belongs to. |
| `sport_name` | character |  |
| `team_id` | character |  |
| `primary_color` | character |  |
| `team_logo_url` | character | Absolute URL of the team's standard logo image on Yahoo's image CDN. |
| `team_logo_white_url` | character | Absolute URL of the team's white knockout logo, the variant used on dark backgrounds. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_playbook_team_social_share-example}

```python
yahoo_playbook_team_social_share()
```

_Last validated n/a._

## yahoo_playbook_tennis_match

Yahoo shangrila persisted query `playbookTennisMatch` -> one row per `events` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookTennisMatch`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookTennisMatch](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookTennisMatch)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameId` | `game_id` |  |  | `Y` | gameId query parameter. |

### Returns {#yahoo_playbook_tennis_match-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_yahoo_shangrila`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_playbook_tennis_match-example}

```python
yahoo_playbook_tennis_match()
```

_Last validated n/a._
