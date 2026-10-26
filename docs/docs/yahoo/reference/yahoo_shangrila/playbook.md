---
title: "Yahoo Sports — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Playbook"
sidebar_label: "Playbook"
sidebar_position: 5
description: "Yahoo Sports — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Playbook — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# Yahoo Sports — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Playbook

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
| `position_id` | String |  |
| `name` | String |  |
| `abbreviation` | String |  |

**football_stat_types**

| col_name | type | description |
|---|---|---|
| `stat_id` | String | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `abbreviation` | String |  |
| `display_name` | String |  |
| `short_name` | String |  |
| `context_agnostic_abbreviation` | String | Abbreviation for the statistic that still reads correctly outside its category (e.g., "PassYds" where the in-category abbreviation is only "Yds"). |
| `sort_order` | String |  |

**games**

| col_name | type | description |
|---|---|---|
| `game_id` | String |  |
| `display_name` | String |  |
| `display_result` | String |  |
| `sport_name` | String |  |
| `league_name` | String |  |
| `league_full_name` | String | Full league name (e.g., "NCAA Football"). |
| `league_display_short` | String | Short league label used in navigation and compact UI (e.g., "NCAA FB"). |
| `league_short_name` | String |  |
| `league_sport` | String | Sport the league belongs to (e.g., "football"). |
| `league_alias` | String | JSON-encoded Yahoo alias object for the league, carrying its site URL and path. |
| `alias_url` | String | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `special_event_type` | String | Marker identifying a special framing for the game, such as a bowl game or neutral-site showcase. |
| `away_team_id` | String |  |
| `basic_away_team_abbreviation` | String | Short abbreviation for the away team used in compact displays, as carried on the boxscore's lightweight team node. |
| `basic_away_team_display_name` | String | Display name of the away team as shown on the scoreboard, as carried on the boxscore's lightweight team node. |
| `basic_away_team_team_id` | String | Yahoo composite team id of the away team (e.g., "ncaaf.t.29"), as carried on the boxscore's lightweight team node. |
| `basic_away_team_team_logo_white` | String | JSON-encoded image node for the away team's white knockout logo, used on dark backgrounds, as carried on the boxscore's lightweight team node. |
| `basic_away_team_team_logo` | String | JSON-encoded image node for the away team's standard logo, as carried on the boxscore's lightweight team node. |
| `basic_away_team_playoff_seeds` | String | JSON-encoded list of the away team's playoff-seed entries for the season, as carried on the boxscore's lightweight team node. |
| `basic_away_team_nickname` | String | Nickname or mascot of the away team, as carried on the boxscore's lightweight team node. |
| `basic_away_team_primary_color` | String | Primary brand color of the away team, as a hex RGB string without the leading hash, as carried on the boxscore's lightweight team node. |
| `basic_away_team_secondary_color` | String | Secondary brand color of the away team, as a hex RGB string without the leading hash, as carried on the boxscore's lightweight team node. |
| `away_team_full_name` | String |  |
| `away_team_team_id` | String | Yahoo composite team id of the away team (e.g., "ncaaf.t.29"). |
| `away_team_primary_color` | String | Primary brand color of the away team, as a hex RGB string without the leading hash. |
| `away_team_secondary_color` | String | Secondary brand color of the away team, as a hex RGB string without the leading hash. |
| `away_team_display_name` | String |  |
| `away_team_abbreviation` | String |  |
| `away_team_location` | String |  |
| `away_team_alias` | String | JSON-encoded Yahoo alias object for the away team, carrying its site URL and path. |
| `away_team_nickname` | String |  |
| `away_team_last_games` | String | JSON-encoded list of the away team's most recently completed games. |
| `away_team_team_logo_white` | String | JSON-encoded image node for the away team's white knockout logo, used on dark backgrounds. |
| `away_team_team_logo` | String | JSON-encoded image node for the away team's standard logo. |
| `away_team_team_logo_white_large` | String | JSON-encoded image node for the large-format white knockout away-team logo. |
| `away_team_team_logo_large` | String | JSON-encoded image node for the large-format away-team logo. |
| `away_team_team_standings` | String | JSON-encoded standings node for the away team, carrying its record, position and streak. |
| `away_team_gametime_ticket_url` | String | Gametime affiliate ticket-purchase URL for the away team's games. |
| `away_team_injured_players` | String | JSON-encoded list of away-team players currently carrying an injury designation. |
| `away_team_rank_polls` | String | JSON-encoded list of the poll rankings the away team currently holds. |
| `away_team_playoff_seeds` | String | JSON-encoded list of the away team's playoff-seed entries for the season. |
| `away_team_record` | String |  |
| `home_team_id` | String |  |
| `basic_home_team_abbreviation` | String | Short abbreviation for the home team used in compact displays, as carried on the boxscore's lightweight team node. |
| `basic_home_team_display_name` | String | Display name of the home team as shown on the scoreboard, as carried on the boxscore's lightweight team node. |
| `basic_home_team_team_id` | String | Yahoo composite team id of the home team (e.g., "ncaaf.t.29"), as carried on the boxscore's lightweight team node. |
| `basic_home_team_team_logo_white` | String | JSON-encoded image node for the home team's white knockout logo, used on dark backgrounds, as carried on the boxscore's lightweight team node. |
| `basic_home_team_team_logo` | String | JSON-encoded image node for the home team's standard logo, as carried on the boxscore's lightweight team node. |
| `basic_home_team_playoff_seeds` | String | JSON-encoded list of the home team's playoff-seed entries for the season, as carried on the boxscore's lightweight team node. |
| `basic_home_team_nickname` | String | Nickname or mascot of the home team, as carried on the boxscore's lightweight team node. |
| `basic_home_team_primary_color` | String | Primary brand color of the home team, as a hex RGB string without the leading hash, as carried on the boxscore's lightweight team node. |
| `basic_home_team_secondary_color` | String | Secondary brand color of the home team, as a hex RGB string without the leading hash, as carried on the boxscore's lightweight team node. |
| `home_team_full_name` | String |  |
| `home_team_team_id` | String | Yahoo composite team id of the home team (e.g., "ncaaf.t.29"). |
| `home_team_primary_color` | String | Primary brand color of the home team, as a hex RGB string without the leading hash. |
| `home_team_secondary_color` | String | Secondary brand color of the home team, as a hex RGB string without the leading hash. |
| `home_team_display_name` | String |  |
| `home_team_abbreviation` | String |  |
| `home_team_location` | String |  |
| `home_team_alias` | String | JSON-encoded Yahoo alias object for the home team, carrying its site URL and path. |
| `home_team_nickname` | String |  |
| `home_team_last_games` | String | JSON-encoded list of the home team's most recently completed games. |
| `home_team_team_logo_white` | String | JSON-encoded image node for the home team's white knockout logo, used on dark backgrounds. |
| `home_team_team_logo` | String | JSON-encoded image node for the home team's standard logo. |
| `home_team_team_logo_white_large` | String | JSON-encoded image node for the large-format white knockout home-team logo. |
| `home_team_team_logo_large` | String | JSON-encoded image node for the large-format home-team logo. |
| `home_team_team_standings` | String | JSON-encoded standings node for the home team, carrying its record, position and streak. |
| `home_team_gametime_ticket_url` | String | Gametime affiliate ticket-purchase URL for the home team's games. |
| `home_team_injured_players` | String | JSON-encoded list of home-team players currently carrying an injury designation. |
| `home_team_rank_polls` | String | JSON-encoded list of the poll rankings the home team currently holds. |
| `home_team_playoff_seeds` | String | JSON-encoded list of the home team's playoff-seed entries for the season. |
| `home_team_record` | String |  |
| `away_score` | Int64 |  |
| `home_score` | Int64 |  |
| `start_time` | String |  |
| `start_date` | String |  |
| `if_necessary` | String |  |
| `status` | String |  |
| `status_display_name` | String | Short game or event status as shown on the scoreboard (e.g., "Final", "12:00 pm ET"). |
| `full_status_display_name` | String | Long-form game status label including overtime and date context (e.g., "Final/OT"). |
| `season` | Int64 |  |
| `season_phase` | String | Phase of the season the game falls in (e.g., "season.phase.season"). |
| `time_left` | String |  |
| `is_halftime` | Boolean | Flag indicating that the game is currently stopped at halftime. |
| `tournament_id` | String |  |
| `week` | Int64 |  |
| `playoff_series` | String | JSON-encoded playoff-series node the game belongs to. |
| `winning_team_id` | String | Composite Yahoo team id of the side that won the game (e.g., "ncaaf.t.29"). |
| `broadcast_channels` | String | JSON-encoded list of the channels broadcasting the event. |
| `news_break_subtext` | String | Secondary line of the news-break banner attached to the game. |
| `news_break_title` | String | Headline of the news-break banner attached to the game. |
| `news_break_url` | String | URL of the article behind the game's news-break banner. |
| `news_break_uuid` | String | Yahoo content UUID of the article behind the game's news-break banner. |
| `brief` | String | Short editorial blurb summarizing the game's state or result. |
| `event_extended_display_name` | String | Long-form event title used for marquee games, such as a bowl or rivalry name. |
| `game_odds_summary_pregame_odds_display` | String | Pregame line formatted for display (e.g., "MICH -6.5"). |
| `game_odds_summary_favorite_id` | String | Yahoo composite team id of the pregame betting favorite. |
| `game_odds_summary_underdog_team_predicted_score` | String | Projected final score for the betting underdog implied by the pregame line. |
| `game_odds_summary_favorite_team_predicted_score` | String | Projected final score for the betting favorite implied by the pregame line. |
| `bets` | String | JSON-encoded list of the betting markets offered on the event (spread, moneyline and total). |
| `partial_game_bets` | String | JSON-encoded list of in-game betting markets covering only part of the game, such as halves or quarters. |
| `venue_display_name` | String | Name of the venue hosting the event. |
| `venue_city` | String |  |
| `venue_cover_type` | String | Whether the venue is open-air, domed or fitted with a retractable roof. |
| `venue_state` | String |  |
| `venue_venue_id` | String | Yahoo identifier of the venue hosting the event. |
| `venue_country` | String | Country the venue is located in. |
| `tv_coverage` | String | Network carrying the game, as a short broadcast abbreviation (e.g., "CBS", "ESPN"). |
| `weather` | String |  |
| `regular_season_series_series_score` | String | Formatted head-to-head record for the regular-season series between the two teams (e.g., "1-1"). |
| `regular_season_series_games` | String | JSON-encoded list of the games making up the regular-season series between the two teams. |
| `regular_season_series_first_home_team_id` | String | Yahoo team id of the side that hosted the first meeting of the regular-season series. |
| `regular_season_series_first_home_team_wins` | String | Wins recorded across the regular-season series by the side that hosted the first meeting. |
| `regular_season_series_first_away_team_id` | String | Yahoo team id of the side that visited in the first meeting of the regular-season series. |
| `regular_season_series_first_away_team_wins` | String | Wins recorded across the regular-season series by the side that visited in the first meeting. |
| `regular_season_series_winning_team_id` | String | Yahoo team id of the side leading, or having won, the regular-season series. |
| `game_win_probability_time_line_win_probability_timeline` | String | JSON-encoded series of win-probability observations across the course of the game. |
| `current_period_short_display_name` | String | Abbreviated label for the period in progress (e.g., "4th"). |
| `current_period_period` | String | Ordinal number of the period currently in progress within the game. |
| `current_period_display_name` | String | Full label for the period in progress (e.g., "4th Quarter"). |
| `current_period_overtime` | String | Flag indicating that the period in progress is an overtime period. |
| `regulation_periods` | String |  |
| `away_team_lineup` | String | JSON-encoded starting lineup fielded by the away team. |
| `home_team_lineup` | String | JSON-encoded starting lineup fielded by the home team. |
| `game_ticket_price` | String | Lowest available ticket price for the game from the Gametime affiliate feed, in US dollars. |
| `gametime_ticket_url` | String | Gametime affiliate ticket-purchase URL for the event or team. |
| `game_coverage` | String | JSON-encoded node describing which live feeds Yahoo carries for the game. |
| `down` | String |  |
| `distance` | String |  |
| `field_position` | String | Ball spot expressed on Yahoo's 0-100 field scale, measured toward the offense's target goal line. |
| `field_position_display_name` | String | Ball spot rendered the way a scoreboard shows it (e.g., "MICH 35"). |
| `home_timeouts_remaining` | Int64 |  |
| `away_timeouts_remaining` | Int64 |  |
| `timeouts_granted` | Int64 | Number of timeouts granted so far in the current period. |
| `team_possessing_ball` | String | Yahoo team id of the side currently possessing the ball. |
| `away_line_score` | String | JSON-encoded per-period scoring line for the away team. |
| `home_line_score` | String | JSON-encoded per-period scoring line for the home team. |
| `first_play` | String | JSON-encoded first play of the game or of the current period. |
| `last_play` | String |  |
| `recap_videos` | String | JSON-encoded list of recap videos published for the game. |
| `play_by_play` | String | JSON-encoded data-island pointer to the game's play-by-play collection in the same editorial payload. |
| `drives` | String | JSON-encoded data-island pointer to the game's drive collection; football only. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_playbook_boxscore-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_playbook_boxscore-example}

```python
yahoo_playbook_boxscore()
```

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
| `position_id` | String |  |
| `name` | String |  |
| `abbreviation` | String |  |

**football_stat_types**

| col_name | type | description |
|---|---|---|
| `stat_id` | String | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `abbreviation` | String |  |
| `display_name` | String |  |
| `short_name` | String |  |
| `context_agnostic_abbreviation` | String | Abbreviation for the statistic that still reads correctly outside its category (e.g., "PassYds" where the in-category abbreviation is only "Yds"). |
| `sort_order` | String |  |

**games**

| col_name | type | description |
|---|---|---|
| `game_id` | String |  |
| `display_name` | String |  |
| `season` | Int64 |  |
| `league_name` | String |  |
| `league_full_name` | String | Full league name (e.g., "NCAA Football"). |
| `sport_name` | String |  |
| `alias_url` | String | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `away_score` | Int64 |  |
| `basic_away_team_abbreviation` | String | Short abbreviation for the away team used in compact displays, as carried on the boxscore's lightweight team node. |
| `basic_away_team_display_name` | String | Display name of the away team as shown on the scoreboard, as carried on the boxscore's lightweight team node. |
| `basic_away_team_team_id` | String | Yahoo composite team id of the away team (e.g., "ncaaf.t.29"), as carried on the boxscore's lightweight team node. |
| `basic_away_team_team_logo_white` | String | JSON-encoded image node for the away team's white knockout logo, used on dark backgrounds, as carried on the boxscore's lightweight team node. |
| `basic_away_team_team_logo` | String | JSON-encoded image node for the away team's standard logo, as carried on the boxscore's lightweight team node. |
| `basic_away_team_playoff_seeds` | String | JSON-encoded list of the away team's playoff-seed entries for the season, as carried on the boxscore's lightweight team node. |
| `basic_away_team_nickname` | String | Nickname or mascot of the away team, as carried on the boxscore's lightweight team node. |
| `basic_away_team_primary_color` | String | Primary brand color of the away team, as a hex RGB string without the leading hash, as carried on the boxscore's lightweight team node. |
| `basic_away_team_secondary_color` | String | Secondary brand color of the away team, as a hex RGB string without the leading hash, as carried on the boxscore's lightweight team node. |
| `away_team_full_name` | String |  |
| `away_team_team_id` | String | Yahoo composite team id of the away team (e.g., "ncaaf.t.29"). |
| `away_team_primary_color` | String | Primary brand color of the away team, as a hex RGB string without the leading hash. |
| `away_team_secondary_color` | String | Secondary brand color of the away team, as a hex RGB string without the leading hash. |
| `away_team_display_name` | String |  |
| `away_team_abbreviation` | String |  |
| `away_team_location` | String |  |
| `away_team_alias` | String | JSON-encoded Yahoo alias object for the away team, carrying its site URL and path. |
| `away_team_nickname` | String |  |
| `away_team_team_logo_white` | String | JSON-encoded image node for the away team's white knockout logo, used on dark backgrounds. |
| `away_team_team_logo` | String | JSON-encoded image node for the away team's standard logo. |
| `away_team_team_logo_white_large` | String | JSON-encoded image node for the large-format white knockout away-team logo. |
| `away_team_team_logo_large` | String | JSON-encoded image node for the large-format away-team logo. |
| `away_team_team_standings` | String | JSON-encoded standings node for the away team, carrying its record, position and streak. |
| `away_team_rank_polls` | String | JSON-encoded list of the poll rankings the away team currently holds. |
| `away_team_playoff_seeds` | String | JSON-encoded list of the away team's playoff-seed entries for the season. |
| `away_team_record` | String |  |
| `away_team_id` | String |  |
| `bets` | String | JSON-encoded list of the betting markets offered on the event (spread, moneyline and total). |
| `partial_game_bets` | String | JSON-encoded list of in-game betting markets covering only part of the game, such as halves or quarters. |
| `brief` | String | Short editorial blurb summarizing the game's state or result. |
| `broadcast_channels` | String | JSON-encoded list of the channels broadcasting the event. |
| `home_score` | Int64 |  |
| `home_team_full_name` | String |  |
| `home_team_team_id` | String | Yahoo composite team id of the home team (e.g., "ncaaf.t.29"). |
| `home_team_primary_color` | String | Primary brand color of the home team, as a hex RGB string without the leading hash. |
| `home_team_secondary_color` | String | Secondary brand color of the home team, as a hex RGB string without the leading hash. |
| `home_team_display_name` | String |  |
| `home_team_abbreviation` | String |  |
| `home_team_location` | String |  |
| `home_team_alias` | String | JSON-encoded Yahoo alias object for the home team, carrying its site URL and path. |
| `home_team_nickname` | String |  |
| `home_team_team_logo_white` | String | JSON-encoded image node for the home team's white knockout logo, used on dark backgrounds. |
| `home_team_team_logo` | String | JSON-encoded image node for the home team's standard logo. |
| `home_team_team_logo_white_large` | String | JSON-encoded image node for the large-format white knockout home-team logo. |
| `home_team_team_logo_large` | String | JSON-encoded image node for the large-format home-team logo. |
| `home_team_team_standings` | String | JSON-encoded standings node for the home team, carrying its record, position and streak. |
| `home_team_rank_polls` | String | JSON-encoded list of the poll rankings the home team currently holds. |
| `home_team_playoff_seeds` | String | JSON-encoded list of the home team's playoff-seed entries for the season. |
| `home_team_record` | String |  |
| `home_team_id` | String |  |
| `basic_home_team_abbreviation` | String | Short abbreviation for the home team used in compact displays, as carried on the boxscore's lightweight team node. |
| `basic_home_team_display_name` | String | Display name of the home team as shown on the scoreboard, as carried on the boxscore's lightweight team node. |
| `basic_home_team_team_id` | String | Yahoo composite team id of the home team (e.g., "ncaaf.t.29"), as carried on the boxscore's lightweight team node. |
| `basic_home_team_team_logo_white` | String | JSON-encoded image node for the home team's white knockout logo, used on dark backgrounds, as carried on the boxscore's lightweight team node. |
| `basic_home_team_team_logo` | String | JSON-encoded image node for the home team's standard logo, as carried on the boxscore's lightweight team node. |
| `basic_home_team_playoff_seeds` | String | JSON-encoded list of the home team's playoff-seed entries for the season, as carried on the boxscore's lightweight team node. |
| `basic_home_team_nickname` | String | Nickname or mascot of the home team, as carried on the boxscore's lightweight team node. |
| `basic_home_team_primary_color` | String | Primary brand color of the home team, as a hex RGB string without the leading hash, as carried on the boxscore's lightweight team node. |
| `basic_home_team_secondary_color` | String | Secondary brand color of the home team, as a hex RGB string without the leading hash, as carried on the boxscore's lightweight team node. |
| `start_date` | String |  |
| `start_time` | String |  |
| `status` | String |  |
| `status_display_name` | String | Short game or event status as shown on the scoreboard (e.g., "Final", "12:00 pm ET"). |
| `full_status_display_name` | String | Long-form game status label including overtime and date context (e.g., "Final/OT"). |
| `time_left` | String |  |
| `venue_display_name` | String | Name of the venue hosting the event. |
| `venue_city` | String |  |
| `venue_cover_type` | String | Whether the venue is open-air, domed or fitted with a retractable roof. |
| `venue_state` | String |  |
| `venue_venue_id` | String | Yahoo identifier of the venue hosting the event. |
| `venue_country` | String | Country the venue is located in. |
| `weather` | String |  |
| `game_win_probability_time_line_win_probability_timeline` | String | JSON-encoded series of win-probability observations across the course of the game. |
| `playoff_series` | String | JSON-encoded playoff-series node the game belongs to. |
| `regular_season_series_games` | String | JSON-encoded list of the games making up the regular-season series between the two teams. |
| `is_halftime` | Boolean | Flag indicating that the game is currently stopped at halftime. |
| `regulation_periods` | String |  |
| `current_period_short_display_name` | String | Abbreviated label for the period in progress (e.g., "4th"). |
| `current_period_period` | String | Ordinal number of the period currently in progress within the game. |
| `current_period_display_name` | String | Full label for the period in progress (e.g., "4th Quarter"). |
| `current_period_overtime` | String | Flag indicating that the period in progress is an overtime period. |
| `away_team_lineup` | String | JSON-encoded starting lineup fielded by the away team. |
| `home_team_lineup` | String | JSON-encoded starting lineup fielded by the home team. |
| `winning_team_id` | String | Composite Yahoo team id of the side that won the game (e.g., "ncaaf.t.29"). |
| `game_coverage` | String | JSON-encoded node describing which live feeds Yahoo carries for the game. |
| `away_line_score` | String | JSON-encoded per-period scoring line for the away team. |
| `away_timeouts_remaining` | Int64 |  |
| `down` | String |  |
| `distance` | String |  |
| `field_position_display_name` | String | Ball spot rendered the way a scoreboard shows it (e.g., "MICH 35"). |
| `field_position` | String | Ball spot expressed on Yahoo's 0-100 field scale, measured toward the offense's target goal line. |
| `home_line_score` | String | JSON-encoded per-period scoring line for the home team. |
| `home_timeouts_remaining` | Int64 |  |
| `team_possessing_ball` | String | Yahoo team id of the side currently possessing the ball. |
| `drives` | String | JSON-encoded data-island pointer to the game's drive collection; football only. |
| `first_play` | String | JSON-encoded first play of the game or of the current period. |
| `play_by_play` | String | JSON-encoded data-island pointer to the game's play-by-play collection in the same editorial payload. |
| `timeouts_granted` | Int64 | Number of timeouts granted so far in the current period. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_playbook_boxscore_poll-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_playbook_boxscore_poll-example}

```python
yahoo_playbook_boxscore_poll()
```

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
| `game_id` | String |  |
| `away_team_active` | String |  |
| `away_team_primary_color` | String | Primary brand color of the away team, as a hex RGB string without the leading hash. |
| `away_team_secondary_color` | String | Secondary brand color of the away team, as a hex RGB string without the leading hash. |
| `away_team_team_logo_white` | String | JSON-encoded image node for the away team's white knockout logo, used on dark backgrounds. |
| `away_team_team_logo` | String | JSON-encoded image node for the away team's standard logo. |
| `home_team_active` | String |  |
| `home_team_primary_color` | String | Primary brand color of the home team, as a hex RGB string without the leading hash. |
| `home_team_secondary_color` | String | Secondary brand color of the home team, as a hex RGB string without the leading hash. |
| `home_team_team_logo_white` | String | JSON-encoded image node for the home team's white knockout logo, used on dark backgrounds. |
| `home_team_team_logo` | String | JSON-encoded image node for the home team's standard logo. |
| `league_league_logo` | String | JSON-encoded image node for the league's logo. |
| `start_time` | String |  |
| `start_date` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_playbook_boxscore_social_share-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_playbook_boxscore_social_share-example}

```python
yahoo_playbook_boxscore_social_share()
```

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

### Raises {#yahoo_playbook_combat_match-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_playbook_combat_match-example}

```python
yahoo_playbook_combat_match()
```

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
| `game_id` | String |  |
| `display_name` | String |  |
| `league_name` | String |  |
| `league_full_name` | String | Full league name (e.g., "NCAA Football"). |
| `league_display_short` | String | Short league label used in navigation and compact UI (e.g., "NCAA FB"). |
| `league_short_name` | String |  |
| `league_sport` | String | Sport the league belongs to (e.g., "football"). |
| `league_alias` | String | JSON-encoded Yahoo alias object for the league, carrying its site URL and path. |
| `alias_url` | String | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `away_team_id` | String |  |
| `away_team_full_name` | String |  |
| `away_team_team_id` | String | Yahoo composite team id of the away team (e.g., "ncaaf.t.29"). |
| `away_team_primary_color` | String | Primary brand color of the away team, as a hex RGB string without the leading hash. |
| `away_team_secondary_color` | String | Secondary brand color of the away team, as a hex RGB string without the leading hash. |
| `away_team_display_name` | String |  |
| `away_team_abbreviation` | String |  |
| `away_team_location` | String |  |
| `away_team_alias` | String | JSON-encoded Yahoo alias object for the away team, carrying its site URL and path. |
| `away_team_nickname` | String |  |
| `away_team_last_games` | String | JSON-encoded list of the away team's most recently completed games. |
| `away_team_team_logo_white` | String | JSON-encoded image node for the away team's white knockout logo, used on dark backgrounds. |
| `away_team_team_logo` | String | JSON-encoded image node for the away team's standard logo. |
| `away_team_team_standings` | String | JSON-encoded standings node for the away team, carrying its record, position and streak. |
| `away_team_players` | String | JSON-encoded roster of away-team players attached to the game. |
| `away_team_rank_polls` | String | JSON-encoded list of the poll rankings the away team currently holds. |
| `away_team_playoff_seeds` | String | JSON-encoded list of the away team's playoff-seed entries for the season. |
| `home_team_id` | String |  |
| `home_team_full_name` | String |  |
| `home_team_team_id` | String | Yahoo composite team id of the home team (e.g., "ncaaf.t.29"). |
| `home_team_primary_color` | String | Primary brand color of the home team, as a hex RGB string without the leading hash. |
| `home_team_secondary_color` | String | Secondary brand color of the home team, as a hex RGB string without the leading hash. |
| `home_team_display_name` | String |  |
| `home_team_abbreviation` | String |  |
| `home_team_location` | String |  |
| `home_team_alias` | String | JSON-encoded Yahoo alias object for the home team, carrying its site URL and path. |
| `home_team_nickname` | String |  |
| `home_team_last_games` | String | JSON-encoded list of the home team's most recently completed games. |
| `home_team_team_logo_white` | String | JSON-encoded image node for the home team's white knockout logo, used on dark backgrounds. |
| `home_team_team_logo` | String | JSON-encoded image node for the home team's standard logo. |
| `home_team_team_standings` | String | JSON-encoded standings node for the home team, carrying its record, position and streak. |
| `home_team_players` | String | JSON-encoded roster of home-team players attached to the game. |
| `home_team_rank_polls` | String | JSON-encoded list of the poll rankings the home team currently holds. |
| `home_team_playoff_seeds` | String | JSON-encoded list of the home team's playoff-seed entries for the season. |
| `away_score` | Int64 |  |
| `home_score` | Int64 |  |
| `start_time` | String |  |
| `start_date` | String |  |
| `if_necessary` | String |  |
| `status` | String |  |
| `status_display_name` | String | Short game or event status as shown on the scoreboard (e.g., "Final", "12:00 pm ET"). |
| `season` | Int64 |  |
| `season_phase` | String | Phase of the season the game falls in (e.g., "season.phase.season"). |
| `time_left` | String |  |
| `tournament_id` | String |  |
| `playoff_series` | String | JSON-encoded playoff-series node the game belongs to. |
| `winning_team_id` | String | Composite Yahoo team id of the side that won the game (e.g., "ncaaf.t.29"). |
| `broadcast_channels` | String | JSON-encoded list of the channels broadcasting the event. |
| `news_break_subtext` | String | Secondary line of the news-break banner attached to the game. |
| `news_break_title` | String | Headline of the news-break banner attached to the game. |
| `news_break_url` | String | URL of the article behind the game's news-break banner. |
| `news_break_uuid` | String | Yahoo content UUID of the article behind the game's news-break banner. |
| `brief` | String | Short editorial blurb summarizing the game's state or result. |
| `bets` | String | JSON-encoded list of the betting markets offered on the event (spread, moneyline and total). |
| `venue_display_name` | String | Name of the venue hosting the event. |
| `venue_city` | String |  |
| `venue_cover_type` | String | Whether the venue is open-air, domed or fitted with a retractable roof. |
| `venue_state` | String |  |
| `venue_venue_id` | String | Yahoo identifier of the venue hosting the event. |
| `venue_country` | String | Country the venue is located in. |
| `tv_coverage` | String | Network carrying the game, as a short broadcast abbreviation (e.g., "CBS", "ESPN"). |
| `weather` | String |  |
| `away_line_score` | String | JSON-encoded per-period scoring line for the away team. |
| `current_period_period` | String | Ordinal number of the period currently in progress within the game. |
| `field_position` | String | Ball spot expressed on Yahoo's 0-100 field scale, measured toward the offense's target goal line. |
| `field_position_display_name` | String | Ball spot rendered the way a scoreboard shows it (e.g., "MICH 35"). |
| `home_line_score` | String | JSON-encoded per-period scoring line for the home team. |
| `home_timeouts_remaining` | Int64 |  |
| `away_timeouts_remaining` | Int64 |  |
| `last_play` | String |  |
| `game_stat_leaders` | String | JSON-encoded pointer to the per-category statistical leaders for the game. |
| `team_possessing_ball` | String | Yahoo team id of the side currently possessing the ball. |
| `recap_videos` | String | JSON-encoded list of recap videos published for the game. |
| `week` | Int64 |  |
| `play_by_play` | String | JSON-encoded data-island pointer to the game's play-by-play collection in the same editorial payload. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_playbook_game-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_playbook_game-example}

```python
yahoo_playbook_game()
```

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
| `game_id` | String |  |
| `bets` | String | JSON-encoded list of the betting markets offered on the event (spread, moneyline and total). |
| `partial_game_bets` | String | JSON-encoded list of in-game betting markets covering only part of the game, such as halves or quarters. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_playbook_game_odds_poll-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_playbook_game_odds_poll-example}

```python
yahoo_playbook_game_odds_poll()
```

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

### Raises {#yahoo_playbook_golf_tournament-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_playbook_golf_tournament-example}

```python
yahoo_playbook_golf_tournament()
```

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
| `ncaaf_games` | String | JSON-encoded list of NCAAF game nodes carrying the pick or odds distribution for the slate. |
| `conferences` | String | JSON-encoded list of the league's conference nodes, each carrying an id, a name and its member teams. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_playbook_league_odds-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_playbook_league_odds-example}

```python
yahoo_playbook_league_odds()
```

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
| `player_id` | String |  |
| `alias_url` | String | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `alias_subpages` | String | JSON-encoded list of subpage aliases (roster, schedule, stats) available beneath the entity's Yahoo page. |
| `sport_name` | String |  |
| `first_name` | String |  |
| `last_name` | String |  |
| `display_name` | String |  |
| `college` | String |  |
| `birth_state` | String |  |
| `birth_city` | String |  |
| `birth_country` | String |  |
| `birth_date` | String |  |
| `height` | Int64 |  |
| `display_height` | String |  |
| `weight` | Int64 |  |
| `status` | String |  |
| `active` | Boolean |  |
| `suggested_headshot` | String | JSON-encoded image node for the headshot Yahoo recommends for this player. |
| `uniform_number` | String | Jersey number the player wears for the team. |
| `positions` | String |  |
| `team_id` | String |  |
| `team_team_id` | String |  |
| `team_display_name` | String |  |
| `team_full_name` | String |  |
| `team_alias` | String | JSON-encoded alias object for the entity's team, carrying its Yahoo page URL and path. |
| `team_team_logo` | String | JSON-encoded image node for the team's standard logo. |
| `team_team_logo_white` | String | JSON-encoded image node for the team's white knockout logo. |
| `team_primary_color` | String | Primary brand color of the entity's team, as a hex RGB string without the leading hash. |
| `team_secondary_color` | String | Secondary brand color of the entity's team, as a hex RGB string without the leading hash. |
| `draft_position` | String | Round and pick at which the player was drafted, left null for undrafted players. |
| `player_seasons` | String | JSON-encoded list of the seasons for which Yahoo carries data on this player. |
| `header_stats_passing` | String | JSON-encoded headline passing statistics shown at the top of the player's page. |
| `season_stats_passing` | String | JSON-encoded full-season passing statistics for the player. |
| `header_stats_rushing` | String | JSON-encoded headline rushing statistics shown at the top of the player's page. |
| `season_stats_rushing` | String | JSON-encoded full-season rushing statistics for the player. |
| `header_stats_receiving` | String | JSON-encoded headline receiving statistics shown at the top of the player's page. |
| `season_stats_receiving` | String | JSON-encoded full-season receiving statistics for the player. |
| `header_stats_defense` | String | JSON-encoded headline defensive statistics shown at the top of the player's page. |
| `season_stats_defense` | String | JSON-encoded full-season defensive statistics for the player. |
| `header_stats_kicking` | String | JSON-encoded headline kicking statistics shown at the top of the player's page. |
| `season_stats_kicking` | String | JSON-encoded full-season kicking statistics for the player. |
| `header_stats_punting` | String | JSON-encoded headline punting statistics shown at the top of the player's page. |
| `season_stats_punting` | String | JSON-encoded full-season punting statistics for the player. |
| `earnings` | String | Prize money the player has earned over the covered period, in US dollars. |
| `first_year` | String | First season in which the player appeared in this league. |
| `last_year` | Int64 | Most recent season in which the player appeared in this league. |
| `injury` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_playbook_player-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_playbook_player-example}

```python
yahoo_playbook_player()
```

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
| `sport_name` | String |  |
| `short_display_name` | String |  |
| `suggested_headshot` | String | JSON-encoded image node for the headshot Yahoo recommends for this player. |
| `team_primary_color` | String | Primary brand color of the entity's team, as a hex RGB string without the leading hash. |
| `team_secondary_color` | String | Secondary brand color of the entity's team, as a hex RGB string without the leading hash. |
| `team_league` | String | JSON-encoded league node for the entity's team. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_playbook_player_social_share-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_playbook_player_social_share-example}

```python
yahoo_playbook_player_social_share()
```

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

### Raises {#yahoo_playbook_race-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_playbook_race-example}

```python
yahoo_playbook_race()
```

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
| `sport_sport_id` | String | Yahoo identifier of the sport the league belongs to. |
| `sport_name` | String |  |
| `league_name` | String |  |
| `league_short_name` | String |  |
| `league_current_season_phase` | String | Phase the league's season is currently in (e.g., "season.phase.season"). |
| `team_id` | String |  |
| `conference_id` | Int64 |  |
| `full_name` | String |  |
| `display_name` | String |  |
| `location` | String |  |
| `nickname` | String |  |
| `primary_color` | String |  |
| `secondary_color` | String |  |
| `team_logo_white_url` | String | Absolute URL of the team's white knockout logo, the variant used on dark backgrounds. |
| `team_logo_url` | String | Absolute URL of the team's standard logo image on Yahoo's image CDN. |
| `alias_navigation_links` | String | JSON-encoded map of navigation links (scores, standings, teams) hanging off the entity's Yahoo alias. |
| `alias_url` | String | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `alias_path` | String | Site-relative path portion of the entity's Yahoo alias (e.g., "/ncaaf/teams/tcu/"). |
| `last_games` | String | JSON-encoded list of the team's most recently completed games, used for form and streak displays. |
| `next_games` | String | JSON-encoded list of the team's next scheduled games. |
| `division_name` | String |  |
| `division_teams` | String | JSON-encoded list of the teams that make up the division. |
| `conference_short_name` | String |  |
| `conference_name` | String |  |
| `conference_conference_id` | String | Yahoo numeric identifier of the conference carried on the team's conference node. |
| `conference_team_standings` | String | JSON-encoded standings rows for every team in the conference. |
| `conference_abbreviation` | String |  |
| `team_standings_team` | String | JSON-encoded team node the standings row describes. |
| `team_standings_conference_id` | String | Yahoo numeric conference id for the team's standings row. |
| `team_standings_conference` | String | JSON-encoded conference node the standings row sits under. |
| `team_standings_display_name` | String | Display name of the team on its standings row. |
| `team_standings_full_name` | String | Full name of the team on its standings row. |
| `team_standings_position` | String | Rank of the team within the standings grouping it is listed in. |
| `team_standings_sequence` | String | Tie-break ordering value Yahoo uses to sequence teams holding identical records. |
| `team_standings_team_record` | String | Formatted overall record for the team (e.g., "8-2"). |
| `team_standings_conference_position` | String | Rank of the team within its conference standings. |
| `team_standings_points_for` | String | Points the team has scored over the standings period. |
| `team_standings_points_against` | String | Points the team has allowed over the standings period. |
| `team_standings_clinched_playoff` | String | Flag indicating that the team has clinched a playoff berth. |
| `team_standings_clinched_division` | String | Flag indicating that the team has clinched its division. |
| `team_standings_streak_display` | String | Formatted current streak for the team (e.g., "W3", "L2"). |
| `gametime_ticket_url` | String | Gametime affiliate ticket-purchase URL for the event or team. |
| `football_team_season_stats` | String | JSON-encoded team-level season statistics for the team's football side. |
| `football_player_season_stats` | String | JSON-encoded per-player season statistics for the team's football roster. |
| `injured_players` | String | JSON-encoded list of the team's players currently carrying an injury designation. |
| `transactions` | String | JSON-encoded list of the team's roster transactions over the requested window. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_playbook_team-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_playbook_team-example}

```python
yahoo_playbook_team()
```

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
| `sport_sport_id` | String | Yahoo identifier of the sport the league belongs to. |
| `sport_name` | String |  |
| `league_name` | String |  |
| `league_short_name` | String |  |
| `league_current_season_phase` | String | Phase the league's season is currently in (e.g., "season.phase.season"). |
| `team_id` | String |  |
| `conference_id` | Int64 |  |
| `full_name` | String |  |
| `display_name` | String |  |
| `location` | String |  |
| `nickname` | String |  |
| `primary_color` | String |  |
| `secondary_color` | String |  |
| `team_logo_white_url` | String | Absolute URL of the team's white knockout logo, the variant used on dark backgrounds. |
| `team_logo_url` | String | Absolute URL of the team's standard logo image on Yahoo's image CDN. |
| `alias_navigation_links` | String | JSON-encoded map of navigation links (scores, standings, teams) hanging off the entity's Yahoo alias. |
| `alias_url` | String | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `alias_path` | String | Site-relative path portion of the entity's Yahoo alias (e.g., "/ncaaf/teams/tcu/"). |
| `last_games` | String | JSON-encoded list of the team's most recently completed games, used for form and streak displays. |
| `next_games` | String | JSON-encoded list of the team's next scheduled games. |
| `division_name` | String |  |
| `conference_short_name` | String |  |
| `conference_name` | String |  |
| `conference_conference_id` | String | Yahoo numeric identifier of the conference carried on the team's conference node. |
| `conference_abbreviation` | String |  |
| `team_standings_team` | String | JSON-encoded team node the standings row describes. |
| `team_standings_conference_id` | String | Yahoo numeric conference id for the team's standings row. |
| `team_standings_conference` | String | JSON-encoded conference node the standings row sits under. |
| `team_standings_display_name` | String | Display name of the team on its standings row. |
| `team_standings_full_name` | String | Full name of the team on its standings row. |
| `team_standings_position` | String | Rank of the team within the standings grouping it is listed in. |
| `team_standings_sequence` | String | Tie-break ordering value Yahoo uses to sequence teams holding identical records. |
| `team_standings_team_record` | String | Formatted overall record for the team (e.g., "8-2"). |
| `team_standings_conference_position` | String | Rank of the team within its conference standings. |
| `team_standings_points_for` | String | Points the team has scored over the standings period. |
| `team_standings_points_against` | String | Points the team has allowed over the standings period. |
| `team_standings_clinched_playoff` | String | Flag indicating that the team has clinched a playoff berth. |
| `team_standings_clinched_division` | String | Flag indicating that the team has clinched its division. |
| `team_standings_streak_display` | String | Formatted current streak for the team (e.g., "W3", "L2"). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_playbook_team_basic-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_playbook_team_basic-example}

```python
yahoo_playbook_team_basic()
```

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
| `sport_sport_id` | String | Yahoo identifier of the sport the league belongs to. |
| `sport_name` | String |  |
| `team_id` | String |  |
| `primary_color` | String |  |
| `team_logo_url` | String | Absolute URL of the team's standard logo image on Yahoo's image CDN. |
| `team_logo_white_url` | String | Absolute URL of the team's white knockout logo, the variant used on dark backgrounds. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_playbook_team_social_share-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_playbook_team_social_share-example}

```python
yahoo_playbook_team_social_share()
```

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

### Raises {#yahoo_playbook_tennis_match-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_playbook_tennis_match-example}

```python
yahoo_playbook_tennis_match()
```
