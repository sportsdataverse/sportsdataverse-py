---
title: "Yahoo Sports — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Other: scoreboard_game–trending_game"
sidebar_label: "Other: scoreboard_game–trending_game"
sidebar_position: 10
description: "Yahoo Sports — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Other: scoreboard_game–trending_game — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# Yahoo Sports — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Other: scoreboard_game–trending_game

## yahoo_scoreboard_game

Yahoo shangrila persisted query `scoreboardGame` -> one row per `games` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/scoreboardGame`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/scoreboardGame](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/scoreboardGame)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameId` | `game_id` |  |  | `Y` | gameId query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `seasonPhase` | `season_phase` |  |  | `Y` | seasonPhase query parameter. |
| `statLeaderCount` | `stat_leader_count` |  |  | `Y` | statLeaderCount query parameter. |
| `singleStatLeader` | `single_stat_leader` |  |  | `Y` | singleStatLeader query parameter. |
| `betEventState` | `bet_event_state` |  |  | `Y` | betEventState query parameter. |

### Returns {#yahoo_scoreboard_game-returns}

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
| `league_league_logo` | String | JSON-encoded image node for the league's logo. |
| `partner_url` | String | Partner or affiliate deep link associated with the scoreboard game. |
| `alias_url` | String | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `away_team_id` | String |  |
| `away_team_full_name` | String |  |
| `away_team_team_id` | String | Yahoo composite team id of the away team (e.g., "ncaaf.t.29"). |
| `away_team_display_name` | String |  |
| `away_team_abbreviation` | String |  |
| `away_team_alias` | String | JSON-encoded Yahoo alias object for the away team, carrying its site URL and path. |
| `away_team_nickname` | String |  |
| `away_team_team_logo_white` | String | JSON-encoded image node for the away team's white knockout logo, used on dark backgrounds. |
| `away_team_team_logo` | String | JSON-encoded image node for the away team's standard logo. |
| `away_team_team_standings` | String | JSON-encoded standings node for the away team, carrying its record, position and streak. |
| `away_team_rank_polls` | String | JSON-encoded list of the poll rankings the away team currently holds. |
| `away_team_playoff_seeds` | String | JSON-encoded list of the away team's playoff-seed entries for the season. |
| `away_team_record` | String |  |
| `home_team_id` | String |  |
| `home_team_full_name` | String |  |
| `home_team_team_id` | String | Yahoo composite team id of the home team (e.g., "ncaaf.t.29"). |
| `home_team_display_name` | String |  |
| `home_team_abbreviation` | String |  |
| `home_team_alias` | String | JSON-encoded Yahoo alias object for the home team, carrying its site URL and path. |
| `home_team_nickname` | String |  |
| `home_team_team_logo_white` | String | JSON-encoded image node for the home team's white knockout logo, used on dark backgrounds. |
| `home_team_team_logo` | String | JSON-encoded image node for the home team's standard logo. |
| `home_team_team_standings` | String | JSON-encoded standings node for the home team, carrying its record, position and streak. |
| `home_team_rank_polls` | String | JSON-encoded list of the poll rankings the home team currently holds. |
| `home_team_playoff_seeds` | String | JSON-encoded list of the home team's playoff-seed entries for the season. |
| `home_team_record` | String |  |
| `current_period_overtime` | String | Flag indicating that the period in progress is an overtime period. |
| `current_period_short_display_name` | String | Abbreviated label for the period in progress (e.g., "4th"). |
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
| `tournament_id` | String |  |
| `display_result` | String |  |
| `playoff_series` | String | JSON-encoded playoff-series node the game belongs to. |
| `winning_team_id` | String | Composite Yahoo team id of the side that won the game (e.g., "ncaaf.t.29"). |
| `broadcast_channels` | String | JSON-encoded list of the channels broadcasting the event. |
| `news_break_subtext` | String | Secondary line of the news-break banner attached to the game. |
| `news_break_title` | String | Headline of the news-break banner attached to the game. |
| `news_break_url` | String | URL of the article behind the game's news-break banner. |
| `news_break_uuid` | String | Yahoo content UUID of the article behind the game's news-break banner. |
| `news_break_type` | String | Category of the news-break banner, such as an injury note, preview or recap. |
| `brief` | String | Short editorial blurb summarizing the game's state or result. |
| `event_extended_display_name` | String | Long-form event title used for marquee games, such as a bowl or rivalry name. |
| `special_event_type` | String | Marker identifying a special framing for the game, such as a bowl game or neutral-site showcase. |
| `bets` | String | JSON-encoded list of the betting markets offered on the event (spread, moneyline and total). |
| `venue_display_name` | String | Name of the venue hosting the event. |
| `weather` | String |  |
| `gametime_ticket_url` | String | Gametime affiliate ticket-purchase URL for the event or team. |
| `game_ticket_price` | String | Lowest available ticket price for the game from the Gametime affiliate feed, in US dollars. |
| `teams` | String |  |
| `field_position` | String | Ball spot expressed on Yahoo's 0-100 field scale, measured toward the offense's target goal line. |
| `field_position_display_name` | String | Ball spot rendered the way a scoreboard shows it (e.g., "MICH 35"). |
| `team_possessing_ball` | String | Yahoo team id of the side currently possessing the ball. |
| `week` | Int64 |  |
| `passing_leader` | String | JSON-encoded leading passer for the game or team, with the statistics that earned the billing. |
| `rushing_leader` | String | JSON-encoded leading rusher for the game or team, with the statistics that earned the billing. |
| `receiving_leader` | String | JSON-encoded leading receiver for the game or team, with the statistics that earned the billing. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_scoreboard_game-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_scoreboard_game-example}

```python
yahoo_scoreboard_game()
```

## yahoo_tennis_matches_by_date

Yahoo shangrila persisted query `tennisMatchesByDate` -> one row per `tennisTournaments` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/tennisMatchesByDate`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/tennisMatchesByDate](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/tennisMatchesByDate)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `tournamentId` | `tournament_id` |  |  | `Y` | tournamentId query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `date` | `date` |  |  | `Y` | date query parameter. |

### Returns {#yahoo_tennis_matches_by_date-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `display_name` | String |  |
| `tournament_status` | String | State of the tournament, distinguishing scheduled, in-progress and completed events. |
| `start_time` | String |  |
| `end_time` | String |  |
| `events` | String |  |
| `champions` | String | JSON-encoded list of the current champions of the tennis event, one entry per draw. |
| `previous_champions` | String | JSON-encoded list of the champions of the previous edition of the tennis event. |
| `venue_country` | String | Country the venue is located in. |
| `venue_city` | String |  |
| `venue_state` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_tennis_matches_by_date-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_tennis_matches_by_date-example}

```python
yahoo_tennis_matches_by_date()
```

## yahoo_tennis_tournament

Yahoo shangrila persisted query `tennisTournament` -> one row per `tennisTournaments` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/tennisTournament`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/tennisTournament](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/tennisTournament)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `tournamentId` | `tournament_id` |  |  | `Y` | tournamentId query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#yahoo_tennis_tournament-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `display_name` | String |  |
| `tournament_status` | String | State of the tournament, distinguishing scheduled, in-progress and completed events. |
| `start_time` | String |  |
| `end_time` | String |  |
| `events` | String |  |
| `champions` | String | JSON-encoded list of the current champions of the tennis event, one entry per draw. |
| `previous_champions` | String | JSON-encoded list of the champions of the previous edition of the tennis event. |
| `venue_country` | String | Country the venue is located in. |
| `venue_city` | String |  |
| `venue_state` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_tennis_tournament-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_tennis_tournament-example}

```python
yahoo_tennis_tournament()
```

## yahoo_tennis_tournaments

Yahoo shangrila persisted query `tennisTournaments` -> one row per `tennisTournaments` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/tennisTournaments`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/tennisTournaments](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/tennisTournaments)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `leagueId` | `league_id` |  |  | `Y` | leagueId query parameter. |
| `matchType` | `match_type` |  |  | `Y` | matchType query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#yahoo_tennis_tournaments-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `gender` | String |  |
| `event_group_id` | String | Yahoo identifier that groups the rounds or legs making up a single tournament. |
| `display_name` | String |  |
| `match_type` | String | Format of the matches in the tennis draw (e.g., "SINGLES", "DOUBLES"). |
| `surface` | String |  |
| `start_time` | String |  |
| `end_time` | String |  |
| `tournament_status` | String | State of the tournament, distinguishing scheduled, in-progress and completed events. |
| `champions` | String | JSON-encoded list of the current champions of the tennis event, one entry per draw. |
| `alias_path` | String | Site-relative path portion of the entity's Yahoo alias (e.g., "/ncaaf/teams/tcu/"). |
| `alias_lang` | String | Language/locale tag attached to the entity's Yahoo alias (e.g., "en-US"). |
| `alias_url` | String | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `alias_domain` | String | Host the entity's Yahoo alias resolves against (e.g., "sports.yahoo.com"). |
| `previous_champions` | String | JSON-encoded list of the champions of the previous edition of the tennis event. |
| `venue_country` | String | Country the venue is located in. |
| `venue_city` | String |  |
| `venue_state` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_tennis_tournaments-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_tennis_tournaments-example}

```python
yahoo_tennis_tournaments()
```

## yahoo_tennis_tournaments_by_date

Yahoo shangrila persisted query `tennisTournamentsByDate` -> one row per `tennisTournaments` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/tennisTournamentsByDate`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/tennisTournamentsByDate](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/tennisTournamentsByDate)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `date` | `date` |  |  | `Y` | date query parameter. |

### Returns {#yahoo_tennis_tournaments_by_date-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_yahoo_shangrila`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_tennis_tournaments_by_date-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_tennis_tournaments_by_date-example}

```python
yahoo_tennis_tournaments_by_date()
```

## yahoo_trending_event_ids

Yahoo shangrila persisted query `trendingEventIds` -> one row per `trendingEvents` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/trendingEventIds`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/trendingEventIds](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/trendingEventIds)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `count` | `count` |  |  | `Y` | count query parameter. |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `dateFlipOffset` | `date_flip_offset` |  |  | `Y` | dateFlipOffset query parameter. |

### Returns {#yahoo_trending_event_ids-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_yahoo_shangrila`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_trending_event_ids-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_trending_event_ids-example}

```python
yahoo_trending_event_ids()
```

## yahoo_trending_game_ids

Yahoo shangrila persisted query `trendingGameIds` -> one row per `trendingGames` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/trendingGameIds`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/trendingGameIds](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/trendingGameIds)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `count` | `count` |  |  | `Y` | count query parameter. |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `dateFlipOffset` | `date_flip_offset` |  |  | `Y` | dateFlipOffset query parameter. |
| `dates` | `dates` |  |  | `Y` | Date or date range filter (YYYYMMDD or YYYYMMDD-YYYYMMDD). |

### Returns {#yahoo_trending_game_ids-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_yahoo_shangrila`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_trending_game_ids-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_trending_game_ids-example}

```python
yahoo_trending_game_ids()
```
