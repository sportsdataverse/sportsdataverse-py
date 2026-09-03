---
title: YAHOO — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com)
sidebar_label: Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com)
description: "YAHOO — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
---
# YAHOO — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com)

`sportsdataverse.yahoo` — 107 endpoints.

## `yahoo_oly_medal_count`

Yahoo shangrila persisted query `OlyMedalCount` -> one row per `olympics` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/OlyMedalCount`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/OlyMedalCount](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/OlyMedalCount)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `sortMethod` | `sort_method` |  |  | `Y` | sortMethod query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `display_name` | character | Display name. |
| `short_display_name` | character | Short display name. |
| `start_date` | character | Start date (YYYY-MM-DD). |
| `end_date` | character | End date (YYYY-MM-DD). |
| `season` | integer | Season year. |
| `alias` | character | JSON-encoded Yahoo alias object for the entity, carrying the site URL, path and subpage routing used to build links to its page. |
| `olympic_team` | character | JSON-encoded national team node whose medal count this row reports. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_oly_medal_count()
```

_Last validated n/a._

## `yahoo_oly_seasons`

Yahoo shangrila persisted query `OlySeasons` -> one row per `olympics` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/OlySeasons`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/OlySeasons](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/OlySeasons)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `seasons` | `seasons` |  |  | `Y` | seasons query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `season` | integer | Season year. |
| `display_name` | character | Display name. |
| `type` | character | Record type / category. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_oly_seasons()
```

_Last validated n/a._

## `yahoo_alias`

Yahoo shangrila persisted query `alias` -> one row per `pageMetaData` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/alias`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/alias](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/alias)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `alias` | `alias` |  |  | `Y` | alias query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `page_type` | character | Two word identifier separated by a dash identifying the type of fantasy ranking (best = bestball; dynasty; redraft) and what position it applies to |
| `league_short_name` | character | League short name. |
| `league_display_name` | character | Display name of the league as rendered on the aliased page. |
| `entity_type` | character | Kind of entity the alias record points at (e.g., "team", "player", "league"). |
| `subpage_translation` | character | Localized display label for the subpage this alias record describes. |
| `entity_alias` | character | Alias template Yahoo serves the entity's own page under. |
| `subpage_alias` | character | Alias template for the subpage this alias record describes. |
| `hotlist_data_desktop_space_id` | character | Content-space identifier for the desktop hotlist module rendered on the aliased page. |
| `hotlist_data_tablet_space_id` | character | Content-space identifier for the tablet hotlist module rendered on the aliased page. |
| `hotlist_data_mobile_space_id` | character | Content-space identifier for the mobile hotlist module rendered on the aliased page. |
| `entity_list_id_desktop_list_id` | character | Identifier of the curated desktop entity list rendered on the aliased page. |
| `entity_list_id_mobile_list_id` | character | Identifier of the curated mobile entity list rendered on the aliased page. |
| `entity_list_id_tablet_list_id` | character | Identifier of the curated tablet entity list rendered on the aliased page. |
| `game` | character | Game. |
| `match` | character | Alias template Yahoo serves the match page under for this league. |
| `race` | character | Alias template Yahoo serves the motorsport race page under for this league. |
| `league` | character | League slug. |
| `team` | character | Team-side label or team identifier. |
| `golf_tournament` | character | Alias template Yahoo serves the golf-tournament page under for this league. |
| `tennis_tournament` | character | Alias template Yahoo serves the tennis-tournament page under for this league. |
| `player` | character | Player name. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_alias()
```

_Last validated n/a._

## `yahoo_article_list_card_players`

Yahoo shangrila persisted query `articleListCardPlayers` -> one row per `players` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/articleListCardPlayers`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/articleListCardPlayers](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/articleListCardPlayers)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `playerIds` | `player_ids` |  |  | `Y` | playerIds query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `alias_path` | character | Site-relative path portion of the entity's Yahoo alias (e.g., "/ncaaf/teams/tcu/"). |
| `alias_lang` | character | Language/locale tag attached to the entity's Yahoo alias (e.g., "en-US"). |
| `alias_url` | character | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `alias_domain` | character | Host the entity's Yahoo alias resolves against (e.g., "sports.yahoo.com"). |
| `player_id` | character | Unique player identifier. |
| `display_name` | character | Display name. |
| `short_display_name` | character | Short display name. |
| `player_cutout` | character | JSON-encoded image node for the player's transparent cut-out portrait. |
| `team_alias` | character | JSON-encoded alias object for the entity's team, carrying its Yahoo page URL and path. |
| `team_display_name` | character | Full team display name. |
| `team_primary_color` | character | Primary brand color of the entity's team, as a hex RGB string without the leading hash. |
| `team_secondary_color` | character | Secondary brand color of the entity's team, as a hex RGB string without the leading hash. |
| `team_team_id` | character | Unique identifier for team team. |
| `team_team_logo_white` | character | JSON-encoded image node for the team's white knockout logo. |
| `team_team_logo` | character | JSON-encoded image node for the team's standard logo. |
| `team_gametime_ticket_url` | character | Gametime affiliate ticket-purchase URL for the team's home games. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_article_list_card_players()
```

_Last validated n/a._

## `yahoo_article_list_card_teams`

Yahoo shangrila persisted query `articleListCardTeams` -> one row per `teams` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/articleListCardTeams`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/articleListCardTeams](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/articleListCardTeams)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `teamIds` | `team_ids` |  |  | `Y` | teamIds query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `alias_path` | character | Site-relative path portion of the entity's Yahoo alias (e.g., "/ncaaf/teams/tcu/"). |
| `alias_lang` | character | Language/locale tag attached to the entity's Yahoo alias (e.g., "en-US"). |
| `alias_url` | character | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `alias_domain` | character | Host the entity's Yahoo alias resolves against (e.g., "sports.yahoo.com"). |
| `display_name` | character | Display name. |
| `nickname` | character | Team or athlete nickname. |
| `primary_color` | character | Primary team color (hex). |
| `secondary_color` | character | Secondary team color (hex). |
| `team_id` | character | Unique team identifier. |
| `team_logo_white_width` | character | Pixel width of the team's white knockout logo image. |
| `team_logo_white_last_updated` | character | Timestamp at which the team's white knockout logo asset was last refreshed. |
| `team_logo_white_image_type` | character | File format of the team's white knockout logo asset (e.g., "png"). |
| `team_logo_white_url` | character | Absolute URL of the team's white knockout logo, the variant used on dark backgrounds. |
| `team_logo_white_height` | character | Pixel height of the team's white knockout logo image. |
| `team_logo_white_team_id` | character | Yahoo composite team id the white knockout logo asset belongs to. |
| `team_logo_width` | character | Pixel width of the team's standard logo image. |
| `team_logo_last_updated` | character | Timestamp at which the team's standard logo asset was last refreshed. |
| `team_logo_image_type` | character | File format of the team's standard logo asset (e.g., "png"). |
| `team_logo_url` | character | Absolute URL of the team's standard logo image on Yahoo's image CDN. |
| `team_logo_height` | character | Pixel height of the team's standard logo image. |
| `team_logo_team_id` | character | Yahoo composite team id the standard logo asset belongs to. |
| `gametime_ticket_url` | character | Gametime affiliate ticket-purchase URL for the event or team. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_article_list_card_teams()
```

_Last validated n/a._

## `yahoo_basic_players`

Yahoo shangrila persisted query `basicPlayers` -> one row per `players` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/basicPlayers`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/basicPlayers](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/basicPlayers)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `players` | `players` |  |  | `Y` | players query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_id` | character | Unique player identifier. |
| `display_name` | character | Display name. |
| `suggested_headshot` | character | JSON-encoded image node for the headshot Yahoo recommends for this player. |
| `alias_url` | character | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_basic_players()
```

_Last validated n/a._

## `yahoo_betting_disclaimer`

Yahoo shangrila persisted query `bettingDisclaimer` -> one row per `bettingDisclaimers` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/bettingDisclaimer`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/bettingDisclaimer](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/bettingDisclaimer)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `bettingDisclaimerId` | `betting_disclaimer_id` |  |  | `Y` | bettingDisclaimerId query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `disclaimer_id` | character | Identifier of the responsible-gambling disclaimer block to render alongside the odds. |
| `text` | character | Text description of the play / record. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_betting_disclaimer()
```

_Last validated n/a._

## `yahoo_combat_event_fights`

Yahoo shangrila persisted query `combatEventFights` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/combatEventFights`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/combatEventFights](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/combatEventFights)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `eventGroupId` | `event_group_id` |  |  | `Y` | eventGroupId query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `league` | `league` |  |  | `Y` | league query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `short_name` | character | Short display name. |
| `full_name` | character | Player's full name. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_combat_event_fights()
```

_Last validated n/a._

## `yahoo_combat_schedule`

Yahoo shangrila persisted query `combatSchedule` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/combatSchedule`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/combatSchedule](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/combatSchedule)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `league` | `league` |  |  | `Y` | league query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `short_name` | character | Short display name. |
| `full_name` | character | Player's full name. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_combat_schedule()
```

_Last validated n/a._

## `yahoo_common_pills`

Yahoo shangrila persisted query `common/pills` (response body not captured; shape unknown)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/common/pills`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/common/pills](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/common/pills)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `addTeamLogos` | `add_team_logos` |  |  | `Y` | addTeamLogos query parameter. |
| `date` | `date` |  |  | `Y` | date query parameter. |
| `teamIds` | `team_ids` |  |  | `Y` | teamIds query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_yahoo_shangrila`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_common_pills()
```

_Last validated n/a._

## `yahoo_consensus_rankings_php`

Yahoo shangrila persisted query `consensus-rankings.php` (response body not captured; shape unknown)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/consensus-rankings.php`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/consensus-rankings.php](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/consensus-rankings.php)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  |  | `Y` | sport query parameter. |
| `position` | `position` |  |  | `Y` | position query parameter. |
| `filters` | `filters` |  |  | `Y` | filters query parameter. |
| `experts` | `experts` |  |  | `Y` | experts query parameter. |
| `scoring` | `scoring` |  |  | `Y` | scoring query parameter. |
| `type` | `type` |  |  | `Y` | type query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_yahoo_shangrila`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_consensus_rankings_php()
```

_Last validated n/a._

## `yahoo_draft`

Yahoo shangrila persisted query `draft` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/draft`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/draft](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/draft)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | league query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_yahoo_shangrila`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_draft()
```

_Last validated n/a._

## `yahoo_draft_prospects`

Yahoo shangrila persisted query `draftProspects` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/draftProspects`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/draftProspects](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/draftProspects)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | league query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `imageHeight` | `image_height` |  |  | `Y` | imageHeight query parameter. |
| `imageWidth` | `image_width` |  |  | `Y` | imageWidth query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_yahoo_shangrila`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_draft_prospects()
```

_Last validated n/a._

## `yahoo_driver_results`

Yahoo shangrila persisted query `driverResults` -> one row per `players` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/driverResults`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/driverResults](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/driverResults)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `playerId` | `player_id` |  |  | `Y` | playerId query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_yahoo_shangrila`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_driver_results()
```

_Last validated n/a._

## `yahoo_driver_splits`

Yahoo shangrila persisted query `driverSplits` -> one row per `players` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/driverSplits`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/driverSplits](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/driverSplits)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `playerId` | `player_id` |  |  | `Y` | playerId query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_yahoo_shangrila`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_driver_splits()
```

_Last validated n/a._

## `yahoo_featured_game_ids`

Yahoo shangrila persisted query `featuredGameIds` -> one row per `featuredGames` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/featuredGameIds`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/featuredGameIds](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/featuredGameIds)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_featured_game_ids()
```

_Last validated n/a._

## `yahoo_game_prop_bets`

Yahoo shangrila persisted query `gamePropBets` -> one row per `games` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/gamePropBets`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/gamePropBets](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/gamePropBets)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameId` | `game_id` |  |  | `Y` | gameId query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `alias_path` | character | Site-relative path portion of the entity's Yahoo alias (e.g., "/ncaaf/teams/tcu/"). |
| `alias_lang` | character | Language/locale tag attached to the entity's Yahoo alias (e.g., "en-US"). |
| `alias_url` | character | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `alias_domain` | character | Host the entity's Yahoo alias resolves against (e.g., "sports.yahoo.com"). |
| `game_id` | character | Unique game identifier. |
| `status` | character | Status label. |
| `away_team_team_id` | character | Yahoo composite team id of the away team (e.g., "ncaaf.t.29"). |
| `away_team_primary_color` | character | Primary brand color of the away team, as a hex RGB string without the leading hash. |
| `away_team_display_name` | character | Away team full display name; `team_detail = TRUE` only. |
| `home_team_team_id` | character | Yahoo composite team id of the home team (e.g., "ncaaf.t.29"). |
| `home_team_primary_color` | character | Primary brand color of the home team, as a hex RGB string without the leading hash. |
| `home_team_display_name` | character | Home team full display name; `team_detail = TRUE` only. |
| `active_prop_bets` | character | JSON-encoded list of the prop-bet markets currently open for the game. |
| `game_props` | character | JSON-encoded list of player and game prop markets offered on the game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_game_prop_bets()
```

_Last validated n/a._

## `yahoo_game_stats_leaders`

Yahoo shangrila persisted query `gameStatsLeaders` -> one row per `games` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/gameStatsLeaders`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/gameStatsLeaders](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/gameStatsLeaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameId` | `game_id` |  |  | `Y` | gameId query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `seasonPhases` | `season_phases` |  |  | `Y` | seasonPhases query parameter. |
| `qualified` | `qualified` |  |  | `Y` | qualified query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `isPregame` | `is_pregame` |  |  | `Y` | isPregame query parameter. |
| `teamImageHeight` | `team_image_height` |  |  | `Y` | teamImageHeight query parameter. |
| `teamImageWidth` | `team_image_width` |  |  | `Y` | teamImageWidth query parameter. |
| `playerImageHeight` | `player_image_height` |  |  | `Y` | playerImageHeight query parameter. |
| `playerImageWidth` | `player_image_width` |  |  | `Y` | playerImageWidth query parameter. |
| `baseballLeaderSortStat0` | `baseball_leader_sort_stat0` |  |  | `Y` | baseballLeaderSortStat0 query parameter. |
| `baseballLeaderSortStat1` | `baseball_leader_sort_stat1` |  |  | `Y` | baseballLeaderSortStat1 query parameter. |
| `baseballLeaderSortStat2` | `baseball_leader_sort_stat2` |  |  | `Y` | baseballLeaderSortStat2 query parameter. |
| `baseballLeaderSortStat3` | `baseball_leader_sort_stat3` |  |  | `Y` | baseballLeaderSortStat3 query parameter. |
| `baseballLeaderSortStat4` | `baseball_leader_sort_stat4` |  |  | `Y` | baseballLeaderSortStat4 query parameter. |
| `baseballLeaderStatIds0` | `baseball_leader_stat_ids0` |  |  | `Y` | baseballLeaderStatIds0 query parameter. |
| `baseballLeaderStatIds1` | `baseball_leader_stat_ids1` |  |  | `Y` | baseballLeaderStatIds1 query parameter. |
| `baseballLeaderStatIds2` | `baseball_leader_stat_ids2` |  |  | `Y` | baseballLeaderStatIds2 query parameter. |
| `baseballLeaderStatIds3` | `baseball_leader_stat_ids3` |  |  | `Y` | baseballLeaderStatIds3 query parameter. |
| `baseballLeaderStatIds4` | `baseball_leader_stat_ids4` |  |  | `Y` | baseballLeaderStatIds4 query parameter. |
| `baseballPlayerStatIds0` | `baseball_player_stat_ids0` |  |  | `Y` | baseballPlayerStatIds0 query parameter. |
| `baseballPlayerStatIds1` | `baseball_player_stat_ids1` |  |  | `Y` | baseballPlayerStatIds1 query parameter. |
| `baseballTeamSortStat0` | `baseball_team_sort_stat0` |  |  | `Y` | baseballTeamSortStat0 query parameter. |
| `baseballTeamSortStat1` | `baseball_team_sort_stat1` |  |  | `Y` | baseballTeamSortStat1 query parameter. |
| `baseballTeamSortStat2` | `baseball_team_sort_stat2` |  |  | `Y` | baseballTeamSortStat2 query parameter. |
| `baseballTeamSortStat3` | `baseball_team_sort_stat3` |  |  | `Y` | baseballTeamSortStat3 query parameter. |
| `baseballTeamSortStat4` | `baseball_team_sort_stat4` |  |  | `Y` | baseballTeamSortStat4 query parameter. |
| `baseballTeamSortStat5` | `baseball_team_sort_stat5` |  |  | `Y` | baseballTeamSortStat5 query parameter. |
| `baseballTeamSortStat6` | `baseball_team_sort_stat6` |  |  | `Y` | baseballTeamSortStat6 query parameter. |
| `baseballTeamSortStat7` | `baseball_team_sort_stat7` |  |  | `Y` | baseballTeamSortStat7 query parameter. |
| `baseballTeamSortStat8` | `baseball_team_sort_stat8` |  |  | `Y` | baseballTeamSortStat8 query parameter. |
| `baseballTeamSortStat9` | `baseball_team_sort_stat9` |  |  | `Y` | baseballTeamSortStat9 query parameter. |
| `baseballTeamSortStat10` | `baseball_team_sort_stat10` |  |  | `Y` | baseballTeamSortStat10 query parameter. |
| `baseballTeamSortStat11` | `baseball_team_sort_stat11` |  |  | `Y` | baseballTeamSortStat11 query parameter. |
| `baseballTeamStatIds0` | `baseball_team_stat_ids0` |  |  | `Y` | baseballTeamStatIds0 query parameter. |
| `baseballTeamStatIds1` | `baseball_team_stat_ids1` |  |  | `Y` | baseballTeamStatIds1 query parameter. |
| `baseballTeamStatIds2` | `baseball_team_stat_ids2` |  |  | `Y` | baseballTeamStatIds2 query parameter. |
| `baseballTeamStatIds3` | `baseball_team_stat_ids3` |  |  | `Y` | baseballTeamStatIds3 query parameter. |
| `baseballTeamStatIds4` | `baseball_team_stat_ids4` |  |  | `Y` | baseballTeamStatIds4 query parameter. |
| `baseballTeamStatIds5` | `baseball_team_stat_ids5` |  |  | `Y` | baseballTeamStatIds5 query parameter. |
| `baseballTeamStatIds6` | `baseball_team_stat_ids6` |  |  | `Y` | baseballTeamStatIds6 query parameter. |
| `baseballTeamStatIds7` | `baseball_team_stat_ids7` |  |  | `Y` | baseballTeamStatIds7 query parameter. |
| `baseballTeamStatIds8` | `baseball_team_stat_ids8` |  |  | `Y` | baseballTeamStatIds8 query parameter. |
| `baseballTeamStatIds9` | `baseball_team_stat_ids9` |  |  | `Y` | baseballTeamStatIds9 query parameter. |
| `baseballTeamStatIds10` | `baseball_team_stat_ids10` |  |  | `Y` | baseballTeamStatIds10 query parameter. |
| `baseballTeamStatIds11` | `baseball_team_stat_ids11` |  |  | `Y` | baseballTeamStatIds11 query parameter. |
| `basketballLeaderSortStat0` | `basketball_leader_sort_stat0` |  |  | `Y` | basketballLeaderSortStat0 query parameter. |
| `basketballLeaderSortStat1` | `basketball_leader_sort_stat1` |  |  | `Y` | basketballLeaderSortStat1 query parameter. |
| `basketballLeaderSortStat2` | `basketball_leader_sort_stat2` |  |  | `Y` | basketballLeaderSortStat2 query parameter. |
| `basketballLeaderSortStat3` | `basketball_leader_sort_stat3` |  |  | `Y` | basketballLeaderSortStat3 query parameter. |
| `basketballLeaderSortStat4` | `basketball_leader_sort_stat4` |  |  | `Y` | basketballLeaderSortStat4 query parameter. |
| `basketballLeaderStatIds0` | `basketball_leader_stat_ids0` |  |  | `Y` | basketballLeaderStatIds0 query parameter. |
| `basketballLeaderStatIds1` | `basketball_leader_stat_ids1` |  |  | `Y` | basketballLeaderStatIds1 query parameter. |
| `basketballLeaderStatIds2` | `basketball_leader_stat_ids2` |  |  | `Y` | basketballLeaderStatIds2 query parameter. |
| `basketballLeaderStatIds3` | `basketball_leader_stat_ids3` |  |  | `Y` | basketballLeaderStatIds3 query parameter. |
| `basketballLeaderStatIds4` | `basketball_leader_stat_ids4` |  |  | `Y` | basketballLeaderStatIds4 query parameter. |
| `basketballPlayerStatIds0` | `basketball_player_stat_ids0` |  |  | `Y` | basketballPlayerStatIds0 query parameter. |
| `basketballTeamSortStat0` | `basketball_team_sort_stat0` |  |  | `Y` | basketballTeamSortStat0 query parameter. |
| `basketballTeamSortStat1` | `basketball_team_sort_stat1` |  |  | `Y` | basketballTeamSortStat1 query parameter. |
| `basketballTeamSortStat2` | `basketball_team_sort_stat2` |  |  | `Y` | basketballTeamSortStat2 query parameter. |
| `basketballTeamSortStat3` | `basketball_team_sort_stat3` |  |  | `Y` | basketballTeamSortStat3 query parameter. |
| `basketballTeamSortStat4` | `basketball_team_sort_stat4` |  |  | `Y` | basketballTeamSortStat4 query parameter. |
| `basketballTeamSortStat5` | `basketball_team_sort_stat5` |  |  | `Y` | basketballTeamSortStat5 query parameter. |
| `basketballTeamSortStat6` | `basketball_team_sort_stat6` |  |  | `Y` | basketballTeamSortStat6 query parameter. |
| `basketballTeamSortStat7` | `basketball_team_sort_stat7` |  |  | `Y` | basketballTeamSortStat7 query parameter. |
| `basketballTeamSortStat8` | `basketball_team_sort_stat8` |  |  | `Y` | basketballTeamSortStat8 query parameter. |
| `basketballTeamSortStat9` | `basketball_team_sort_stat9` |  |  | `Y` | basketballTeamSortStat9 query parameter. |
| `basketballTeamStatIds0` | `basketball_team_stat_ids0` |  |  | `Y` | basketballTeamStatIds0 query parameter. |
| `basketballTeamStatIds1` | `basketball_team_stat_ids1` |  |  | `Y` | basketballTeamStatIds1 query parameter. |
| `basketballTeamStatIds2` | `basketball_team_stat_ids2` |  |  | `Y` | basketballTeamStatIds2 query parameter. |
| `basketballTeamStatIds3` | `basketball_team_stat_ids3` |  |  | `Y` | basketballTeamStatIds3 query parameter. |
| `basketballTeamStatIds4` | `basketball_team_stat_ids4` |  |  | `Y` | basketballTeamStatIds4 query parameter. |
| `basketballTeamStatIds5` | `basketball_team_stat_ids5` |  |  | `Y` | basketballTeamStatIds5 query parameter. |
| `basketballTeamStatIds6` | `basketball_team_stat_ids6` |  |  | `Y` | basketballTeamStatIds6 query parameter. |
| `basketballTeamStatIds7` | `basketball_team_stat_ids7` |  |  | `Y` | basketballTeamStatIds7 query parameter. |
| `basketballTeamStatIds8` | `basketball_team_stat_ids8` |  |  | `Y` | basketballTeamStatIds8 query parameter. |
| `basketballTeamStatIds9` | `basketball_team_stat_ids9` |  |  | `Y` | basketballTeamStatIds9 query parameter. |
| `footballLeaderSortStat0` | `football_leader_sort_stat0` |  |  | `Y` | footballLeaderSortStat0 query parameter. |
| `footballLeaderSortStat1` | `football_leader_sort_stat1` |  |  | `Y` | footballLeaderSortStat1 query parameter. |
| `footballLeaderSortStat2` | `football_leader_sort_stat2` |  |  | `Y` | footballLeaderSortStat2 query parameter. |
| `footballLeaderSortStat3` | `football_leader_sort_stat3` |  |  | `Y` | footballLeaderSortStat3 query parameter. |
| `footballLeaderStatIds0` | `football_leader_stat_ids0` |  |  | `Y` | footballLeaderStatIds0 query parameter. |
| `footballLeaderStatIds1` | `football_leader_stat_ids1` |  |  | `Y` | footballLeaderStatIds1 query parameter. |
| `footballLeaderStatIds2` | `football_leader_stat_ids2` |  |  | `Y` | footballLeaderStatIds2 query parameter. |
| `footballLeaderStatIds3` | `football_leader_stat_ids3` |  |  | `Y` | footballLeaderStatIds3 query parameter. |
| `footballPlayerStatIds0` | `football_player_stat_ids0` |  |  | `Y` | footballPlayerStatIds0 query parameter. |
| `footballPlayerStatIds1` | `football_player_stat_ids1` |  |  | `Y` | footballPlayerStatIds1 query parameter. |
| `footballPlayerStatIds2` | `football_player_stat_ids2` |  |  | `Y` | footballPlayerStatIds2 query parameter. |
| `footballPlayerStatIds3` | `football_player_stat_ids3` |  |  | `Y` | footballPlayerStatIds3 query parameter. |
| `footballPlayerStatIds4` | `football_player_stat_ids4` |  |  | `Y` | footballPlayerStatIds4 query parameter. |
| `footballPlayerStatIds5` | `football_player_stat_ids5` |  |  | `Y` | footballPlayerStatIds5 query parameter. |
| `footballPlayerStatIds6` | `football_player_stat_ids6` |  |  | `Y` | footballPlayerStatIds6 query parameter. |
| `footballPlayerStatIds7` | `football_player_stat_ids7` |  |  | `Y` | footballPlayerStatIds7 query parameter. |
| `footballTeamSortStat0` | `football_team_sort_stat0` |  |  | `Y` | footballTeamSortStat0 query parameter. |
| `footballTeamSortStat1` | `football_team_sort_stat1` |  |  | `Y` | footballTeamSortStat1 query parameter. |
| `footballTeamSortStat2` | `football_team_sort_stat2` |  |  | `Y` | footballTeamSortStat2 query parameter. |
| `footballTeamSortStat3` | `football_team_sort_stat3` |  |  | `Y` | footballTeamSortStat3 query parameter. |
| `footballTeamSortStat4` | `football_team_sort_stat4` |  |  | `Y` | footballTeamSortStat4 query parameter. |
| `footballTeamSortStat5` | `football_team_sort_stat5` |  |  | `Y` | footballTeamSortStat5 query parameter. |
| `footballTeamSortStat6` | `football_team_sort_stat6` |  |  | `Y` | footballTeamSortStat6 query parameter. |
| `footballTeamSortStat7` | `football_team_sort_stat7` |  |  | `Y` | footballTeamSortStat7 query parameter. |
| `footballTeamSortStat8` | `football_team_sort_stat8` |  |  | `Y` | footballTeamSortStat8 query parameter. |
| `footballTeamSortStat9` | `football_team_sort_stat9` |  |  | `Y` | footballTeamSortStat9 query parameter. |
| `footballTeamSortStat10` | `football_team_sort_stat10` |  |  | `Y` | footballTeamSortStat10 query parameter. |
| `footballTeamSortStat11` | `football_team_sort_stat11` |  |  | `Y` | footballTeamSortStat11 query parameter. |
| `footballTeamStatIds0` | `football_team_stat_ids0` |  |  | `Y` | footballTeamStatIds0 query parameter. |
| `footballTeamStatIds1` | `football_team_stat_ids1` |  |  | `Y` | footballTeamStatIds1 query parameter. |
| `footballTeamStatIds2` | `football_team_stat_ids2` |  |  | `Y` | footballTeamStatIds2 query parameter. |
| `footballTeamStatIds3` | `football_team_stat_ids3` |  |  | `Y` | footballTeamStatIds3 query parameter. |
| `footballTeamStatIds4` | `football_team_stat_ids4` |  |  | `Y` | footballTeamStatIds4 query parameter. |
| `footballTeamStatIds5` | `football_team_stat_ids5` |  |  | `Y` | footballTeamStatIds5 query parameter. |
| `footballTeamStatIds6` | `football_team_stat_ids6` |  |  | `Y` | footballTeamStatIds6 query parameter. |
| `footballTeamStatIds7` | `football_team_stat_ids7` |  |  | `Y` | footballTeamStatIds7 query parameter. |
| `footballTeamStatIds8` | `football_team_stat_ids8` |  |  | `Y` | footballTeamStatIds8 query parameter. |
| `footballTeamStatIds9` | `football_team_stat_ids9` |  |  | `Y` | footballTeamStatIds9 query parameter. |
| `footballTeamStatIds10` | `football_team_stat_ids10` |  |  | `Y` | footballTeamStatIds10 query parameter. |
| `footballTeamStatIds11` | `football_team_stat_ids11` |  |  | `Y` | footballTeamStatIds11 query parameter. |
| `hockeyLeaderSortStat0` | `hockey_leader_sort_stat0` |  |  | `Y` | hockeyLeaderSortStat0 query parameter. |
| `hockeyLeaderSortStat1` | `hockey_leader_sort_stat1` |  |  | `Y` | hockeyLeaderSortStat1 query parameter. |
| `hockeyLeaderSortStat2` | `hockey_leader_sort_stat2` |  |  | `Y` | hockeyLeaderSortStat2 query parameter. |
| `hockeyLeaderSortStat3` | `hockey_leader_sort_stat3` |  |  | `Y` | hockeyLeaderSortStat3 query parameter. |
| `hockeyLeaderStatIds0` | `hockey_leader_stat_ids0` |  |  | `Y` | hockeyLeaderStatIds0 query parameter. |
| `hockeyLeaderStatIds1` | `hockey_leader_stat_ids1` |  |  | `Y` | hockeyLeaderStatIds1 query parameter. |
| `hockeyLeaderStatIds2` | `hockey_leader_stat_ids2` |  |  | `Y` | hockeyLeaderStatIds2 query parameter. |
| `hockeyLeaderStatIds3` | `hockey_leader_stat_ids3` |  |  | `Y` | hockeyLeaderStatIds3 query parameter. |
| `hockeyPlayerStatIds0` | `hockey_player_stat_ids0` |  |  | `Y` | hockeyPlayerStatIds0 query parameter. |
| `hockeyPlayerStatIds1` | `hockey_player_stat_ids1` |  |  | `Y` | hockeyPlayerStatIds1 query parameter. |
| `hockeyPlayerStatIds2` | `hockey_player_stat_ids2` |  |  | `Y` | hockeyPlayerStatIds2 query parameter. |
| `hockeyTeamSortStat0` | `hockey_team_sort_stat0` |  |  | `Y` | hockeyTeamSortStat0 query parameter. |
| `hockeyTeamSortStat1` | `hockey_team_sort_stat1` |  |  | `Y` | hockeyTeamSortStat1 query parameter. |
| `hockeyTeamSortStat2` | `hockey_team_sort_stat2` |  |  | `Y` | hockeyTeamSortStat2 query parameter. |
| `hockeyTeamSortStat3` | `hockey_team_sort_stat3` |  |  | `Y` | hockeyTeamSortStat3 query parameter. |
| `hockeyTeamSortStat4` | `hockey_team_sort_stat4` |  |  | `Y` | hockeyTeamSortStat4 query parameter. |
| `hockeyTeamSortStat5` | `hockey_team_sort_stat5` |  |  | `Y` | hockeyTeamSortStat5 query parameter. |
| `hockeyTeamSortStat6` | `hockey_team_sort_stat6` |  |  | `Y` | hockeyTeamSortStat6 query parameter. |
| `hockeyTeamStatIds0` | `hockey_team_stat_ids0` |  |  | `Y` | hockeyTeamStatIds0 query parameter. |
| `hockeyTeamStatIds1` | `hockey_team_stat_ids1` |  |  | `Y` | hockeyTeamStatIds1 query parameter. |
| `hockeyTeamStatIds2` | `hockey_team_stat_ids2` |  |  | `Y` | hockeyTeamStatIds2 query parameter. |
| `hockeyTeamStatIds3` | `hockey_team_stat_ids3` |  |  | `Y` | hockeyTeamStatIds3 query parameter. |
| `hockeyTeamStatIds4` | `hockey_team_stat_ids4` |  |  | `Y` | hockeyTeamStatIds4 query parameter. |
| `hockeyTeamStatIds5` | `hockey_team_stat_ids5` |  |  | `Y` | hockeyTeamStatIds5 query parameter. |
| `hockeyTeamStatIds6` | `hockey_team_stat_ids6` |  |  | `Y` | hockeyTeamStatIds6 query parameter. |
| `soccerPlayerStatIds0` | `soccer_player_stat_ids0` |  |  | `Y` | soccerPlayerStatIds0 query parameter. |
| `soccerPlayerStatIds1` | `soccer_player_stat_ids1` |  |  | `Y` | soccerPlayerStatIds1 query parameter. |
| `soccerPlayerStatIds2` | `soccer_player_stat_ids2` |  |  | `Y` | soccerPlayerStatIds2 query parameter. |
| `soccerPlayerStatIds3` | `soccer_player_stat_ids3` |  |  | `Y` | soccerPlayerStatIds3 query parameter. |
| `soccerPlayerStatIds4` | `soccer_player_stat_ids4` |  |  | `Y` | soccerPlayerStatIds4 query parameter. |
| `soccerTeamSortStat0` | `soccer_team_sort_stat0` |  |  | `Y` | soccerTeamSortStat0 query parameter. |
| `soccerTeamSortStat1` | `soccer_team_sort_stat1` |  |  | `Y` | soccerTeamSortStat1 query parameter. |
| `soccerTeamSortStat2` | `soccer_team_sort_stat2` |  |  | `Y` | soccerTeamSortStat2 query parameter. |
| `soccerTeamSortStat3` | `soccer_team_sort_stat3` |  |  | `Y` | soccerTeamSortStat3 query parameter. |
| `soccerTeamSortStat4` | `soccer_team_sort_stat4` |  |  | `Y` | soccerTeamSortStat4 query parameter. |
| `soccerTeamSortStat5` | `soccer_team_sort_stat5` |  |  | `Y` | soccerTeamSortStat5 query parameter. |
| `soccerTeamStatIds0` | `soccer_team_stat_ids0` |  |  | `Y` | soccerTeamStatIds0 query parameter. |
| `soccerTeamStatIds1` | `soccer_team_stat_ids1` |  |  | `Y` | soccerTeamStatIds1 query parameter. |
| `soccerTeamStatIds2` | `soccer_team_stat_ids2` |  |  | `Y` | soccerTeamStatIds2 query parameter. |
| `soccerTeamStatIds3` | `soccer_team_stat_ids3` |  |  | `Y` | soccerTeamStatIds3 query parameter. |
| `soccerTeamStatIds4` | `soccer_team_stat_ids4` |  |  | `Y` | soccerTeamStatIds4 query parameter. |
| `soccerTeamStatIds5` | `soccer_team_stat_ids5` |  |  | `Y` | soccerTeamStatIds5 query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `status` | character | Status label. |
| `league_full_name` | character | Full league name (e.g., "NCAA Football"). |
| `league_football_team_season_stats0` | character | JSON-encoded league-wide team season-stat leader board occupying slot 0 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `league_football_team_season_stats1` | character | JSON-encoded league-wide team season-stat leader board occupying slot 1 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `league_football_team_season_stats2` | character | JSON-encoded league-wide team season-stat leader board occupying slot 2 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `league_football_team_season_stats3` | character | JSON-encoded league-wide team season-stat leader board occupying slot 3 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `league_football_team_season_stats4` | character | JSON-encoded league-wide team season-stat leader board occupying slot 4 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `league_football_team_season_stats5` | character | JSON-encoded league-wide team season-stat leader board occupying slot 5 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `league_football_team_season_stats6` | character | JSON-encoded league-wide team season-stat leader board occupying slot 6 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `league_football_team_season_stats7` | character | JSON-encoded league-wide team season-stat leader board occupying slot 7 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `league_football_team_season_stats8` | character | JSON-encoded league-wide team season-stat leader board occupying slot 8 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `league_football_team_season_stats9` | character | JSON-encoded league-wide team season-stat leader board occupying slot 9 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `league_football_team_season_stats10` | character | JSON-encoded league-wide team season-stat leader board occupying slot 10 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `league_football_team_season_stats11` | character | JSON-encoded league-wide team season-stat leader board occupying slot 11 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `game_leader_stats0` | character | JSON-encoded in-game statistical leader board occupying slot 0 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `game_leader_stats1` | character | JSON-encoded in-game statistical leader board occupying slot 1 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `game_leader_stats2` | character | JSON-encoded in-game statistical leader board occupying slot 2 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `game_leader_stats3` | character | JSON-encoded in-game statistical leader board occupying slot 3 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `away_team_game_stats0_stats` | character | JSON-encoded away-team game-stat block occupying slot 0 of that team's game-stats list; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `away_team_game_stats1_stats` | character | JSON-encoded away-team game-stat block occupying slot 1 of that team's game-stats list; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `home_team_game_stats0_stats` | character | JSON-encoded home-team game-stat block occupying slot 0 of that team's game-stats list; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `home_team_game_stats1_stats` | character | JSON-encoded home-team game-stat block occupying slot 1 of that team's game-stats list; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `home_team_lineup` | character | JSON-encoded starting lineup fielded by the home team. |
| `away_team_lineup` | character | JSON-encoded starting lineup fielded by the away team. |
| `away_team_id` | character | Unique identifier for the away team. |
| `away_team_full_name` | character | Full away team name (e.g. 'Las Vegas Aces'). |
| `away_team_team_id` | character | Yahoo composite team id of the away team (e.g., "ncaaf.t.29"). |
| `away_team_primary_color` | character | Primary brand color of the away team, as a hex RGB string without the leading hash. |
| `away_team_secondary_color` | character | Secondary brand color of the away team, as a hex RGB string without the leading hash. |
| `away_team_display_name` | character | Away team full display name; `team_detail = TRUE` only. |
| `away_team_abbreviation` | character | Away team abbreviation; `team_detail = TRUE` only. |
| `away_team_team_logo_white` | character | JSON-encoded image node for the away team's white knockout logo, used on dark backgrounds. |
| `away_team_team_logo` | character | JSON-encoded image node for the away team's standard logo. |
| `away_team_league` | character | JSON-encoded league node identifying the league the away team plays in. |
| `away_team_season_leader_stats0` | character | JSON-encoded away-team season leader board occupying slot 0 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `away_team_season_leader_stats1` | character | JSON-encoded away-team season leader board occupying slot 1 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `away_team_season_leader_stats2` | character | JSON-encoded away-team season leader board occupying slot 2 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `away_team_season_leader_stats3` | character | JSON-encoded away-team season leader board occupying slot 3 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `away_team_player_season_stats0` | character | JSON-encoded away-team player season-stat block occupying slot 0 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `away_team_player_season_stats1` | character | JSON-encoded away-team player season-stat block occupying slot 1 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `away_team_player_season_stats2` | character | JSON-encoded away-team player season-stat block occupying slot 2 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `away_team_player_season_stats3` | character | JSON-encoded away-team player season-stat block occupying slot 3 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `away_team_player_season_stats4` | character | JSON-encoded away-team player season-stat block occupying slot 4 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `away_team_player_season_stats5` | character | JSON-encoded away-team player season-stat block occupying slot 5 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `away_team_player_season_stats6` | character | JSON-encoded away-team player season-stat block occupying slot 6 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `away_team_player_season_stats7` | character | JSON-encoded away-team player season-stat block occupying slot 7 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `home_team_id` | character | Unique identifier for the home team. |
| `home_team_full_name` | character | Full home team name (e.g. 'Las Vegas Aces'). |
| `home_team_team_id` | character | Yahoo composite team id of the home team (e.g., "ncaaf.t.29"). |
| `home_team_primary_color` | character | Primary brand color of the home team, as a hex RGB string without the leading hash. |
| `home_team_secondary_color` | character | Secondary brand color of the home team, as a hex RGB string without the leading hash. |
| `home_team_display_name` | character | Home team full display name; `team_detail = TRUE` only. |
| `home_team_abbreviation` | character | Home team abbreviation; `team_detail = TRUE` only. |
| `home_team_team_logo_white` | character | JSON-encoded image node for the home team's white knockout logo, used on dark backgrounds. |
| `home_team_team_logo` | character | JSON-encoded image node for the home team's standard logo. |
| `home_team_league` | character | JSON-encoded league node identifying the league the home team plays in. |
| `home_team_season_leader_stats0` | character | JSON-encoded home-team season leader board occupying slot 0 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `home_team_season_leader_stats1` | character | JSON-encoded home-team season leader board occupying slot 1 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `home_team_season_leader_stats2` | character | JSON-encoded home-team season leader board occupying slot 2 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `home_team_season_leader_stats3` | character | JSON-encoded home-team season leader board occupying slot 3 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `home_team_player_season_stats0` | character | JSON-encoded home-team player season-stat block occupying slot 0 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `home_team_player_season_stats1` | character | JSON-encoded home-team player season-stat block occupying slot 1 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `home_team_player_season_stats2` | character | JSON-encoded home-team player season-stat block occupying slot 2 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `home_team_player_season_stats3` | character | JSON-encoded home-team player season-stat block occupying slot 3 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `home_team_player_season_stats4` | character | JSON-encoded home-team player season-stat block occupying slot 4 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `home_team_player_season_stats5` | character | JSON-encoded home-team player season-stat block occupying slot 5 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `home_team_player_season_stats6` | character | JSON-encoded home-team player season-stat block occupying slot 6 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |
| `home_team_player_season_stats7` | character | JSON-encoded home-team player season-stat block occupying slot 7 of that list in the payload; the slots are positional, so read the block's own stat ids rather than assuming a fixed category order. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_game_stats_leaders()
```

_Last validated n/a._

## `yahoo_gametime_game`

Yahoo shangrila persisted query `gametimeGame` -> one row per `games` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/gametimeGame`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/gametimeGame](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/gametimeGame)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameId` | `game_id` |  |  | `Y` | gameId query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `gametime_ticket_url` | character | Gametime affiliate ticket-purchase URL for the event or team. |
| `game_ticket_price` | character | Lowest available ticket price for the game from the Gametime affiliate feed, in US dollars. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_gametime_game()
```

_Last validated n/a._

## `yahoo_gametime_team`

Yahoo shangrila persisted query `gametimeTeam` -> one row per `teams` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/gametimeTeam`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/gametimeTeam](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/gametimeTeam)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `teamId` | `team_id` |  |  | `Y` | teamId query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `gametime_ticket_url` | character | Gametime affiliate ticket-purchase URL for the event or team. |
| `team_id` | character | Unique team identifier. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_gametime_team()
```

_Last validated n/a._

## `yahoo_golf_tournament_seasons`

Yahoo shangrila persisted query `golfTournamentSeasons` (response body not captured; shape unknown)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/golfTournamentSeasons`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/golfTournamentSeasons](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/golfTournamentSeasons)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `eventGroupId` | `event_group_id` |  |  | `Y` | eventGroupId query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_yahoo_shangrila`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_golf_tournament_seasons()
```

_Last validated n/a._

## `yahoo_golf_tournaments`

Yahoo shangrila persisted query `golfTournaments` -> one row per `golfTournaments` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/golfTournaments`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/golfTournaments](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/golfTournaments)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `association` | `association` |  |  | `Y` | association query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `showDefendingChamps` | `show_defending_champs` |  |  | `Y` | showDefendingChamps query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `alias_url` | character | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `alias_path` | character | Site-relative path portion of the entity's Yahoo alias (e.g., "/ncaaf/teams/tcu/"). |
| `event_group_id` | character | Yahoo identifier that groups the rounds or legs making up a single tournament. |
| `name` | character | Display name. |
| `display_name` | character | Display name. |
| `start_time` | character | Kickoff time in eastern time zone. |
| `start_date` | character | Start date (YYYY-MM-DD). |
| `end_date` | character | End date (YYYY-MM-DD). |
| `status` | character | Status label. |
| `status_display_name` | character | Short game or event status as shown on the scoreboard (e.g., "Final", "12:00 pm ET"). |
| `player_tournament_stats` | character | JSON-encoded per-player statistics recorded at the golf tournament. |
| `purse` | character | Total prize money on offer at the tournament, in US dollars. |
| `major` | logical | Flag indicating that the golf tournament is one of the sport's majors. |
| `venue_display_name` | character | Name of the venue hosting the event. |
| `venue_country` | character | Country the venue is located in. |
| `venue_city` | character | Venue city. |
| `venue_state` | character | Venue state / region. |
| `par` | integer | Par of the golf course in play for the tournament. |
| `yardage` | integer | Total yardage of the golf course in play for the tournament. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_golf_tournaments()
```

_Last validated n/a._

## `yahoo_golf_tournaments_basic`

Yahoo shangrila persisted query `golfTournamentsBasic` -> one row per `golfTournaments` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/golfTournamentsBasic`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/golfTournamentsBasic](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/golfTournamentsBasic)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `eventGroupId` | `event_group_id` |  |  | `Y` | eventGroupId query parameter. |
| `association` | `association` |  |  | `Y` | association query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `event_group_id` | character | Yahoo identifier that groups the rounds or legs making up a single tournament. |
| `start_time` | character | Kickoff time in eastern time zone. |
| `start_date` | character | Start date (YYYY-MM-DD). |
| `end_date` | character | End date (YYYY-MM-DD). |
| `season` | integer | Season year. |
| `clubs` | character | JSON-encoded list of the golf clubs hosting the tournament. |
| `courses` | character | JSON-encoded list of the courses in play at the tournament, with their par and yardage. |
| `name` | character | Display name. |
| `status` | character | Status label. |
| `alias_url` | character | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `alias_path` | character | Site-relative path portion of the entity's Yahoo alias (e.g., "/ncaaf/teams/tcu/"). |
| `association` | character | Governing tour that sanctions the event (e.g., "pga"). |
| `league_short_name` | character | League short name. |
| `league_full_name` | character | Full league name (e.g., "NCAA Football"). |
| `league_alias` | character | JSON-encoded Yahoo alias object for the league, carrying its site URL and path. |
| `purse` | character | Total prize money on offer at the tournament, in US dollars. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_golf_tournaments_basic()
```

_Last validated n/a._

## `yahoo_league_conferences`

Yahoo shangrila persisted query `leagueConferences` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueConferences`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueConferences](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueConferences)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | league query parameter. |
| `divisionIds` | `division_ids` |  |  | `Y` | divisionIds query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `short_name` | character | Short display name. |
| `conferences` | character | JSON-encoded list of the league's conference nodes, each carrying an id, a name and its member teams. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_league_conferences()
```

_Last validated n/a._

## `yahoo_league_filters_data`

Yahoo shangrila persisted query `leagueFiltersData` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueFiltersData`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueFiltersData](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueFiltersData)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | league query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `viewType` | `view_type` |  |  | `Y` | viewType query parameter. |
| `includePosAndSplitsData` | `include_pos_and_splits_data` |  |  | `Y` | includePosAndSplitsData query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `sport_sport_id` | character | Yahoo identifier of the sport the league belongs to. |
| `sport_name` | character | Sport name (e.g., Major League Baseball). |
| `short_name` | character | Short display name. |
| `full_name` | character | Player's full name. |
| `name` | character | Display name. |
| `current_league_day` | character | Calendar date the league's live scoreboard is anchored on, in YYYY-MM-DD form. |
| `teams` | character | Nested list of member-team membership spans. |
| `current_week` | integer | Week number within the league's current season phase, counting from 1. |
| `current_season_phase` | character | Phase of the season currently in effect (e.g., "season.phase.season", "season.phase.offseason"). |
| `current_game_season_phase` | character | Season phase of the games the league feed is currently serving. |
| `current_season` | integer | Season the league is currently playing, as the four-digit starting year. |
| `current_league_season` | character | Yahoo league-season identifier for the season currently in progress. |
| `league_seasons` | character | JSON-encoded list of the seasons for which Yahoo carries data for this league. |
| `league_weeks` | character | JSON-encoded list of the league's week nodes for the season. |
| `current_season_league_weeks` | character | JSON-encoded list of the week nodes making up the current league season. |
| `divisions` | character | JSON-encoded list of the league's division nodes, each carrying its member conferences and teams. |
| `conferences` | character | JSON-encoded list of the league's conference nodes, each carrying an id, a name and its member teams. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_league_filters_data()
```

_Last validated n/a._

## `yahoo_league_future_odds`

Yahoo shangrila persisted query `leagueFutureOdds` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueFutureOdds`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueFutureOdds](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueFutureOdds)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | league query parameter. |
| `betCategories` | `bet_categories` |  |  | `Y` | betCategories query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `bets` | character | JSON-encoded list of the betting markets offered on the event (spread, moneyline and total). |
| `league` | character | League slug. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_league_future_odds()
```

_Last validated n/a._

## `yahoo_league_game_ids`

Yahoo shangrila persisted query `leagueGameIds` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueGameIds`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueGameIds](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueGameIds)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `count` | `count` |  |  | `Y` | count query parameter. |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `week` | `week` |  |  | `Y` | Week number within the season. |
| `date` | `date` |  |  | `Y` | date query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `gameStatusOrder` | `game_status_order` |  |  | `Y` | gameStatusOrder query parameter. |
| `startTimeOrder` | `start_time_order` |  |  | `Y` | startTimeOrder query parameter. |
| `dateFlipOffset` | `date_flip_offset` |  |  | `Y` | dateFlipOffset query parameter. |
| `seasonPhase` | `season_phase` |  |  | `Y` | seasonPhase query parameter. |
| `conferenceIds` | `conference_ids` |  |  | `Y` | conferenceIds query parameter. |
| `top25` | `top25` |  |  | `Y` | top25 query parameter. |
| `gameDayQueryType` | `game_day_query_type` |  |  | `Y` | gameDayQueryType query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `alias_url` | character | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `alias_navigation_links` | character | JSON-encoded map of navigation links (scores, standings, teams) hanging off the entity's Yahoo alias. |
| `current_week` | integer | Week number within the league's current season phase, counting from 1. |
| `games` | character | Games played. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_league_game_ids()
```

_Last validated n/a._

## `yahoo_league_game_ids_by_date`

Yahoo shangrila persisted query `leagueGameIdsByDate` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueGameIdsByDate`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueGameIdsByDate](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueGameIdsByDate)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `leagues` | `leagues` |  |  | `Y` | leagues query parameter. |
| `week` | `week` |  |  | `Y` | Week number within the season. |
| `dates` | `dates` |  |  | `Y` | Date or date range filter (YYYYMMDD or YYYYMMDD-YYYYMMDD). |
| `startRange` | `start_range` |  |  | `Y` | startRange query parameter. |
| `endRange` | `end_range` |  |  | `Y` | endRange query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `seasonPhases` | `season_phases` |  |  | `Y` | seasonPhases query parameter. |
| `conferenceIds` | `conference_ids` |  |  | `Y` | conferenceIds query parameter. |
| `divisionIds` | `division_ids` |  |  | `Y` | divisionIds query parameter. |
| `top25` | `top25` |  |  | `Y` | top25 query parameter. |
| `tournamentIds` | `tournament_ids` |  |  | `Y` | tournamentIds query parameter. |
| `isTennis` | `is_tennis` |  |  | `Y` | isTennis query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `display_short` | character | Short league label used in navigation and compact UI (e.g., "NCAA FB"). |
| `full_name` | character | Player's full name. |
| `name` | character | Display name. |
| `current_week` | integer | Week number within the league's current season phase, counting from 1. |
| `current_game_season_phase` | character | Season phase of the games the league feed is currently serving. |
| `current_league_season` | character | Yahoo league-season identifier for the season currently in progress. |
| `games` | character | Games played. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_league_game_ids_by_date()
```

_Last validated n/a._

## `yahoo_league_games_by_round`

Yahoo shangrila persisted query `leagueGamesByRound` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueGamesByRound`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueGamesByRound](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueGamesByRound)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | league query parameter. |
| `tournamentRoundIds` | `tournament_round_ids` |  |  | `Y` | tournamentRoundIds query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_yahoo_shangrila`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_league_games_by_round()
```

_Last validated n/a._

## `yahoo_league_info`

Yahoo shangrila persisted query `leagueInfo` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueInfo`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueInfo](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueInfo)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | league query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `display_short` | character | Short league label used in navigation and compact UI (e.g., "NCAA FB"). |
| `name` | character | Display name. |
| `full_name` | character | Player's full name. |
| `short_name` | character | Short display name. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_league_info()
```

_Last validated n/a._

## `yahoo_league_injuries`

Yahoo shangrila persisted query `leagueInjuries` -> one row per `leagues.teams` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueInjuries`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueInjuries](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueInjuries)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `leagueId` | `league_id` |  |  | `Y` | leagueId query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `team_id` | character | Unique team identifier. |
| `nickname` | character | Team or athlete nickname. |
| `full_name` | character | Player's full name. |
| `location` | character | Either Home if the home team is playing in their home stadium, or Neutral if the game is being played at a neutral location. This still shows as Home for games between the Giants and Jets even though they share the same home stadium. |
| `display_name` | character | Display name. |
| `primary_color` | character | Primary team color (hex). |
| `abbreviation` | character | Short abbreviation. |
| `alias` | character | JSON-encoded Yahoo alias object for the entity, carrying the site URL, path and subpage routing used to build links to its page. |
| `team_logo_white` | character | JSON-encoded image node for the team's white knockout logo. |
| `team_logo` | character | Team logo image URL. |
| `players` | character | Nested list of per-player box scores. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_league_injuries()
```

_Last validated n/a._

## `yahoo_league_names`

Yahoo shangrila persisted query `leagueNames` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueNames`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueNames](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueNames)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `leagues` | `leagues` |  |  | `Y` | leagues query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `league_id` | integer | League identifier ('10' = WNBA). |
| `short_name` | character | Short display name. |
| `full_name` | character | Player's full name. |
| `name` | character | Display name. |
| `display_short` | character | Short league label used in navigation and compact UI (e.g., "NCAA FB"). |
| `display_abbr` | character | Compact league abbreviation used in dense UI (e.g., "NCAAF"). |
| `current_season` | integer | Season the league is currently playing, as the four-digit starting year. |
| `league_seasons` | character | JSON-encoded list of the seasons for which Yahoo carries data for this league. |
| `alias_url` | character | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `alias_path` | character | Site-relative path portion of the entity's Yahoo alias (e.g., "/ncaaf/teams/tcu/"). |
| `alias_subpages` | character | JSON-encoded list of subpage aliases (roster, schedule, stats) available beneath the entity's Yahoo page. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_league_names()
```

_Last validated n/a._

## `yahoo_league_prop_odds`

Yahoo shangrila persisted query `leaguePropOdds` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leaguePropOdds`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leaguePropOdds](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leaguePropOdds)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `count` | `count` |  |  | `Y` | count query parameter. |
| `league` | `league` |  |  | `Y` | league query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_yahoo_shangrila`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_league_prop_odds()
```

_Last validated n/a._

## `yahoo_league_standings`

Yahoo shangrila persisted query `leagueStandings` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueStandings`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueStandings](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueStandings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | league query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `seasonPhase` | `season_phase` |  |  | `Y` | seasonPhase query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `sport_name` | character | Sport name (e.g., Major League Baseball). |
| `current_season_phase` | character | Phase of the season currently in effect (e.g., "season.phase.season", "season.phase.offseason"). |
| `current_league_season` | character | Yahoo league-season identifier for the season currently in progress. |
| `divisions` | character | JSON-encoded list of the league's division nodes, each carrying its member conferences and teams. |
| `teams` | character | Nested list of member-team membership spans. |
| `conferences` | character | JSON-encoded list of the league's conference nodes, each carrying an id, a name and its member teams. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_league_standings()
```

_Last validated n/a._

## `yahoo_league_stats_by_team`

Yahoo shangrila persisted query `leagueStatsByTeam` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueStatsByTeam`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueStatsByTeam](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueStatsByTeam)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `leagues` | `leagues` |  |  | `Y` | leagues query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `leagueStructureId` | `league_structure_id` |  |  | `Y` | leagueStructureId query parameter. |
| `baseballCutType` | `baseball_cut_type` |  |  | `Y` | baseballCutType query parameter. |
| `basketballCutType` | `basketball_cut_type` |  |  | `Y` | basketballCutType query parameter. |
| `footballCutType` | `football_cut_type` |  |  | `Y` | footballCutType query parameter. |
| `hockeyCutType` | `hockey_cut_type` |  |  | `Y` | hockeyCutType query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `sport_sport_id` | character | Yahoo identifier of the sport the league belongs to. |
| `sport_name` | character | Sport name (e.g., Major League Baseball). |
| `short_name` | character | Short display name. |
| `full_name` | character | Player's full name. |
| `name` | character | Display name. |
| `football_stats` | character | JSON-encoded football statistics block returned by the league stats query. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_league_stats_by_team()
```

_Last validated n/a._

## `yahoo_league_stats_individual`

Yahoo shangrila persisted query `leagueStatsIndividual` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueStatsIndividual`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueStatsIndividual](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueStatsIndividual)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `leagues` | `leagues` |  |  | `Y` | leagues query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `qualified` | `qualified` |  |  | `Y` | qualified query parameter. |
| `leagueStructureId` | `league_structure_id` |  |  | `Y` | leagueStructureId query parameter. |
| `baseballCutType` | `baseball_cut_type` |  |  | `Y` | baseballCutType query parameter. |
| `baseballPosition` | `baseball_position` |  |  | `Y` | baseballPosition query parameter. |
| `basketballCutType` | `basketball_cut_type` |  |  | `Y` | basketballCutType query parameter. |
| `basketballPosition` | `basketball_position` |  |  | `Y` | basketballPosition query parameter. |
| `footballCutType` | `football_cut_type` |  |  | `Y` | footballCutType query parameter. |
| `hockeyCutType` | `hockey_cut_type` |  |  | `Y` | hockeyCutType query parameter. |
| `hockeyPosition` | `hockey_position` |  |  | `Y` | hockeyPosition query parameter. |
| `golfSortStat` | `golf_sort_stat` |  |  | `Y` | golfSortStat query parameter. |
| `golfStatIds` | `golf_stat_ids` |  |  | `Y` | golfStatIds query parameter. |
| `motorsportsSortStat` | `motorsports_sort_stat` |  |  | `Y` | motorsportsSortStat query parameter. |
| `motorsportsStatIds` | `motorsports_stat_ids` |  |  | `Y` | motorsportsStatIds query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `sport_sport_id` | character | Yahoo identifier of the sport the league belongs to. |
| `sport_name` | character | Sport name (e.g., Major League Baseball). |
| `short_name` | character | Short display name. |
| `full_name` | character | Player's full name. |
| `name` | character | Display name. |
| `football_stats` | character | JSON-encoded football statistics block returned by the league stats query. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_league_stats_individual()
```

_Last validated n/a._

## `yahoo_league_stats_overview`

Yahoo shangrila persisted query `leagueStatsOverview` (response body not captured; shape unknown)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueStatsOverview`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueStatsOverview](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueStatsOverview)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `leagues` | `leagues` |  |  | `Y` | leagues query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `week` | `week` |  |  | `Y` | Week number within the season. |
| `weekSeasonPhase` | `week_season_phase` |  |  | `Y` | weekSeasonPhase query parameter. |
| `seasonPhase` | `season_phase` |  |  | `Y` | seasonPhase query parameter. |
| `leagueStructureId` | `league_structure_id` |  |  | `Y` | leagueStructureId query parameter. |
| `golfSortStat` | `golf_sort_stat` |  |  | `Y` | golfSortStat query parameter. |
| `golfStatIds` | `golf_stat_ids` |  |  | `Y` | golfStatIds query parameter. |
| `motorsportsSortStat` | `motorsports_sort_stat` |  |  | `Y` | motorsportsSortStat query parameter. |
| `motorsportsStatIds` | `motorsports_stat_ids` |  |  | `Y` | motorsportsStatIds query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_yahoo_shangrila`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_league_stats_overview()
```

_Last validated n/a._

## `yahoo_league_stats_weekly`

Yahoo shangrila persisted query `leagueStatsWeekly` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueStatsWeekly`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueStatsWeekly](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueStatsWeekly)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `leagues` | `leagues` |  |  | `Y` | leagues query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `week` | `week` |  |  | `Y` | Week number within the season. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `seasonPhase` | `season_phase` |  |  | `Y` | seasonPhase query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `sport_sport_id` | character | Yahoo identifier of the sport the league belongs to. |
| `sport_name` | character | Sport name (e.g., Major League Baseball). |
| `short_name` | character | Short display name. |
| `full_name` | character | Player's full name. |
| `name` | character | Display name. |
| `football_stats` | character | JSON-encoded football statistics block returned by the league stats query. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_league_stats_weekly()
```

_Last validated n/a._

## `yahoo_league_team_ids`

Yahoo shangrila persisted query `leagueTeamIds` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueTeamIds`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueTeamIds](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueTeamIds)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | league query parameter. |
| `divisionIds` | `division_ids` |  |  | `Y` | divisionIds query parameter. |
| `getTeamsByDivision` | `get_teams_by_division` |  |  | `Y` | getTeamsByDivision query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `short_name` | character | Short display name. |
| `teams` | character | Nested list of member-team membership spans. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_league_team_ids()
```

_Last validated n/a._

## `yahoo_league_teams`

Yahoo shangrila persisted query `leagueTeams` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueTeams`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueTeams](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueTeams)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | league query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `divisionIds` | `division_ids` |  |  | `Y` | divisionIds query parameter. |
| `getTeamsByDivision` | `get_teams_by_division` |  |  | `Y` | getTeamsByDivision query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `short_name` | character | Short display name. |
| `teams` | character | Nested list of member-team membership spans. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_league_teams()
```

_Last validated n/a._

## `yahoo_leagues_season_states`

Yahoo shangrila persisted query `leaguesSeasonStates` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leaguesSeasonStates`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leaguesSeasonStates](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leaguesSeasonStates)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `leagues` | `leagues` |  |  | `Y` | leagues query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `name` | character | Display name. |
| `short_name` | character | Short display name. |
| `full_name` | character | Player's full name. |
| `display_short` | character | Short league label used in navigation and compact UI (e.g., "NCAA FB"). |
| `current_season_phase` | character | Phase of the season currently in effect (e.g., "season.phase.season", "season.phase.offseason"). |
| `current_week` | integer | Week number within the league's current season phase, counting from 1. |
| `current_season` | integer | Season the league is currently playing, as the four-digit starting year. |
| `stats_season` | character | Season the returned statistics cover, as a four-digit year. |
| `sport_name` | character | Sport name (e.g., Major League Baseball). |
| `league_weeks` | character | JSON-encoded list of the league's week nodes for the season. |
| `alias_url` | character | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `alias_navigation_links` | character | JSON-encoded map of navigation links (scores, standings, teams) hanging off the entity's Yahoo alias. |
| `league_seasons` | character | JSON-encoded list of the seasons for which Yahoo carries data for this league. |
| `bye_weeks` | character | JSON-encoded list of the week numbers in which the team has no scheduled game. |
| `divisions` | character | JSON-encoded list of the league's division nodes, each carrying its member conferences and teams. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_leagues_season_states()
```

_Last validated n/a._

## `yahoo_module_game`

Yahoo shangrila persisted query `moduleGame` -> one row per `games` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/moduleGame`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/moduleGame](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/moduleGame)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameId` | `game_id` |  |  | `Y` | gameId query parameter. |
| `imageHeight` | `image_height` |  |  | `Y` | imageHeight query parameter. |
| `imageWidth` | `image_width` |  |  | `Y` | imageWidth query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `display_name` | character | Display name. |
| `league_name` | character | League name. |
| `league_full_name` | character | Full league name (e.g., "NCAA Football"). |
| `league_display_abbr` | character | Compact league abbreviation used in dense UI alongside the game. |
| `league_display_short` | character | Short league label used in navigation and compact UI (e.g., "NCAA FB"). |
| `league_short_name` | character | League short name. |
| `league_sport` | character | Sport the league belongs to (e.g., "football"). |
| `league_alias` | character | JSON-encoded Yahoo alias object for the league, carrying its site URL and path. |
| `alias_url` | character | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `away_team_id` | character | Unique identifier for the away team. |
| `away_team_record` | character | Away team's win-loss record. |
| `away_team_full_name` | character | Full away team name (e.g. 'Las Vegas Aces'). |
| `away_team_team_id` | character | Yahoo composite team id of the away team (e.g., "ncaaf.t.29"). |
| `away_team_primary_color` | character | Primary brand color of the away team, as a hex RGB string without the leading hash. |
| `away_team_secondary_color` | character | Secondary brand color of the away team, as a hex RGB string without the leading hash. |
| `away_team_display_name` | character | Away team full display name; `team_detail = TRUE` only. |
| `away_team_abbreviation` | character | Away team abbreviation; `team_detail = TRUE` only. |
| `away_team_location` | character | Away team's team location. |
| `away_team_gametime_ticket_url` | character | Gametime affiliate ticket-purchase URL for the away team's games. |
| `away_team_alias` | character | JSON-encoded Yahoo alias object for the away team, carrying its site URL and path. |
| `away_team_nickname` | character | Away team nickname label; `team_detail = TRUE` only. |
| `away_team_last_games` | character | JSON-encoded list of the away team's most recently completed games. |
| `away_team_team_logo_white` | character | JSON-encoded image node for the away team's white knockout logo, used on dark backgrounds. |
| `away_team_team_logo` | character | JSON-encoded image node for the away team's standard logo. |
| `away_team_team_standings` | character | JSON-encoded standings node for the away team, carrying its record, position and streak. |
| `away_team_rank_polls` | character | JSON-encoded list of the poll rankings the away team currently holds. |
| `away_team_playoff_seeds` | character | JSON-encoded list of the away team's playoff-seed entries for the season. |
| `home_team_id` | character | Unique identifier for the home team. |
| `home_team_record` | character | Home team's win-loss record. |
| `home_team_full_name` | character | Full home team name (e.g. 'Las Vegas Aces'). |
| `home_team_team_id` | character | Yahoo composite team id of the home team (e.g., "ncaaf.t.29"). |
| `home_team_primary_color` | character | Primary brand color of the home team, as a hex RGB string without the leading hash. |
| `home_team_secondary_color` | character | Secondary brand color of the home team, as a hex RGB string without the leading hash. |
| `home_team_display_name` | character | Home team full display name; `team_detail = TRUE` only. |
| `home_team_abbreviation` | character | Home team abbreviation; `team_detail = TRUE` only. |
| `home_team_location` | character | Home team's team location. |
| `home_team_gametime_ticket_url` | character | Gametime affiliate ticket-purchase URL for the home team's games. |
| `home_team_alias` | character | JSON-encoded Yahoo alias object for the home team, carrying its site URL and path. |
| `home_team_nickname` | character | Home team nickname label; `team_detail = TRUE` only. |
| `home_team_last_games` | character | JSON-encoded list of the home team's most recently completed games. |
| `home_team_team_logo_white` | character | JSON-encoded image node for the home team's white knockout logo, used on dark backgrounds. |
| `home_team_team_logo` | character | JSON-encoded image node for the home team's standard logo. |
| `home_team_team_standings` | character | JSON-encoded standings node for the home team, carrying its record, position and streak. |
| `home_team_rank_polls` | character | JSON-encoded list of the poll rankings the home team currently holds. |
| `home_team_playoff_seeds` | character | JSON-encoded list of the home team's playoff-seed entries for the season. |
| `away_score` | integer | Away team score at the time of the play. |
| `home_score` | integer | Home team score at the time of the play. |
| `start_time` | character | Kickoff time in eastern time zone. |
| `start_date` | character | Start date (YYYY-MM-DD). |
| `if_necessary` | character | If necessary. |
| `status` | character | Status label. |
| `status_display_name` | character | Short game or event status as shown on the scoreboard (e.g., "Final", "12:00 pm ET"). |
| `season` | integer | Season year. |
| `season_phase` | character | Phase of the season the game falls in (e.g., "season.phase.season"). |
| `time_left` | character | Time left. |
| `tournament_id` | character | ESPN tournament identifier. |
| `gametime_ticket_url` | character | Gametime affiliate ticket-purchase URL for the event or team. |
| `game_ticket_price` | character | Lowest available ticket price for the game from the Gametime affiliate feed, in US dollars. |
| `playoff_series` | character | JSON-encoded playoff-series node the game belongs to. |
| `winning_team_id` | character | Composite Yahoo team id of the side that won the game (e.g., "ncaaf.t.29"). |
| `broadcast_channels` | character | JSON-encoded list of the channels broadcasting the event. |
| `news_break_subtext` | character | Secondary line of the news-break banner attached to the game. |
| `news_break_title` | character | Headline of the news-break banner attached to the game. |
| `news_break_url` | character | URL of the article behind the game's news-break banner. |
| `news_break_uuid` | character | Yahoo content UUID of the article behind the game's news-break banner. |
| `brief` | character | Short editorial blurb summarizing the game's state or result. |
| `event_extended_display_name` | character | Long-form event title used for marquee games, such as a bowl or rivalry name. |
| `bets` | character | JSON-encoded list of the betting markets offered on the event (spread, moneyline and total). |
| `venue_display_name` | character | Name of the venue hosting the event. |
| `venue_city` | character | Venue city. |
| `venue_cover_type` | character | Whether the venue is open-air, domed or fitted with a retractable roof. |
| `venue_state` | character | Venue state / region. |
| `venue_venue_id` | character | Yahoo identifier of the venue hosting the event. |
| `venue_country` | character | Country the venue is located in. |
| `tv_coverage` | character | Network carrying the game, as a short broadcast abbreviation (e.g., "CBS", "ESPN"). |
| `weather` | character | String describing the weather including temperature, humidity and wind (direction and speed). Doesn't change during the game! |
| `away_line_score` | character | JSON-encoded per-period scoring line for the away team. |
| `current_period_period` | character | Ordinal number of the period currently in progress within the game. |
| `field_position` | character | Ball spot expressed on Yahoo's 0-100 field scale, measured toward the offense's target goal line. |
| `field_position_display_name` | character | Ball spot rendered the way a scoreboard shows it (e.g., "MICH 35"). |
| `home_line_score` | character | JSON-encoded per-period scoring line for the home team. |
| `home_timeouts_remaining` | integer | Numeric timeouts remaining in the half for the home team. |
| `away_timeouts_remaining` | integer | Numeric timeouts remaining in the half for the away team. |
| `last_play` | character | Free-text description of the most recent play. |
| `game_stat_leaders` | character | JSON-encoded pointer to the per-category statistical leaders for the game. |
| `team_possessing_ball` | character | Yahoo team id of the side currently possessing the ball. |
| `recap_videos` | character | JSON-encoded list of recap videos published for the game. |
| `week` | integer | Week number. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_module_game()
```

_Last validated n/a._

## `yahoo_motorsport_standings`

Yahoo shangrila persisted query `motorsportStandings` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/motorsportStandings`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/motorsportStandings](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/motorsportStandings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | league query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `name` | character | Display name. |
| `full_name` | character | Player's full name. |
| `current_league_season` | character | Yahoo league-season identifier for the season currently in progress. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_motorsport_standings()
```

_Last validated n/a._

## `yahoo_nascar_drivers`

Yahoo shangrila persisted query `nascarDrivers` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/nascarDrivers`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/nascarDrivers](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/nascarDrivers)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | league query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `short_name` | character | Short display name. |
| `players` | character | Nested list of per-player box scores. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_nascar_drivers()
```

_Last validated n/a._

## `yahoo_nav_dropdown_tray`

Yahoo shangrila persisted query `navDropdownTray` -> tables: nfl, nhl, nba, mlb, wnba, ncaab, ncaaf, ncaaw, sportsbook_legal_states

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/navDropdownTray`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/navDropdownTray](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/navDropdownTray)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `getSoccerData` | `get_soccer_data` |  |  | `Y` | getSoccerData query parameter. |
| `soccerLeagueIds` | `soccer_league_ids` |  |  | `Y` | soccerLeagueIds query parameter. |
| `soccerTeamIds` | `soccer_team_ids` |  |  | `Y` | soccerTeamIds query parameter. |

### Returns

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (representative columns below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**nfl**

| col_name | type | description |
|---|---|---|
| `short_name` | character | Short display name. |
| `teams` | character | Nested list of member-team membership spans. |

**nhl**

| col_name | type | description |
|---|---|---|
| `short_name` | character | Short display name. |
| `teams` | character | Nested list of member-team membership spans. |

**nba**

| col_name | type | description |
|---|---|---|
| `short_name` | character | Short display name. |
| `teams` | character | Nested list of member-team membership spans. |

**mlb**

| col_name | type | description |
|---|---|---|
| `short_name` | character | Short display name. |
| `teams` | character | Nested list of member-team membership spans. |

**wnba**

| col_name | type | description |
|---|---|---|
| `short_name` | character | Short display name. |
| `teams` | character | Nested list of member-team membership spans. |

**ncaab**

| col_name | type | description |
|---|---|---|
| `poll_name` | character | Poll display name. |
| `rank` | character | Position of the school within the poll for the given week (1 = top-ranked). |
| `team_id` | character | Unique team identifier. |
| `team` | character | Team-side label or team identifier. |

**ncaaf**

| col_name | type | description |
|---|---|---|
| `poll_name` | character | Poll display name. |
| `rank` | character | Position of the school within the poll for the given week (1 = top-ranked). |
| `team_id` | character | Unique team identifier. |
| `team` | character | Team-side label or team identifier. |

**ncaaw**

| col_name | type | description |
|---|---|---|
| `poll_name` | character | Poll display name. |
| `rank` | character | Position of the school within the poll for the given week (1 = top-ranked). |
| `team_id` | character | Unique team identifier. |
| `team` | character | Team-side label or team identifier. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_nav_dropdown_tray()
```

_Last validated n/a._

## `yahoo_pick_distribution`

Yahoo shangrila persisted query `pickDistribution` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/pickDistribution`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/pickDistribution](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/pickDistribution)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | league query parameter. |
| `dates` | `dates` |  |  | `Y` | Date or date range filter (YYYYMMDD or YYYYMMDD-YYYYMMDD). |
| `count` | `count` |  |  | `Y` | count query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `ncaaf_games` | character | JSON-encoded list of NCAAF game nodes carrying the pick or odds distribution for the slate. |
| `conferences` | character | JSON-encoded list of the league's conference nodes, each carrying an id, a name and its member teams. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_pick_distribution()
```

_Last validated n/a._

## `yahoo_playbook_boxscore`

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

### Returns

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (representative columns below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**football_positions**

| col_name | type | description |
|---|---|---|
| `position_id` | character | Unique position identifier. |
| `name` | character | Display name. |
| `abbreviation` | character | Short abbreviation. |

**football_stat_types**

| col_name | type | description |
|---|---|---|
| `stat_id` | character | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `abbreviation` | character | Short abbreviation. |
| `display_name` | character | Display name. |
| `short_name` | character | Short display name. |
| `context_agnostic_abbreviation` | character | Abbreviation for the statistic that still reads correctly outside its category (e.g., "PassYds" where the in-category abbreviation is only "Yds"). |
| `sort_order` | character | Display sort order for the sport. |

**games**

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `display_name` | character | Display name. |
| `display_result` | character | Drive-result label (e.g. `Punt`, `Touchdown`). |
| `sport_name` | character | Sport name (e.g., Major League Baseball). |
| `league_name` | character | League name. |
| `league_full_name` | character | Full league name (e.g., "NCAA Football"). |
| `league_display_short` | character | Short league label used in navigation and compact UI (e.g., "NCAA FB"). |
| `league_short_name` | character | League short name. |
| `league_sport` | character | Sport the league belongs to (e.g., "football"). |
| `league_alias` | character | JSON-encoded Yahoo alias object for the league, carrying its site URL and path. |
| `alias_url` | character | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `special_event_type` | character | Marker identifying a special framing for the game, such as a bowl game or neutral-site showcase. |
| `away_team_id` | character | Unique identifier for the away team. |
| `basic_away_team_abbreviation` | character | Short abbreviation for the away team used in compact displays, as carried on the boxscore's lightweight team node. |
| `basic_away_team_display_name` | character | Display name of the away team as shown on the scoreboard, as carried on the boxscore's lightweight team node. |
| `basic_away_team_team_id` | character | Yahoo composite team id of the away team (e.g., "ncaaf.t.29"), as carried on the boxscore's lightweight team node. |
| `basic_away_team_team_logo_white` | character | JSON-encoded image node for the away team's white knockout logo, used on dark backgrounds, as carried on the boxscore's lightweight team node. |
| `basic_away_team_team_logo` | character | JSON-encoded image node for the away team's standard logo, as carried on the boxscore's lightweight team node. |
| `basic_away_team_playoff_seeds` | character | JSON-encoded list of the away team's playoff-seed entries for the season, as carried on the boxscore's lightweight team node. |
| `basic_away_team_nickname` | character | Nickname or mascot of the away team, as carried on the boxscore's lightweight team node. |
| `basic_away_team_primary_color` | character | Primary brand color of the away team, as a hex RGB string without the leading hash, as carried on the boxscore's lightweight team node. |
| `basic_away_team_secondary_color` | character | Secondary brand color of the away team, as a hex RGB string without the leading hash, as carried on the boxscore's lightweight team node. |
| `away_team_full_name` | character | Full away team name (e.g. 'Las Vegas Aces'). |
| `away_team_team_id` | character | Yahoo composite team id of the away team (e.g., "ncaaf.t.29"). |
| `away_team_primary_color` | character | Primary brand color of the away team, as a hex RGB string without the leading hash. |
| `away_team_secondary_color` | character | Secondary brand color of the away team, as a hex RGB string without the leading hash. |
| `away_team_display_name` | character | Away team full display name; `team_detail = TRUE` only. |
| `away_team_abbreviation` | character | Away team abbreviation; `team_detail = TRUE` only. |
| `away_team_location` | character | Away team's team location. |
| `away_team_alias` | character | JSON-encoded Yahoo alias object for the away team, carrying its site URL and path. |
| `away_team_nickname` | character | Away team nickname label; `team_detail = TRUE` only. |
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
| `away_team_record` | character | Away team's win-loss record. |
| `home_team_id` | character | Unique identifier for the home team. |
| `basic_home_team_abbreviation` | character | Short abbreviation for the home team used in compact displays, as carried on the boxscore's lightweight team node. |
| `basic_home_team_display_name` | character | Display name of the home team as shown on the scoreboard, as carried on the boxscore's lightweight team node. |
| `basic_home_team_team_id` | character | Yahoo composite team id of the home team (e.g., "ncaaf.t.29"), as carried on the boxscore's lightweight team node. |
| `basic_home_team_team_logo_white` | character | JSON-encoded image node for the home team's white knockout logo, used on dark backgrounds, as carried on the boxscore's lightweight team node. |
| `basic_home_team_team_logo` | character | JSON-encoded image node for the home team's standard logo, as carried on the boxscore's lightweight team node. |
| `basic_home_team_playoff_seeds` | character | JSON-encoded list of the home team's playoff-seed entries for the season, as carried on the boxscore's lightweight team node. |
| `basic_home_team_nickname` | character | Nickname or mascot of the home team, as carried on the boxscore's lightweight team node. |
| `basic_home_team_primary_color` | character | Primary brand color of the home team, as a hex RGB string without the leading hash, as carried on the boxscore's lightweight team node. |
| `basic_home_team_secondary_color` | character | Secondary brand color of the home team, as a hex RGB string without the leading hash, as carried on the boxscore's lightweight team node. |
| `home_team_full_name` | character | Full home team name (e.g. 'Las Vegas Aces'). |
| `home_team_team_id` | character | Yahoo composite team id of the home team (e.g., "ncaaf.t.29"). |
| `home_team_primary_color` | character | Primary brand color of the home team, as a hex RGB string without the leading hash. |
| `home_team_secondary_color` | character | Secondary brand color of the home team, as a hex RGB string without the leading hash. |
| `home_team_display_name` | character | Home team full display name; `team_detail = TRUE` only. |
| `home_team_abbreviation` | character | Home team abbreviation; `team_detail = TRUE` only. |
| `home_team_location` | character | Home team's team location. |
| `home_team_alias` | character | JSON-encoded Yahoo alias object for the home team, carrying its site URL and path. |
| `home_team_nickname` | character | Home team nickname label; `team_detail = TRUE` only. |
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
| `home_team_record` | character | Home team's win-loss record. |
| `away_score` | integer | Away team score at the time of the play. |
| `home_score` | integer | Home team score at the time of the play. |
| `start_time` | character | Kickoff time in eastern time zone. |
| `start_date` | character | Start date (YYYY-MM-DD). |
| `if_necessary` | character | If necessary. |
| `status` | character | Status label. |
| `status_display_name` | character | Short game or event status as shown on the scoreboard (e.g., "Final", "12:00 pm ET"). |
| `full_status_display_name` | character | Long-form game status label including overtime and date context (e.g., "Final/OT"). |
| `season` | integer | Season year. |
| `season_phase` | character | Phase of the season the game falls in (e.g., "season.phase.season"). |
| `time_left` | character | Time left. |
| `is_halftime` | logical | Flag indicating that the game is currently stopped at halftime. |
| `tournament_id` | character | ESPN tournament identifier. |
| `week` | integer | Week number. |
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
| `venue_city` | character | Venue city. |
| `venue_cover_type` | character | Whether the venue is open-air, domed or fitted with a retractable roof. |
| `venue_state` | character | Venue state / region. |
| `venue_venue_id` | character | Yahoo identifier of the venue hosting the event. |
| `venue_country` | character | Country the venue is located in. |
| `tv_coverage` | character | Network carrying the game, as a short broadcast abbreviation (e.g., "CBS", "ESPN"). |
| `weather` | character | String describing the weather including temperature, humidity and wind (direction and speed). Doesn't change during the game! |
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
| `regulation_periods` | character | Regulation periods. |
| `away_team_lineup` | character | JSON-encoded starting lineup fielded by the away team. |
| `home_team_lineup` | character | JSON-encoded starting lineup fielded by the home team. |
| `game_ticket_price` | character | Lowest available ticket price for the game from the Gametime affiliate feed, in US dollars. |
| `gametime_ticket_url` | character | Gametime affiliate ticket-purchase URL for the event or team. |
| `game_coverage` | character | JSON-encoded node describing which live feeds Yahoo carries for the game. |
| `down` | character | The down for the given play. |
| `distance` | character | Distance value (in feet for shot data; otherwise context-dependent). |
| `field_position` | character | Ball spot expressed on Yahoo's 0-100 field scale, measured toward the offense's target goal line. |
| `field_position_display_name` | character | Ball spot rendered the way a scoreboard shows it (e.g., "MICH 35"). |
| `home_timeouts_remaining` | integer | Numeric timeouts remaining in the half for the home team. |
| `away_timeouts_remaining` | integer | Numeric timeouts remaining in the half for the away team. |
| `timeouts_granted` | integer | Number of timeouts granted so far in the current period. |
| `team_possessing_ball` | character | Yahoo team id of the side currently possessing the ball. |
| `away_line_score` | character | JSON-encoded per-period scoring line for the away team. |
| `home_line_score` | character | JSON-encoded per-period scoring line for the home team. |
| `first_play` | character | JSON-encoded first play of the game or of the current period. |
| `last_play` | character | Free-text description of the most recent play. |
| `recap_videos` | character | JSON-encoded list of recap videos published for the game. |
| `play_by_play` | character | JSON-encoded data-island pointer to the game's play-by-play collection in the same editorial payload. |
| `drives` | character | JSON-encoded data-island pointer to the game's drive collection; football only. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_playbook_boxscore()
```

_Last validated n/a._

## `yahoo_playbook_boxscore_poll`

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

### Returns

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (representative columns below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**football_positions**

| col_name | type | description |
|---|---|---|
| `position_id` | character | Unique position identifier. |
| `name` | character | Display name. |
| `abbreviation` | character | Short abbreviation. |

**football_stat_types**

| col_name | type | description |
|---|---|---|
| `stat_id` | character | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `abbreviation` | character | Short abbreviation. |
| `display_name` | character | Display name. |
| `short_name` | character | Short display name. |
| `context_agnostic_abbreviation` | character | Abbreviation for the statistic that still reads correctly outside its category (e.g., "PassYds" where the in-category abbreviation is only "Yds"). |
| `sort_order` | character | Display sort order for the sport. |

**games**

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `display_name` | character | Display name. |
| `season` | integer | Season year. |
| `league_name` | character | League name. |
| `league_full_name` | character | Full league name (e.g., "NCAA Football"). |
| `sport_name` | character | Sport name (e.g., Major League Baseball). |
| `alias_url` | character | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `away_score` | integer | Away team score at the time of the play. |
| `basic_away_team_abbreviation` | character | Short abbreviation for the away team used in compact displays, as carried on the boxscore's lightweight team node. |
| `basic_away_team_display_name` | character | Display name of the away team as shown on the scoreboard, as carried on the boxscore's lightweight team node. |
| `basic_away_team_team_id` | character | Yahoo composite team id of the away team (e.g., "ncaaf.t.29"), as carried on the boxscore's lightweight team node. |
| `basic_away_team_team_logo_white` | character | JSON-encoded image node for the away team's white knockout logo, used on dark backgrounds, as carried on the boxscore's lightweight team node. |
| `basic_away_team_team_logo` | character | JSON-encoded image node for the away team's standard logo, as carried on the boxscore's lightweight team node. |
| `basic_away_team_playoff_seeds` | character | JSON-encoded list of the away team's playoff-seed entries for the season, as carried on the boxscore's lightweight team node. |
| `basic_away_team_nickname` | character | Nickname or mascot of the away team, as carried on the boxscore's lightweight team node. |
| `basic_away_team_primary_color` | character | Primary brand color of the away team, as a hex RGB string without the leading hash, as carried on the boxscore's lightweight team node. |
| `basic_away_team_secondary_color` | character | Secondary brand color of the away team, as a hex RGB string without the leading hash, as carried on the boxscore's lightweight team node. |
| `away_team_full_name` | character | Full away team name (e.g. 'Las Vegas Aces'). |
| `away_team_team_id` | character | Yahoo composite team id of the away team (e.g., "ncaaf.t.29"). |
| `away_team_primary_color` | character | Primary brand color of the away team, as a hex RGB string without the leading hash. |
| `away_team_secondary_color` | character | Secondary brand color of the away team, as a hex RGB string without the leading hash. |
| `away_team_display_name` | character | Away team full display name; `team_detail = TRUE` only. |
| `away_team_abbreviation` | character | Away team abbreviation; `team_detail = TRUE` only. |
| `away_team_location` | character | Away team's team location. |
| `away_team_alias` | character | JSON-encoded Yahoo alias object for the away team, carrying its site URL and path. |
| `away_team_nickname` | character | Away team nickname label; `team_detail = TRUE` only. |
| `away_team_team_logo_white` | character | JSON-encoded image node for the away team's white knockout logo, used on dark backgrounds. |
| `away_team_team_logo` | character | JSON-encoded image node for the away team's standard logo. |
| `away_team_team_logo_white_large` | character | JSON-encoded image node for the large-format white knockout away-team logo. |
| `away_team_team_logo_large` | character | JSON-encoded image node for the large-format away-team logo. |
| `away_team_team_standings` | character | JSON-encoded standings node for the away team, carrying its record, position and streak. |
| `away_team_rank_polls` | character | JSON-encoded list of the poll rankings the away team currently holds. |
| `away_team_playoff_seeds` | character | JSON-encoded list of the away team's playoff-seed entries for the season. |
| `away_team_record` | character | Away team's win-loss record. |
| `away_team_id` | character | Unique identifier for the away team. |
| `bets` | character | JSON-encoded list of the betting markets offered on the event (spread, moneyline and total). |
| `partial_game_bets` | character | JSON-encoded list of in-game betting markets covering only part of the game, such as halves or quarters. |
| `brief` | character | Short editorial blurb summarizing the game's state or result. |
| `broadcast_channels` | character | JSON-encoded list of the channels broadcasting the event. |
| `home_score` | integer | Home team score at the time of the play. |
| `home_team_full_name` | character | Full home team name (e.g. 'Las Vegas Aces'). |
| `home_team_team_id` | character | Yahoo composite team id of the home team (e.g., "ncaaf.t.29"). |
| `home_team_primary_color` | character | Primary brand color of the home team, as a hex RGB string without the leading hash. |
| `home_team_secondary_color` | character | Secondary brand color of the home team, as a hex RGB string without the leading hash. |
| `home_team_display_name` | character | Home team full display name; `team_detail = TRUE` only. |
| `home_team_abbreviation` | character | Home team abbreviation; `team_detail = TRUE` only. |
| `home_team_location` | character | Home team's team location. |
| `home_team_alias` | character | JSON-encoded Yahoo alias object for the home team, carrying its site URL and path. |
| `home_team_nickname` | character | Home team nickname label; `team_detail = TRUE` only. |
| `home_team_team_logo_white` | character | JSON-encoded image node for the home team's white knockout logo, used on dark backgrounds. |
| `home_team_team_logo` | character | JSON-encoded image node for the home team's standard logo. |
| `home_team_team_logo_white_large` | character | JSON-encoded image node for the large-format white knockout home-team logo. |
| `home_team_team_logo_large` | character | JSON-encoded image node for the large-format home-team logo. |
| `home_team_team_standings` | character | JSON-encoded standings node for the home team, carrying its record, position and streak. |
| `home_team_rank_polls` | character | JSON-encoded list of the poll rankings the home team currently holds. |
| `home_team_playoff_seeds` | character | JSON-encoded list of the home team's playoff-seed entries for the season. |
| `home_team_record` | character | Home team's win-loss record. |
| `home_team_id` | character | Unique identifier for the home team. |
| `basic_home_team_abbreviation` | character | Short abbreviation for the home team used in compact displays, as carried on the boxscore's lightweight team node. |
| `basic_home_team_display_name` | character | Display name of the home team as shown on the scoreboard, as carried on the boxscore's lightweight team node. |
| `basic_home_team_team_id` | character | Yahoo composite team id of the home team (e.g., "ncaaf.t.29"), as carried on the boxscore's lightweight team node. |
| `basic_home_team_team_logo_white` | character | JSON-encoded image node for the home team's white knockout logo, used on dark backgrounds, as carried on the boxscore's lightweight team node. |
| `basic_home_team_team_logo` | character | JSON-encoded image node for the home team's standard logo, as carried on the boxscore's lightweight team node. |
| `basic_home_team_playoff_seeds` | character | JSON-encoded list of the home team's playoff-seed entries for the season, as carried on the boxscore's lightweight team node. |
| `basic_home_team_nickname` | character | Nickname or mascot of the home team, as carried on the boxscore's lightweight team node. |
| `basic_home_team_primary_color` | character | Primary brand color of the home team, as a hex RGB string without the leading hash, as carried on the boxscore's lightweight team node. |
| `basic_home_team_secondary_color` | character | Secondary brand color of the home team, as a hex RGB string without the leading hash, as carried on the boxscore's lightweight team node. |
| `start_date` | character | Start date (YYYY-MM-DD). |
| `start_time` | character | Kickoff time in eastern time zone. |
| `status` | character | Status label. |
| `status_display_name` | character | Short game or event status as shown on the scoreboard (e.g., "Final", "12:00 pm ET"). |
| `full_status_display_name` | character | Long-form game status label including overtime and date context (e.g., "Final/OT"). |
| `time_left` | character | Time left. |
| `venue_display_name` | character | Name of the venue hosting the event. |
| `venue_city` | character | Venue city. |
| `venue_cover_type` | character | Whether the venue is open-air, domed or fitted with a retractable roof. |
| `venue_state` | character | Venue state / region. |
| `venue_venue_id` | character | Yahoo identifier of the venue hosting the event. |
| `venue_country` | character | Country the venue is located in. |
| `weather` | character | String describing the weather including temperature, humidity and wind (direction and speed). Doesn't change during the game! |
| `game_win_probability_time_line_win_probability_timeline` | character | JSON-encoded series of win-probability observations across the course of the game. |
| `playoff_series` | character | JSON-encoded playoff-series node the game belongs to. |
| `regular_season_series_games` | character | JSON-encoded list of the games making up the regular-season series between the two teams. |
| `is_halftime` | logical | Flag indicating that the game is currently stopped at halftime. |
| `regulation_periods` | character | Regulation periods. |
| `current_period_short_display_name` | character | Abbreviated label for the period in progress (e.g., "4th"). |
| `current_period_period` | character | Ordinal number of the period currently in progress within the game. |
| `current_period_display_name` | character | Full label for the period in progress (e.g., "4th Quarter"). |
| `current_period_overtime` | character | Flag indicating that the period in progress is an overtime period. |
| `away_team_lineup` | character | JSON-encoded starting lineup fielded by the away team. |
| `home_team_lineup` | character | JSON-encoded starting lineup fielded by the home team. |
| `winning_team_id` | character | Composite Yahoo team id of the side that won the game (e.g., "ncaaf.t.29"). |
| `game_coverage` | character | JSON-encoded node describing which live feeds Yahoo carries for the game. |
| `away_line_score` | character | JSON-encoded per-period scoring line for the away team. |
| `away_timeouts_remaining` | integer | Numeric timeouts remaining in the half for the away team. |
| `down` | character | The down for the given play. |
| `distance` | character | Distance value (in feet for shot data; otherwise context-dependent). |
| `field_position_display_name` | character | Ball spot rendered the way a scoreboard shows it (e.g., "MICH 35"). |
| `field_position` | character | Ball spot expressed on Yahoo's 0-100 field scale, measured toward the offense's target goal line. |
| `home_line_score` | character | JSON-encoded per-period scoring line for the home team. |
| `home_timeouts_remaining` | integer | Numeric timeouts remaining in the half for the home team. |
| `team_possessing_ball` | character | Yahoo team id of the side currently possessing the ball. |
| `drives` | character | JSON-encoded data-island pointer to the game's drive collection; football only. |
| `first_play` | character | JSON-encoded first play of the game or of the current period. |
| `play_by_play` | character | JSON-encoded data-island pointer to the game's play-by-play collection in the same editorial payload. |
| `timeouts_granted` | integer | Number of timeouts granted so far in the current period. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_playbook_boxscore_poll()
```

_Last validated n/a._

## `yahoo_playbook_boxscore_social_share`

Yahoo shangrila persisted query `playbookBoxscoreSocialShare` -> one row per `games` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookBoxscoreSocialShare`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookBoxscoreSocialShare](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookBoxscoreSocialShare)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameId` | `game_id` |  |  | `Y` | gameId query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `away_team_active` | character | Whether the away team is active. |
| `away_team_primary_color` | character | Primary brand color of the away team, as a hex RGB string without the leading hash. |
| `away_team_secondary_color` | character | Secondary brand color of the away team, as a hex RGB string without the leading hash. |
| `away_team_team_logo_white` | character | JSON-encoded image node for the away team's white knockout logo, used on dark backgrounds. |
| `away_team_team_logo` | character | JSON-encoded image node for the away team's standard logo. |
| `home_team_active` | character | Whether the home team is active. |
| `home_team_primary_color` | character | Primary brand color of the home team, as a hex RGB string without the leading hash. |
| `home_team_secondary_color` | character | Secondary brand color of the home team, as a hex RGB string without the leading hash. |
| `home_team_team_logo_white` | character | JSON-encoded image node for the home team's white knockout logo, used on dark backgrounds. |
| `home_team_team_logo` | character | JSON-encoded image node for the home team's standard logo. |
| `league_league_logo` | character | JSON-encoded image node for the league's logo. |
| `start_time` | character | Kickoff time in eastern time zone. |
| `start_date` | character | Start date (YYYY-MM-DD). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_playbook_boxscore_social_share()
```

_Last validated n/a._

## `yahoo_playbook_combat_match`

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

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_yahoo_shangrila`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_playbook_combat_match()
```

_Last validated n/a._

## `yahoo_playbook_game`

Yahoo shangrila persisted query `playbookGame` -> one row per `games` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookGame`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookGame](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookGame)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameId` | `game_id` |  |  | `Y` | gameId query parameter. |
| `imageHeight` | `image_height` |  |  | `Y` | imageHeight query parameter. |
| `imageWidth` | `image_width` |  |  | `Y` | imageWidth query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `display_name` | character | Display name. |
| `league_name` | character | League name. |
| `league_full_name` | character | Full league name (e.g., "NCAA Football"). |
| `league_display_short` | character | Short league label used in navigation and compact UI (e.g., "NCAA FB"). |
| `league_short_name` | character | League short name. |
| `league_sport` | character | Sport the league belongs to (e.g., "football"). |
| `league_alias` | character | JSON-encoded Yahoo alias object for the league, carrying its site URL and path. |
| `alias_url` | character | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `away_team_id` | character | Unique identifier for the away team. |
| `away_team_full_name` | character | Full away team name (e.g. 'Las Vegas Aces'). |
| `away_team_team_id` | character | Yahoo composite team id of the away team (e.g., "ncaaf.t.29"). |
| `away_team_primary_color` | character | Primary brand color of the away team, as a hex RGB string without the leading hash. |
| `away_team_secondary_color` | character | Secondary brand color of the away team, as a hex RGB string without the leading hash. |
| `away_team_display_name` | character | Away team full display name; `team_detail = TRUE` only. |
| `away_team_abbreviation` | character | Away team abbreviation; `team_detail = TRUE` only. |
| `away_team_location` | character | Away team's team location. |
| `away_team_alias` | character | JSON-encoded Yahoo alias object for the away team, carrying its site URL and path. |
| `away_team_nickname` | character | Away team nickname label; `team_detail = TRUE` only. |
| `away_team_last_games` | character | JSON-encoded list of the away team's most recently completed games. |
| `away_team_team_logo_white` | character | JSON-encoded image node for the away team's white knockout logo, used on dark backgrounds. |
| `away_team_team_logo` | character | JSON-encoded image node for the away team's standard logo. |
| `away_team_team_standings` | character | JSON-encoded standings node for the away team, carrying its record, position and streak. |
| `away_team_players` | character | JSON-encoded roster of away-team players attached to the game. |
| `away_team_rank_polls` | character | JSON-encoded list of the poll rankings the away team currently holds. |
| `away_team_playoff_seeds` | character | JSON-encoded list of the away team's playoff-seed entries for the season. |
| `home_team_id` | character | Unique identifier for the home team. |
| `home_team_full_name` | character | Full home team name (e.g. 'Las Vegas Aces'). |
| `home_team_team_id` | character | Yahoo composite team id of the home team (e.g., "ncaaf.t.29"). |
| `home_team_primary_color` | character | Primary brand color of the home team, as a hex RGB string without the leading hash. |
| `home_team_secondary_color` | character | Secondary brand color of the home team, as a hex RGB string without the leading hash. |
| `home_team_display_name` | character | Home team full display name; `team_detail = TRUE` only. |
| `home_team_abbreviation` | character | Home team abbreviation; `team_detail = TRUE` only. |
| `home_team_location` | character | Home team's team location. |
| `home_team_alias` | character | JSON-encoded Yahoo alias object for the home team, carrying its site URL and path. |
| `home_team_nickname` | character | Home team nickname label; `team_detail = TRUE` only. |
| `home_team_last_games` | character | JSON-encoded list of the home team's most recently completed games. |
| `home_team_team_logo_white` | character | JSON-encoded image node for the home team's white knockout logo, used on dark backgrounds. |
| `home_team_team_logo` | character | JSON-encoded image node for the home team's standard logo. |
| `home_team_team_standings` | character | JSON-encoded standings node for the home team, carrying its record, position and streak. |
| `home_team_players` | character | JSON-encoded roster of home-team players attached to the game. |
| `home_team_rank_polls` | character | JSON-encoded list of the poll rankings the home team currently holds. |
| `home_team_playoff_seeds` | character | JSON-encoded list of the home team's playoff-seed entries for the season. |
| `away_score` | integer | Away team score at the time of the play. |
| `home_score` | integer | Home team score at the time of the play. |
| `start_time` | character | Kickoff time in eastern time zone. |
| `start_date` | character | Start date (YYYY-MM-DD). |
| `if_necessary` | character | If necessary. |
| `status` | character | Status label. |
| `status_display_name` | character | Short game or event status as shown on the scoreboard (e.g., "Final", "12:00 pm ET"). |
| `season` | integer | Season year. |
| `season_phase` | character | Phase of the season the game falls in (e.g., "season.phase.season"). |
| `time_left` | character | Time left. |
| `tournament_id` | character | ESPN tournament identifier. |
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
| `venue_city` | character | Venue city. |
| `venue_cover_type` | character | Whether the venue is open-air, domed or fitted with a retractable roof. |
| `venue_state` | character | Venue state / region. |
| `venue_venue_id` | character | Yahoo identifier of the venue hosting the event. |
| `venue_country` | character | Country the venue is located in. |
| `tv_coverage` | character | Network carrying the game, as a short broadcast abbreviation (e.g., "CBS", "ESPN"). |
| `weather` | character | String describing the weather including temperature, humidity and wind (direction and speed). Doesn't change during the game! |
| `away_line_score` | character | JSON-encoded per-period scoring line for the away team. |
| `current_period_period` | character | Ordinal number of the period currently in progress within the game. |
| `field_position` | character | Ball spot expressed on Yahoo's 0-100 field scale, measured toward the offense's target goal line. |
| `field_position_display_name` | character | Ball spot rendered the way a scoreboard shows it (e.g., "MICH 35"). |
| `home_line_score` | character | JSON-encoded per-period scoring line for the home team. |
| `home_timeouts_remaining` | integer | Numeric timeouts remaining in the half for the home team. |
| `away_timeouts_remaining` | integer | Numeric timeouts remaining in the half for the away team. |
| `last_play` | character | Free-text description of the most recent play. |
| `game_stat_leaders` | character | JSON-encoded pointer to the per-category statistical leaders for the game. |
| `team_possessing_ball` | character | Yahoo team id of the side currently possessing the ball. |
| `recap_videos` | character | JSON-encoded list of recap videos published for the game. |
| `week` | integer | Week number. |
| `play_by_play` | character | JSON-encoded data-island pointer to the game's play-by-play collection in the same editorial payload. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_playbook_game()
```

_Last validated n/a._

## `yahoo_playbook_game_odds_poll`

Yahoo shangrila persisted query `playbookGameOddsPoll` -> one row per `games` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookGameOddsPoll`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookGameOddsPoll](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookGameOddsPoll)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameId` | `game_id` |  |  | `Y` | gameId query parameter. |
| `eventState` | `event_state` |  |  | `Y` | eventState query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `bets` | character | JSON-encoded list of the betting markets offered on the event (spread, moneyline and total). |
| `partial_game_bets` | character | JSON-encoded list of in-game betting markets covering only part of the game, such as halves or quarters. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_playbook_game_odds_poll()
```

_Last validated n/a._

## `yahoo_playbook_golf_tournament`

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

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_yahoo_shangrila`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_playbook_golf_tournament()
```

_Last validated n/a._

## `yahoo_playbook_league_odds`

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

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `ncaaf_games` | character | JSON-encoded list of NCAAF game nodes carrying the pick or odds distribution for the slate. |
| `conferences` | character | JSON-encoded list of the league's conference nodes, each carrying an id, a name and its member teams. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_playbook_league_odds()
```

_Last validated n/a._

## `yahoo_playbook_player`

Yahoo shangrila persisted query `playbookPlayer` -> one row per `players` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookPlayer`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookPlayer](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookPlayer)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `playerId` | `player_id` |  |  | `Y` | playerId query parameter. |
| `seasonPhases` | `season_phases` |  |  | `Y` | seasonPhases query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_id` | character | Unique player identifier. |
| `alias_url` | character | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `alias_subpages` | character | JSON-encoded list of subpage aliases (roster, schedule, stats) available beneath the entity's Yahoo page. |
| `sport_name` | character | Sport name (e.g., Major League Baseball). |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `display_name` | character | Display name. |
| `college` | character | Official college (usually the last one attended) |
| `birth_state` | character | Birth state / region. |
| `birth_city` | character | Birth city. |
| `birth_country` | character | Player birth country. |
| `birth_date` | character | Date of birth (YYYY-MM-DD). |
| `height` | integer | Player height (string e.g. '6-2' or inches). |
| `display_height` | character | Player height in display format (e.g. '6-2'). |
| `weight` | integer | Player weight in pounds. |
| `status` | character | Status label. |
| `active` | logical | TRUE if the row represents an active record (player / team / season). |
| `suggested_headshot` | character | JSON-encoded image node for the headshot Yahoo recommends for this player. |
| `uniform_number` | character | Jersey number the player wears for the team. |
| `positions` | character | Positions. |
| `team_id` | character | Unique team identifier. |
| `team_team_id` | character | Unique identifier for team team. |
| `team_display_name` | character | Full team display name. |
| `team_full_name` | character | Full team name. |
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
| `injury` | character | Injury (body part / description). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_playbook_player()
```

_Last validated n/a._

## `yahoo_playbook_player_social_share`

Yahoo shangrila persisted query `playbookPlayerSocialShare` -> one row per `players` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookPlayerSocialShare`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookPlayerSocialShare](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookPlayerSocialShare)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `playerId` | `player_id` |  |  | `Y` | playerId query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `sport_name` | character | Sport name (e.g., Major League Baseball). |
| `short_display_name` | character | Short display name. |
| `suggested_headshot` | character | JSON-encoded image node for the headshot Yahoo recommends for this player. |
| `team_primary_color` | character | Primary brand color of the entity's team, as a hex RGB string without the leading hash. |
| `team_secondary_color` | character | Secondary brand color of the entity's team, as a hex RGB string without the leading hash. |
| `team_league` | character | JSON-encoded league node for the entity's team. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_playbook_player_social_share()
```

_Last validated n/a._

## `yahoo_playbook_race`

Yahoo shangrila persisted query `playbookRace` -> one row per `games` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookRace`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookRace](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookRace)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameId` | `game_id` |  |  | `Y` | gameId query parameter. |
| `playerImageHeight` | `player_image_height` |  |  | `Y` | playerImageHeight query parameter. |
| `playerImageWidth` | `player_image_width` |  |  | `Y` | playerImageWidth query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_yahoo_shangrila`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_playbook_race()
```

_Last validated n/a._

## `yahoo_playbook_team`

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

### Returns

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (representative columns below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**teams**

| col_name | type | description |
|---|---|---|
| `sport_sport_id` | character | Yahoo identifier of the sport the league belongs to. |
| `sport_name` | character | Sport name (e.g., Major League Baseball). |
| `league_name` | character | League name. |
| `league_short_name` | character | League short name. |
| `league_current_season_phase` | character | Phase the league's season is currently in (e.g., "season.phase.season"). |
| `team_id` | character | Unique team identifier. |
| `conference_id` | integer | Conference identifier. |
| `full_name` | character | Player's full name. |
| `display_name` | character | Display name. |
| `location` | character | Either Home if the home team is playing in their home stadium, or Neutral if the game is being played at a neutral location. This still shows as Home for games between the Giants and Jets even though they share the same home stadium. |
| `nickname` | character | Team or athlete nickname. |
| `primary_color` | character | Primary team color (hex). |
| `secondary_color` | character | Secondary team color (hex). |
| `team_logo_white_url` | character | Absolute URL of the team's white knockout logo, the variant used on dark backgrounds. |
| `team_logo_url` | character | Absolute URL of the team's standard logo image on Yahoo's image CDN. |
| `alias_navigation_links` | character | JSON-encoded map of navigation links (scores, standings, teams) hanging off the entity's Yahoo alias. |
| `alias_url` | character | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `alias_path` | character | Site-relative path portion of the entity's Yahoo alias (e.g., "/ncaaf/teams/tcu/"). |
| `last_games` | character | JSON-encoded list of the team's most recently completed games, used for form and streak displays. |
| `next_games` | character | JSON-encoded list of the team's next scheduled games. |
| `division_name` | character | Division name. |
| `division_teams` | character | JSON-encoded list of the teams that make up the division. |
| `conference_short_name` | character | Conference short name (e.g. 'ACC'). |
| `conference_name` | character | Full conference name. |
| `conference_conference_id` | character | Yahoo numeric identifier of the conference carried on the team's conference node. |
| `conference_team_standings` | character | JSON-encoded standings rows for every team in the conference. |
| `conference_abbreviation` | character | Conference abbreviation. |
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

### Example

```python
yahoo_playbook_team()
```

_Last validated n/a._

## `yahoo_playbook_team_basic`

Yahoo shangrila persisted query `playbookTeamBasic` -> one row per `teams` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookTeamBasic`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookTeamBasic](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookTeamBasic)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `teamId` | `team_id` |  |  | `Y` | teamId query parameter. |
| `imageHeight` | `image_height` |  |  | `Y` | imageHeight query parameter. |
| `imageWidth` | `image_width` |  |  | `Y` | imageWidth query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `sport_sport_id` | character | Yahoo identifier of the sport the league belongs to. |
| `sport_name` | character | Sport name (e.g., Major League Baseball). |
| `league_name` | character | League name. |
| `league_short_name` | character | League short name. |
| `league_current_season_phase` | character | Phase the league's season is currently in (e.g., "season.phase.season"). |
| `team_id` | character | Unique team identifier. |
| `conference_id` | integer | Conference identifier. |
| `full_name` | character | Player's full name. |
| `display_name` | character | Display name. |
| `location` | character | Either Home if the home team is playing in their home stadium, or Neutral if the game is being played at a neutral location. This still shows as Home for games between the Giants and Jets even though they share the same home stadium. |
| `nickname` | character | Team or athlete nickname. |
| `primary_color` | character | Primary team color (hex). |
| `secondary_color` | character | Secondary team color (hex). |
| `team_logo_white_url` | character | Absolute URL of the team's white knockout logo, the variant used on dark backgrounds. |
| `team_logo_url` | character | Absolute URL of the team's standard logo image on Yahoo's image CDN. |
| `alias_navigation_links` | character | JSON-encoded map of navigation links (scores, standings, teams) hanging off the entity's Yahoo alias. |
| `alias_url` | character | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `alias_path` | character | Site-relative path portion of the entity's Yahoo alias (e.g., "/ncaaf/teams/tcu/"). |
| `last_games` | character | JSON-encoded list of the team's most recently completed games, used for form and streak displays. |
| `next_games` | character | JSON-encoded list of the team's next scheduled games. |
| `division_name` | character | Division name. |
| `conference_short_name` | character | Conference short name (e.g. 'ACC'). |
| `conference_name` | character | Full conference name. |
| `conference_conference_id` | character | Yahoo numeric identifier of the conference carried on the team's conference node. |
| `conference_abbreviation` | character | Conference abbreviation. |
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

### Example

```python
yahoo_playbook_team_basic()
```

_Last validated n/a._

## `yahoo_playbook_team_social_share`

Yahoo shangrila persisted query `playbookTeamSocialShare` -> one row per `teams` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookTeamSocialShare`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookTeamSocialShare](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookTeamSocialShare)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `teamId` | `team_id` |  |  | `Y` | teamId query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `sport_sport_id` | character | Yahoo identifier of the sport the league belongs to. |
| `sport_name` | character | Sport name (e.g., Major League Baseball). |
| `team_id` | character | Unique team identifier. |
| `primary_color` | character | Primary team color (hex). |
| `team_logo_url` | character | Absolute URL of the team's standard logo image on Yahoo's image CDN. |
| `team_logo_white_url` | character | Absolute URL of the team's white knockout logo, the variant used on dark backgrounds. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_playbook_team_social_share()
```

_Last validated n/a._

## `yahoo_playbook_tennis_match`

Yahoo shangrila persisted query `playbookTennisMatch` -> one row per `events` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookTennisMatch`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookTennisMatch](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playbookTennisMatch)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameId` | `game_id` |  |  | `Y` | gameId query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_yahoo_shangrila`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_playbook_tennis_match()
```

_Last validated n/a._

## `yahoo_player_basic`

Yahoo shangrila persisted query `playerBasic` -> tables: players, leagues

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playerBasic`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playerBasic](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playerBasic)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | league query parameter. |
| `playerId` | `player_id` |  |  | `Y` | playerId query parameter. |

### Returns

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (representative columns below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**players**

| col_name | type | description |
|---|---|---|
| `alias_path` | character | Site-relative path portion of the entity's Yahoo alias (e.g., "/ncaaf/teams/tcu/"). |
| `alias_lang` | character | Language/locale tag attached to the entity's Yahoo alias (e.g., "en-US"). |
| `alias_url` | character | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `alias_domain` | character | Host the entity's Yahoo alias resolves against (e.g., "sports.yahoo.com"). |
| `display_name` | character | Display name. |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `player_id` | character | Unique player identifier. |
| `positions` | character | Positions. |
| `team_display_name` | character | Full team display name. |
| `team_team_id` | character | Unique identifier for team team. |
| `uniform_number` | character | Jersey number the player wears for the team. |
| `injury` | character | Injury (body part / description). |

**leagues**

| col_name | type | description |
|---|---|---|
| `current_season` | integer | Season the league is currently playing, as the four-digit starting year. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_player_basic()
```

_Last validated n/a._

## `yahoo_player_career_stats`

Yahoo shangrila persisted query `playerCareerStats` -> one row per `players` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playerCareerStats`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playerCareerStats](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playerCareerStats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `playerId` | `player_id` |  |  | `Y` | playerId query parameter. |
| `seasonPhases` | `season_phases` |  |  | `Y` | seasonPhases query parameter. |
| `footballStatIds` | `football_stat_ids` |  |  | `Y` | footballStatIds query parameter. |
| `basketballStatIds` | `basketball_stat_ids` |  |  | `Y` | basketballStatIds query parameter. |
| `baseballStatIds` | `baseball_stat_ids` |  |  | `Y` | baseballStatIds query parameter. |
| `hockeyStatIds` | `hockey_stat_ids` |  |  | `Y` | hockeyStatIds query parameter. |
| `soccerStatIds` | `soccer_stat_ids` |  |  | `Y` | soccerStatIds query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `positions` | character | Positions. |
| `stats_by_season` | character | JSON-encoded per-season statistical lines for the player. |
| `total_stats` | character | JSON-encoded career-total statistical line summing the player's seasons. |
| `career_stats` | character | JSON-encoded career statistical totals for the player across every season. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_player_career_stats()
```

_Last validated n/a._

## `yahoo_player_game_log`

Yahoo shangrila persisted query `playerGameLog` -> one row per `players` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playerGameLog`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playerGameLog](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playerGameLog)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `playerId` | `player_id` |  |  | `Y` | playerId query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `seasons` | `seasons` |  |  | `Y` | seasons query parameter. |
| `seasonPhases` | `season_phases` |  |  | `Y` | seasonPhases query parameter. |
| `footballStatIds` | `football_stat_ids` |  |  | `Y` | footballStatIds query parameter. |
| `basketballStatIds` | `basketball_stat_ids` |  |  | `Y` | basketballStatIds query parameter. |
| `baseballStatIds` | `baseball_stat_ids` |  |  | `Y` | baseballStatIds query parameter. |
| `hockeyStatIds` | `hockey_stat_ids` |  |  | `Y` | hockeyStatIds query parameter. |
| `soccerStatIds` | `soccer_stat_ids` |  |  | `Y` | soccerStatIds query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_id` | character | Unique player identifier. |
| `active` | logical | TRUE if the row represents an active record (player / team / season). |
| `positions` | character | Positions. |
| `team_id` | character | Unique team identifier. |
| `player_game_stats` | character | JSON-encoded per-game statistical lines for the player across the requested game log. |
| `player_season_stats` | character | JSON-encoded season statistical totals for the player. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_player_game_log()
```

_Last validated n/a._

## `yahoo_player_props`

Yahoo shangrila persisted query `playerProps` -> one row per `players` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playerProps`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playerProps](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playerProps)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `playerId` | `player_id` |  |  | `Y` | playerId query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `games` | character | Games played. |
| `player_id` | character | Unique player identifier. |
| `display_name` | character | Display name. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_player_props()
```

_Last validated n/a._

## `yahoo_player_search`

Yahoo shangrila persisted query `playerSearch` -> one row per `leagues.players` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playerSearch`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playerSearch](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playerSearch)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | league query parameter. |
| `name` | `name` |  |  | `Y` | name query parameter. |
| `onActiveRosterOnly` | `on_active_roster_only` |  |  | `Y` | onActiveRosterOnly query parameter. |
| `nflPositionId` | `nfl_position_id` |  |  | `Y` | nflPositionId query parameter. |
| `nbaPositionId` | `nba_position_id` |  |  | `Y` | nbaPositionId query parameter. |
| `mlbPositionId` | `mlb_position_id` |  |  | `Y` | mlbPositionId query parameter. |
| `nhlPositionId` | `nhl_position_id` |  |  | `Y` | nhlPositionId query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_id` | character | Unique player identifier. |
| `active` | character | TRUE if the row represents an active record (player / team / season). |
| `alias` | character | JSON-encoded Yahoo alias object for the entity, carrying the site URL, path and subpage routing used to build links to its page. |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `display_name` | character | Display name. |
| `suggested_headshot` | character | JSON-encoded image node for the headshot Yahoo recommends for this player. |
| `team` | character | Team-side label or team identifier. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_player_search()
```

_Last validated n/a._

## `yahoo_player_season_stats`

Yahoo shangrila persisted query `playerSeasonStats` -> one row per `players` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playerSeasonStats`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playerSeasonStats](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playerSeasonStats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `playerId` | `player_id` |  |  | `Y` | playerId query parameter. |
| `seasons` | `seasons` |  |  | `Y` | seasons query parameter. |
| `seasonPhases` | `season_phases` |  |  | `Y` | seasonPhases query parameter. |
| `footballStatIds` | `football_stat_ids` |  |  | `Y` | footballStatIds query parameter. |
| `footballCutTypeGroups` | `football_cut_type_groups` |  |  | `Y` | footballCutTypeGroups query parameter. |
| `basketballStatIds` | `basketball_stat_ids` |  |  | `Y` | basketballStatIds query parameter. |
| `basketballCutTypeGroups` | `basketball_cut_type_groups` |  |  | `Y` | basketballCutTypeGroups query parameter. |
| `baseballStatIds` | `baseball_stat_ids` |  |  | `Y` | baseballStatIds query parameter. |
| `baseballCutTypeGroups` | `baseball_cut_type_groups` |  |  | `Y` | baseballCutTypeGroups query parameter. |
| `hockeyStatIds` | `hockey_stat_ids` |  |  | `Y` | hockeyStatIds query parameter. |
| `hockeyCutTypeGroups` | `hockey_cut_type_groups` |  |  | `Y` | hockeyCutTypeGroups query parameter. |
| `groupBySeasonPhase` | `group_by_season_phase` |  |  | `Y` | groupBySeasonPhase query parameter. |
| `usePlayerUniqueId` | `use_player_unique_id` |  |  | `Y` | usePlayerUniqueId query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_id` | character | Unique player identifier. |
| `active` | logical | TRUE if the row represents an active record (player / team / season). |
| `positions` | character | Positions. |
| `team_id` | character | Unique team identifier. |
| `player_season_stats` | character | JSON-encoded season statistical totals for the player. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_player_season_stats()
```

_Last validated n/a._

## `yahoo_playoff_bracket`

Yahoo shangrila persisted query `playoffBracket` -> one row per `leagues.bracketSlots` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playoffBracket`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playoffBracket](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playoffBracket)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | league query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `tournament` | `tournament` |  |  | `Y` | tournament query parameter. |
| `type` | `type` |  |  | `Y` | type query parameter. |
| `playoffRounds` | `playoff_rounds` |  |  | `Y` | playoffRounds query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `conference` | character | Conference name. |
| `id` | character | ID of the player in the 'name' column. |
| `location` | character | Either Home if the home team is playing in their home stadium, or Neutral if the game is being played at a neutral location. This still shows as Home for games between the Giants and Jets even though they share the same home stadium. |
| `playoff_round` | character | Playoff round identifier. |
| `display_order` | character | Position of the bracket slot within its round, controlling top-to-bottom rendering. |
| `season` | character | Season year. |
| `max_games` | character | Maximum number of games the playoff series can run to. |
| `winner_bracket_slot_id` | character | Identifier of the bracket slot the winner of this slot advances into. |
| `playoff_series` | character | JSON-encoded playoff-series node the game belongs to. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_playoff_bracket()
```

_Last validated n/a._

## `yahoo_playoff_series_game`

Yahoo shangrila persisted query `playoffSeriesGame` -> one row per `games` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playoffSeriesGame`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playoffSeriesGame](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playoffSeriesGame)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameId` | `game_id` |  |  | `Y` | gameId query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `playoff_series` | character | JSON-encoded playoff-series node the game belongs to. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_playoff_series_game()
```

_Last validated n/a._

## `yahoo_polymarket_game`

Yahoo shangrila persisted query `polymarketGame` -> one row per `games` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/polymarketGame`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/polymarketGame](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/polymarketGame)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameId` | `game_id` |  |  | `Y` | gameId query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `polymarket_url` | character | Polymarket prediction-market URL for wagering on the game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_polymarket_game()
```

_Last validated n/a._

## `yahoo_racing_schedule`

Yahoo shangrila persisted query `racingSchedule` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/racingSchedule`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/racingSchedule](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/racingSchedule)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | league query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `today` | `today` |  |  | `Y` | today query parameter. |
| `hasSeries` | `has_series` |  |  | `Y` | hasSeries query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `league_seasons` | character | JSON-encoded list of the seasons for which Yahoo carries data for this league. |
| `current_season` | integer | Season the league is currently playing, as the four-digit starting year. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_racing_schedule()
```

_Last validated n/a._

## `yahoo_scoreboard_game`

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

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `display_name` | character | Display name. |
| `league_name` | character | League name. |
| `league_full_name` | character | Full league name (e.g., "NCAA Football"). |
| `league_display_short` | character | Short league label used in navigation and compact UI (e.g., "NCAA FB"). |
| `league_short_name` | character | League short name. |
| `league_sport` | character | Sport the league belongs to (e.g., "football"). |
| `league_alias` | character | JSON-encoded Yahoo alias object for the league, carrying its site URL and path. |
| `league_league_logo` | character | JSON-encoded image node for the league's logo. |
| `partner_url` | character | Partner or affiliate deep link associated with the scoreboard game. |
| `alias_url` | character | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `away_team_id` | character | Unique identifier for the away team. |
| `away_team_full_name` | character | Full away team name (e.g. 'Las Vegas Aces'). |
| `away_team_team_id` | character | Yahoo composite team id of the away team (e.g., "ncaaf.t.29"). |
| `away_team_display_name` | character | Away team full display name; `team_detail = TRUE` only. |
| `away_team_abbreviation` | character | Away team abbreviation; `team_detail = TRUE` only. |
| `away_team_alias` | character | JSON-encoded Yahoo alias object for the away team, carrying its site URL and path. |
| `away_team_nickname` | character | Away team nickname label; `team_detail = TRUE` only. |
| `away_team_team_logo_white` | character | JSON-encoded image node for the away team's white knockout logo, used on dark backgrounds. |
| `away_team_team_logo` | character | JSON-encoded image node for the away team's standard logo. |
| `away_team_team_standings` | character | JSON-encoded standings node for the away team, carrying its record, position and streak. |
| `away_team_rank_polls` | character | JSON-encoded list of the poll rankings the away team currently holds. |
| `away_team_playoff_seeds` | character | JSON-encoded list of the away team's playoff-seed entries for the season. |
| `away_team_record` | character | Away team's win-loss record. |
| `home_team_id` | character | Unique identifier for the home team. |
| `home_team_full_name` | character | Full home team name (e.g. 'Las Vegas Aces'). |
| `home_team_team_id` | character | Yahoo composite team id of the home team (e.g., "ncaaf.t.29"). |
| `home_team_display_name` | character | Home team full display name; `team_detail = TRUE` only. |
| `home_team_abbreviation` | character | Home team abbreviation; `team_detail = TRUE` only. |
| `home_team_alias` | character | JSON-encoded Yahoo alias object for the home team, carrying its site URL and path. |
| `home_team_nickname` | character | Home team nickname label; `team_detail = TRUE` only. |
| `home_team_team_logo_white` | character | JSON-encoded image node for the home team's white knockout logo, used on dark backgrounds. |
| `home_team_team_logo` | character | JSON-encoded image node for the home team's standard logo. |
| `home_team_team_standings` | character | JSON-encoded standings node for the home team, carrying its record, position and streak. |
| `home_team_rank_polls` | character | JSON-encoded list of the poll rankings the home team currently holds. |
| `home_team_playoff_seeds` | character | JSON-encoded list of the home team's playoff-seed entries for the season. |
| `home_team_record` | character | Home team's win-loss record. |
| `current_period_overtime` | character | Flag indicating that the period in progress is an overtime period. |
| `current_period_short_display_name` | character | Abbreviated label for the period in progress (e.g., "4th"). |
| `away_score` | integer | Away team score at the time of the play. |
| `home_score` | integer | Home team score at the time of the play. |
| `start_time` | character | Kickoff time in eastern time zone. |
| `start_date` | character | Start date (YYYY-MM-DD). |
| `if_necessary` | character | If necessary. |
| `status` | character | Status label. |
| `status_display_name` | character | Short game or event status as shown on the scoreboard (e.g., "Final", "12:00 pm ET"). |
| `full_status_display_name` | character | Long-form game status label including overtime and date context (e.g., "Final/OT"). |
| `season` | integer | Season year. |
| `season_phase` | character | Phase of the season the game falls in (e.g., "season.phase.season"). |
| `time_left` | character | Time left. |
| `tournament_id` | character | ESPN tournament identifier. |
| `display_result` | character | Drive-result label (e.g. `Punt`, `Touchdown`). |
| `playoff_series` | character | JSON-encoded playoff-series node the game belongs to. |
| `winning_team_id` | character | Composite Yahoo team id of the side that won the game (e.g., "ncaaf.t.29"). |
| `broadcast_channels` | character | JSON-encoded list of the channels broadcasting the event. |
| `news_break_subtext` | character | Secondary line of the news-break banner attached to the game. |
| `news_break_title` | character | Headline of the news-break banner attached to the game. |
| `news_break_url` | character | URL of the article behind the game's news-break banner. |
| `news_break_uuid` | character | Yahoo content UUID of the article behind the game's news-break banner. |
| `news_break_type` | character | Category of the news-break banner, such as an injury note, preview or recap. |
| `brief` | character | Short editorial blurb summarizing the game's state or result. |
| `event_extended_display_name` | character | Long-form event title used for marquee games, such as a bowl or rivalry name. |
| `special_event_type` | character | Marker identifying a special framing for the game, such as a bowl game or neutral-site showcase. |
| `bets` | character | JSON-encoded list of the betting markets offered on the event (spread, moneyline and total). |
| `venue_display_name` | character | Name of the venue hosting the event. |
| `weather` | character | String describing the weather including temperature, humidity and wind (direction and speed). Doesn't change during the game! |
| `gametime_ticket_url` | character | Gametime affiliate ticket-purchase URL for the event or team. |
| `game_ticket_price` | character | Lowest available ticket price for the game from the Gametime affiliate feed, in US dollars. |
| `teams` | character | Nested list of member-team membership spans. |
| `field_position` | character | Ball spot expressed on Yahoo's 0-100 field scale, measured toward the offense's target goal line. |
| `field_position_display_name` | character | Ball spot rendered the way a scoreboard shows it (e.g., "MICH 35"). |
| `team_possessing_ball` | character | Yahoo team id of the side currently possessing the ball. |
| `week` | integer | Week number. |
| `passing_leader` | character | JSON-encoded leading passer for the game or team, with the statistics that earned the billing. |
| `rushing_leader` | character | JSON-encoded leading rusher for the game or team, with the statistics that earned the billing. |
| `receiving_leader` | character | JSON-encoded leading receiver for the game or team, with the statistics that earned the billing. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_scoreboard_game()
```

_Last validated n/a._

## `yahoo_season_stats_football_defense_ncaaf`

Legacy player season Defense leaders (NCAAF)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonStatsFootballDefenseNcaaf`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonStatsFootballDefenseNcaaf](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonStatsFootballDefenseNcaaf)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `leagueStructure` | `league_structure` |  |  | `Y` | leagueStructure query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `sortStatId` | `sort_stat_id` |  |  | `Y` | sortStatId query parameter. |

### Returns

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (representative columns below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**stat_types**

| col_name | type | description |
|---|---|---|
| `stat_id` | character | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | character | Display name. |
| `abbreviation` | character | Short abbreviation. |
| `sort_order` | character | Display sort order for the sport. |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | character | Full name of the player |
| `player_player_id` | character | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | character | The player's team. |
| `player_positions` | character | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | character | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | character | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | character | Stats. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_season_stats_football_defense_ncaaf()
```

_Last validated n/a._

## `yahoo_season_stats_football_kicking_ncaaf`

Legacy player season Kicking leaders (NCAAF)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonStatsFootballKickingNcaaf`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonStatsFootballKickingNcaaf](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonStatsFootballKickingNcaaf)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `leagueStructure` | `league_structure` |  |  | `Y` | leagueStructure query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `sortStatId` | `sort_stat_id` |  |  | `Y` | sortStatId query parameter. |

### Returns

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (representative columns below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**stat_types**

| col_name | type | description |
|---|---|---|
| `stat_id` | character | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | character | Display name. |
| `abbreviation` | character | Short abbreviation. |
| `sort_order` | character | Display sort order for the sport. |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | character | Full name of the player |
| `player_player_id` | character | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | character | The player's team. |
| `player_positions` | character | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | character | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | character | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | character | Stats. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_season_stats_football_kicking_ncaaf()
```

_Last validated n/a._

## `yahoo_season_stats_football_passing_ncaaf`

Legacy player season Passing leaders (NCAAF)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonStatsFootballPassingNcaaf`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonStatsFootballPassingNcaaf](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonStatsFootballPassingNcaaf)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `leagueStructure` | `league_structure` |  |  | `Y` | leagueStructure query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `sortStatId` | `sort_stat_id` |  |  | `Y` | sortStatId query parameter. |

### Returns

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (representative columns below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**stat_types**

| col_name | type | description |
|---|---|---|
| `stat_id` | character | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | character | Display name. |
| `abbreviation` | character | Short abbreviation. |
| `sort_order` | character | Display sort order for the sport. |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | character | Full name of the player |
| `player_player_id` | character | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | character | The player's team. |
| `player_positions` | character | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | character | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | character | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | character | Stats. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_season_stats_football_passing_ncaaf()
```

_Last validated n/a._

## `yahoo_season_stats_football_punting_ncaaf`

Legacy player season Punting leaders (NCAAF)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonStatsFootballPuntingNcaaf`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonStatsFootballPuntingNcaaf](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonStatsFootballPuntingNcaaf)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `leagueStructure` | `league_structure` |  |  | `Y` | leagueStructure query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `sortStatId` | `sort_stat_id` |  |  | `Y` | sortStatId query parameter. |

### Returns

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (representative columns below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**stat_types**

| col_name | type | description |
|---|---|---|
| `stat_id` | character | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | character | Display name. |
| `abbreviation` | character | Short abbreviation. |
| `sort_order` | character | Display sort order for the sport. |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | character | Full name of the player |
| `player_player_id` | character | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | character | The player's team. |
| `player_positions` | character | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | character | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | character | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | character | Stats. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_season_stats_football_punting_ncaaf()
```

_Last validated n/a._

## `yahoo_season_stats_football_receiving_ncaaf`

Legacy player season Receiving leaders (NCAAF)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonStatsFootballReceivingNcaaf`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonStatsFootballReceivingNcaaf](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonStatsFootballReceivingNcaaf)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `leagueStructure` | `league_structure` |  |  | `Y` | leagueStructure query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `sortStatId` | `sort_stat_id` |  |  | `Y` | sortStatId query parameter. |

### Returns

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (representative columns below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**stat_types**

| col_name | type | description |
|---|---|---|
| `stat_id` | character | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | character | Display name. |
| `abbreviation` | character | Short abbreviation. |
| `sort_order` | character | Display sort order for the sport. |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | character | Full name of the player |
| `player_player_id` | character | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | character | The player's team. |
| `player_positions` | character | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | character | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | character | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | character | Stats. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_season_stats_football_receiving_ncaaf()
```

_Last validated n/a._

## `yahoo_season_stats_football_returns_ncaaf`

Legacy player season Returns leaders (NCAAF)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonStatsFootballReturnsNcaaf`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonStatsFootballReturnsNcaaf](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonStatsFootballReturnsNcaaf)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `leagueStructure` | `league_structure` |  |  | `Y` | leagueStructure query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `sortStatId` | `sort_stat_id` |  |  | `Y` | sortStatId query parameter. |

### Returns

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (representative columns below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**stat_types**

| col_name | type | description |
|---|---|---|
| `stat_id` | character | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | character | Display name. |
| `abbreviation` | character | Short abbreviation. |
| `sort_order` | character | Display sort order for the sport. |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | character | Full name of the player |
| `player_player_id` | character | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | character | The player's team. |
| `player_positions` | character | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | character | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | character | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | character | Stats. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_season_stats_football_returns_ncaaf()
```

_Last validated n/a._

## `yahoo_season_stats_football_rushing_ncaaf`

Legacy player season Rushing leaders (NCAAF)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonStatsFootballRushingNcaaf`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonStatsFootballRushingNcaaf](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonStatsFootballRushingNcaaf)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `leagueStructure` | `league_structure` |  |  | `Y` | leagueStructure query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `sortStatId` | `sort_stat_id` |  |  | `Y` | sortStatId query parameter. |

### Returns

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (representative columns below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**stat_types**

| col_name | type | description |
|---|---|---|
| `stat_id` | character | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | character | Display name. |
| `abbreviation` | character | Short abbreviation. |
| `sort_order` | character | Display sort order for the sport. |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | character | Full name of the player |
| `player_player_id` | character | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | character | The player's team. |
| `player_positions` | character | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | character | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | character | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | character | Stats. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_season_stats_football_rushing_ncaaf()
```

_Last validated n/a._

## `yahoo_season_team_stats_football_defense`

Legacy team season Defense (NCAAF)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballDefense`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballDefense](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballDefense)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `leagueStructure` | `league_structure` |  |  | `Y` | leagueStructure query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `sortStatId` | `sort_stat_id` |  |  | `Y` | sortStatId query parameter. |

### Returns

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (representative columns below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**stat_types**

| col_name | type | description |
|---|---|---|
| `stat_id` | character | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | character | Display name. |
| `abbreviation` | character | Short abbreviation. |
| `sort_order` | character | Display sort order for the sport. |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | character | Full name of the player |
| `player_player_id` | character | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | character | The player's team. |
| `player_positions` | character | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | character | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | character | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | character | Stats. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_season_team_stats_football_defense()
```

_Last validated n/a._

## `yahoo_season_team_stats_football_kicking`

Legacy team season Kicking (NCAAF)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballKicking`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballKicking](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballKicking)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `leagueStructure` | `league_structure` |  |  | `Y` | leagueStructure query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `sortStatId` | `sort_stat_id` |  |  | `Y` | sortStatId query parameter. |

### Returns

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (representative columns below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**stat_types**

| col_name | type | description |
|---|---|---|
| `stat_id` | character | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | character | Display name. |
| `abbreviation` | character | Short abbreviation. |
| `sort_order` | character | Display sort order for the sport. |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | character | Full name of the player |
| `player_player_id` | character | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | character | The player's team. |
| `player_positions` | character | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | character | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | character | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | character | Stats. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_season_team_stats_football_kicking()
```

_Last validated n/a._

## `yahoo_season_team_stats_football_kickoffs`

Legacy team season Kickoffs (NCAAF)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballKickoffs`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballKickoffs](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballKickoffs)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `leagueStructure` | `league_structure` |  |  | `Y` | leagueStructure query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `sortStatId` | `sort_stat_id` |  |  | `Y` | sortStatId query parameter. |

### Returns

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (representative columns below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**stat_types**

| col_name | type | description |
|---|---|---|
| `stat_id` | character | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | character | Display name. |
| `abbreviation` | character | Short abbreviation. |
| `sort_order` | character | Display sort order for the sport. |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | character | Full name of the player |
| `player_player_id` | character | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | character | The player's team. |
| `player_positions` | character | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | character | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | character | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | character | Stats. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_season_team_stats_football_kickoffs()
```

_Last validated n/a._

## `yahoo_season_team_stats_football_offense`

Legacy team season Offense (NCAAF)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballOffense`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballOffense](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballOffense)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `leagueStructure` | `league_structure` |  |  | `Y` | leagueStructure query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `sortStatId` | `sort_stat_id` |  |  | `Y` | sortStatId query parameter. |

### Returns

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (representative columns below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**stat_types**

| col_name | type | description |
|---|---|---|
| `stat_id` | character | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | character | Display name. |
| `abbreviation` | character | Short abbreviation. |
| `sort_order` | character | Display sort order for the sport. |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | character | Full name of the player |
| `player_player_id` | character | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | character | The player's team. |
| `player_positions` | character | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | character | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | character | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | character | Stats. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_season_team_stats_football_offense()
```

_Last validated n/a._

## `yahoo_season_team_stats_football_passing`

Legacy team season Passing (NCAAF)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballPassing`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballPassing](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballPassing)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `leagueStructure` | `league_structure` |  |  | `Y` | leagueStructure query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `sortStatId` | `sort_stat_id` |  |  | `Y` | sortStatId query parameter. |

### Returns

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (representative columns below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**stat_types**

| col_name | type | description |
|---|---|---|
| `stat_id` | character | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | character | Display name. |
| `abbreviation` | character | Short abbreviation. |
| `sort_order` | character | Display sort order for the sport. |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | character | Full name of the player |
| `player_player_id` | character | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | character | The player's team. |
| `player_positions` | character | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | character | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | character | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | character | Stats. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_season_team_stats_football_passing()
```

_Last validated n/a._

## `yahoo_season_team_stats_football_passing_defense`

Legacy team Passing defense allowed (NCAAF)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballPassingDefense`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballPassingDefense](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballPassingDefense)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `leagueStructure` | `league_structure` |  |  | `Y` | leagueStructure query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `sortStatId` | `sort_stat_id` |  |  | `Y` | sortStatId query parameter. |

### Returns

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (representative columns below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**stat_types**

| col_name | type | description |
|---|---|---|
| `stat_id` | character | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | character | Display name. |
| `abbreviation` | character | Short abbreviation. |
| `sort_order` | character | Display sort order for the sport. |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | character | Full name of the player |
| `player_player_id` | character | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | character | The player's team. |
| `player_positions` | character | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | character | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | character | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | character | Stats. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_season_team_stats_football_passing_defense()
```

_Last validated n/a._

## `yahoo_season_team_stats_football_punting`

Legacy team season Punting (NCAAF)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballPunting`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballPunting](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballPunting)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `leagueStructure` | `league_structure` |  |  | `Y` | leagueStructure query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `sortStatId` | `sort_stat_id` |  |  | `Y` | sortStatId query parameter. |

### Returns

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (representative columns below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**stat_types**

| col_name | type | description |
|---|---|---|
| `stat_id` | character | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | character | Display name. |
| `abbreviation` | character | Short abbreviation. |
| `sort_order` | character | Display sort order for the sport. |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | character | Full name of the player |
| `player_player_id` | character | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | character | The player's team. |
| `player_positions` | character | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | character | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | character | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | character | Stats. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_season_team_stats_football_punting()
```

_Last validated n/a._

## `yahoo_season_team_stats_football_receiving`

Legacy team season Receiving (NCAAF)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballReceiving`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballReceiving](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballReceiving)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `leagueStructure` | `league_structure` |  |  | `Y` | leagueStructure query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `sortStatId` | `sort_stat_id` |  |  | `Y` | sortStatId query parameter. |

### Returns

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (representative columns below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**stat_types**

| col_name | type | description |
|---|---|---|
| `stat_id` | character | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | character | Display name. |
| `abbreviation` | character | Short abbreviation. |
| `sort_order` | character | Display sort order for the sport. |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | character | Full name of the player |
| `player_player_id` | character | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | character | The player's team. |
| `player_positions` | character | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | character | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | character | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | character | Stats. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_season_team_stats_football_receiving()
```

_Last validated n/a._

## `yahoo_season_team_stats_football_receiving_defense`

Legacy team Receiving defense allowed (NCAAF)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballReceivingDefense`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballReceivingDefense](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballReceivingDefense)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `leagueStructure` | `league_structure` |  |  | `Y` | leagueStructure query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `sortStatId` | `sort_stat_id` |  |  | `Y` | sortStatId query parameter. |

### Returns

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (representative columns below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**stat_types**

| col_name | type | description |
|---|---|---|
| `stat_id` | character | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | character | Display name. |
| `abbreviation` | character | Short abbreviation. |
| `sort_order` | character | Display sort order for the sport. |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | character | Full name of the player |
| `player_player_id` | character | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | character | The player's team. |
| `player_positions` | character | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | character | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | character | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | character | Stats. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_season_team_stats_football_receiving_defense()
```

_Last validated n/a._

## `yahoo_season_team_stats_football_returns`

Legacy team season Returns (NCAAF)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballReturns`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballReturns](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballReturns)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `leagueStructure` | `league_structure` |  |  | `Y` | leagueStructure query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `sortStatId` | `sort_stat_id` |  |  | `Y` | sortStatId query parameter. |

### Returns

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (representative columns below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**stat_types**

| col_name | type | description |
|---|---|---|
| `stat_id` | character | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | character | Display name. |
| `abbreviation` | character | Short abbreviation. |
| `sort_order` | character | Display sort order for the sport. |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | character | Full name of the player |
| `player_player_id` | character | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | character | The player's team. |
| `player_positions` | character | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | character | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | character | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | character | Stats. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_season_team_stats_football_returns()
```

_Last validated n/a._

## `yahoo_season_team_stats_football_rushing`

Legacy team season Rushing (NCAAF)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballRushing`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballRushing](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballRushing)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `leagueStructure` | `league_structure` |  |  | `Y` | leagueStructure query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `sortStatId` | `sort_stat_id` |  |  | `Y` | sortStatId query parameter. |

### Returns

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (representative columns below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**stat_types**

| col_name | type | description |
|---|---|---|
| `stat_id` | character | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | character | Display name. |
| `abbreviation` | character | Short abbreviation. |
| `sort_order` | character | Display sort order for the sport. |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | character | Full name of the player |
| `player_player_id` | character | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | character | The player's team. |
| `player_positions` | character | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | character | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | character | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | character | Stats. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_season_team_stats_football_rushing()
```

_Last validated n/a._

## `yahoo_season_team_stats_football_rushing_defense`

Legacy team Rushing defense allowed (NCAAF)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballRushingDefense`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballRushingDefense](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/seasonTeamStatsFootballRushingDefense)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `leagueStructure` | `league_structure` |  |  | `Y` | leagueStructure query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `sortStatId` | `sort_stat_id` |  |  | `Y` | sortStatId query parameter. |

### Returns

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (representative columns below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**stat_types**

| col_name | type | description |
|---|---|---|
| `stat_id` | character | Yahoo stat-type key the leader board is built on (e.g., "PASSING_YARDS", "GAMES_RUSHING"). |
| `display_name` | character | Display name. |
| `abbreviation` | character | Short abbreviation. |
| `sort_order` | character | Display sort order for the sport. |

**leagues**

| col_name | type | description |
|---|---|---|
| `player_display_name` | character | Full name of the player |
| `player_player_id` | character | Yahoo composite player id of the leader-board entry (e.g., "ncaaf.p.464024"); always carried as Utf8. |
| `player_team` | character | The player's team. |
| `player_positions` | character | JSON-encoded list of the positions the leader-board entry plays, each with a name, abbreviation and position id (e.g., [{"name": "Quarterback", "abbreviation": "QB", "positionId": "QUARTERBACK"}]). |
| `player_alias` | character | JSON-encoded alias object for the leader-board entry, carrying the Yahoo page URL for that player or team. |
| `player_player_cutout` | character | JSON-encoded image node for the leader-board entry's transparent cut-out portrait. |
| `stats` | character | Stats. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_season_team_stats_football_rushing_defense()
```

_Last validated n/a._

## `yahoo_team_injuries`

Yahoo shangrila persisted query `teamInjuries` -> one row per `teams` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamInjuries`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamInjuries](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamInjuries)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `teamId` | `team_id` |  |  | `Y` | teamId query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `team_id` | character | Unique team identifier. |
| `nickname` | character | Team or athlete nickname. |
| `full_name` | character | Player's full name. |
| `location` | character | Either Home if the home team is playing in their home stadium, or Neutral if the game is being played at a neutral location. This still shows as Home for games between the Giants and Jets even though they share the same home stadium. |
| `display_name` | character | Display name. |
| `primary_color` | character | Primary team color (hex). |
| `abbreviation` | character | Short abbreviation. |
| `alias_path` | character | Site-relative path portion of the entity's Yahoo alias (e.g., "/ncaaf/teams/tcu/"). |
| `alias_url` | character | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `team_logo_white_url` | character | Absolute URL of the team's white knockout logo, the variant used on dark backgrounds. |
| `team_logo_url` | character | Absolute URL of the team's standard logo image on Yahoo's image CDN. |
| `players` | character | Nested list of per-player box scores. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_team_injuries()
```

_Last validated n/a._

## `yahoo_team_playoff_series`

Yahoo shangrila persisted query `teamPlayoffSeries` -> one row per `teams.playoffSeries` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamPlayoffSeries`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamPlayoffSeries](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamPlayoffSeries)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `teamId` | `team_id` |  |  | `Y` | teamId query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_yahoo_shangrila`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_team_playoff_series()
```

_Last validated n/a._

## `yahoo_team_roster`

Yahoo shangrila persisted query `teamRoster` -> one row per `teams` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamRoster`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamRoster](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamRoster)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `teamId` | `team_id` |  |  | `Y` | teamId query parameter. |
| `playerImageHeight` | `player_image_height` |  |  | `Y` | playerImageHeight query parameter. |
| `playerImageWidth` | `player_image_width` |  |  | `Y` | playerImageWidth query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `league_current_season` | character | Yahoo league-season identifier for the league's season currently in progress. |
| `roster` | character | JSON-encoded roster of the players on the team for the requested season. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_team_roster()
```

_Last validated n/a._

## `yahoo_team_schedule_by_season`

Yahoo shangrila persisted query `teamScheduleBySeason` -> one row per `teams` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamScheduleBySeason`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamScheduleBySeason](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamScheduleBySeason)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `teamId` | `team_id` |  |  | `Y` | teamId query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `team_id` | character | Unique team identifier. |
| `display_name` | character | Display name. |
| `primary_color` | character | Primary team color (hex). |
| `secondary_color` | character | Secondary team color (hex). |
| `gametime_ticket_url` | character | Gametime affiliate ticket-purchase URL for the event or team. |
| `bye_weeks` | character | JSON-encoded list of the week numbers in which the team has no scheduled game. |
| `games` | character | Games played. |
| `leagues` | character | JSON-encoded list of the league nodes the team's schedule spans. |
| `full_name` | character | Player's full name. |
| `abbreviation` | character | Short abbreviation. |
| `nickname` | character | Team or athlete nickname. |
| `team_logo_white_url` | character | Absolute URL of the team's white knockout logo, the variant used on dark backgrounds. |
| `team_logo_url` | character | Absolute URL of the team's standard logo image on Yahoo's image CDN. |
| `team_standings_team_record` | character | Formatted overall record for the team (e.g., "8-2"). |
| `alias_url` | character | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_team_schedule_by_season()
```

_Last validated n/a._

## `yahoo_team_search`

Yahoo shangrila persisted query `teamSearch` -> one row per `teams` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamSearch`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamSearch](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamSearch)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `name` | `name` |  |  | `Y` | name query parameter. |
| `imageHeight` | `image_height` |  |  | `Y` | imageHeight query parameter. |
| `imageWidth` | `image_width` |  |  | `Y` | imageWidth query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `team_id` | character | Unique team identifier. |
| `display_name` | character | Display name. |
| `full_name` | character | Player's full name. |
| `alias_url` | character | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `alias_path` | character | Site-relative path portion of the entity's Yahoo alias (e.g., "/ncaaf/teams/tcu/"). |
| `primary_color` | character | Primary team color (hex). |
| `secondary_color` | character | Secondary team color (hex). |
| `abbreviation` | character | Short abbreviation. |
| `league_short_name` | character | League short name. |
| `league_display_short` | character | Short league label used in navigation and compact UI (e.g., "NCAA FB"). |
| `league_name` | character | League name. |
| `team_logo_url` | character | Absolute URL of the team's standard logo image on Yahoo's image CDN. |
| `team_logo_white_url` | character | Absolute URL of the team's white knockout logo, the variant used on dark backgrounds. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_team_search()
```

_Last validated n/a._

## `yahoo_team_stats_leaders_v2`

Yahoo shangrila persisted query `teamStatsLeadersV2` -> tables: leagues, teams

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamStatsLeadersV2`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamStatsLeadersV2](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamStatsLeadersV2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | league query parameter. |
| `teamId` | `team_id` |  |  | `Y` | teamId query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `baseballCutType` | `baseball_cut_type` |  |  | `Y` | baseballCutType query parameter. |
| `qualified` | `qualified` |  |  | `Y` | qualified query parameter. |
| `includeTeamStats` | `include_team_stats` |  |  | `Y` | includeTeamStats query parameter. |
| `includePlayerStats` | `include_player_stats` |  |  | `Y` | includePlayerStats query parameter. |
| `isBaseball` | `is_baseball` |  |  | `Y` | isBaseball query parameter. |

### Returns

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (representative columns below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**teams**

| col_name | type | description |
|---|---|---|
| `display_name` | character | Display name. |
| `full_name` | character | Player's full name. |
| `nickname` | character | Team or athlete nickname. |
| `team_football` | character | JSON-encoded team-level football leader board for the team. |
| `individual_football` | character | JSON-encoded player-level football leader board for the team. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_team_stats_leaders_v2()
```

_Last validated n/a._

## `yahoo_team_transactions`

Yahoo shangrila persisted query `teamTransactions` -> one row per `teams` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamTransactions`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamTransactions](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamTransactions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `teamId` | `team_id` |  |  | `Y` | teamId query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `team_id` | character | Unique team identifier. |
| `nickname` | character | Team or athlete nickname. |
| `full_name` | character | Player's full name. |
| `location` | character | Either Home if the home team is playing in their home stadium, or Neutral if the game is being played at a neutral location. This still shows as Home for games between the Giants and Jets even though they share the same home stadium. |
| `display_name` | character | Display name. |
| `primary_color` | character | Primary team color (hex). |
| `abbreviation` | character | Short abbreviation. |
| `alias_path` | character | Site-relative path portion of the entity's Yahoo alias (e.g., "/ncaaf/teams/tcu/"). |
| `alias_url` | character | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `alias_domain` | character | Host the entity's Yahoo alias resolves against (e.g., "sports.yahoo.com"). |
| `team_logo_white_url` | character | Absolute URL of the team's white knockout logo, the variant used on dark backgrounds. |
| `team_logo_url` | character | Absolute URL of the team's standard logo image on Yahoo's image CDN. |
| `transactions` | character | JSON-encoded list of the team's roster transactions over the requested window. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_team_transactions()
```

_Last validated n/a._

## `yahoo_teams_basic`

Yahoo shangrila persisted query `teamsBasic` -> one row per `teams` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamsBasic`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamsBasic](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamsBasic)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `teamIds` | `team_ids` |  |  | `Y` | teamIds query parameter. |
| `imageHeight` | `image_height` |  |  | `Y` | imageHeight query parameter. |
| `imageWidth` | `image_width` |  |  | `Y` | imageWidth query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `team_id` | character | Unique team identifier. |
| `team_logo_url` | character | Absolute URL of the team's standard logo image on Yahoo's image CDN. |
| `team_logo_white_url` | character | Absolute URL of the team's white knockout logo, the variant used on dark backgrounds. |
| `display_name` | character | Display name. |
| `full_name` | character | Player's full name. |
| `nickname` | character | Team or athlete nickname. |
| `alias_url` | character | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `alias_path` | character | Site-relative path portion of the entity's Yahoo alias (e.g., "/ncaaf/teams/tcu/"). |
| `primary_color` | character | Primary team color (hex). |
| `secondary_color` | character | Secondary team color (hex). |
| `abbreviation` | character | Short abbreviation. |
| `league_display_short` | character | Short league label used in navigation and compact UI (e.g., "NCAA FB"). |
| `league_name` | character | League name. |
| `league_short_name` | character | League short name. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_teams_basic()
```

_Last validated n/a._

## `yahoo_tennis_matches_by_date`

Yahoo shangrila persisted query `tennisMatchesByDate` -> one row per `tennisTournaments` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/tennisMatchesByDate`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/tennisMatchesByDate](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/tennisMatchesByDate)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `tournamentId` | `tournament_id` |  |  | `Y` | tournamentId query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `date` | `date` |  |  | `Y` | date query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `display_name` | character | Display name. |
| `tournament_status` | character | State of the tournament, distinguishing scheduled, in-progress and completed events. |
| `start_time` | character | Kickoff time in eastern time zone. |
| `end_time` | character | Shift end time (MM:SS countdown clock). |
| `events` | character | Nested list of non-game events. |
| `champions` | character | JSON-encoded list of the current champions of the tennis event, one entry per draw. |
| `previous_champions` | character | JSON-encoded list of the champions of the previous edition of the tennis event. |
| `venue_country` | character | Country the venue is located in. |
| `venue_city` | character | Venue city. |
| `venue_state` | character | Venue state / region. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_tennis_matches_by_date()
```

_Last validated n/a._

## `yahoo_tennis_tournament`

Yahoo shangrila persisted query `tennisTournament` -> one row per `tennisTournaments` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/tennisTournament`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/tennisTournament](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/tennisTournament)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `tournamentId` | `tournament_id` |  |  | `Y` | tournamentId query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `display_name` | character | Display name. |
| `tournament_status` | character | State of the tournament, distinguishing scheduled, in-progress and completed events. |
| `start_time` | character | Kickoff time in eastern time zone. |
| `end_time` | character | Shift end time (MM:SS countdown clock). |
| `events` | character | Nested list of non-game events. |
| `champions` | character | JSON-encoded list of the current champions of the tennis event, one entry per draw. |
| `previous_champions` | character | JSON-encoded list of the champions of the previous edition of the tennis event. |
| `venue_country` | character | Country the venue is located in. |
| `venue_city` | character | Venue city. |
| `venue_state` | character | Venue state / region. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_tennis_tournament()
```

_Last validated n/a._

## `yahoo_tennis_tournaments`

Yahoo shangrila persisted query `tennisTournaments` -> one row per `tennisTournaments` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/tennisTournaments`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/tennisTournaments](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/tennisTournaments)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `leagueId` | `league_id` |  |  | `Y` | leagueId query parameter. |
| `matchType` | `match_type` |  |  | `Y` | matchType query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `gender` | character | League gender designation. |
| `event_group_id` | character | Yahoo identifier that groups the rounds or legs making up a single tournament. |
| `display_name` | character | Display name. |
| `match_type` | character | Format of the matches in the tennis draw (e.g., "SINGLES", "DOUBLES"). |
| `surface` | character | What type of ground the game was played on. (Source: Pro-Football-Reference) |
| `start_time` | character | Kickoff time in eastern time zone. |
| `end_time` | character | Shift end time (MM:SS countdown clock). |
| `tournament_status` | character | State of the tournament, distinguishing scheduled, in-progress and completed events. |
| `champions` | character | JSON-encoded list of the current champions of the tennis event, one entry per draw. |
| `alias_path` | character | Site-relative path portion of the entity's Yahoo alias (e.g., "/ncaaf/teams/tcu/"). |
| `alias_lang` | character | Language/locale tag attached to the entity's Yahoo alias (e.g., "en-US"). |
| `alias_url` | character | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `alias_domain` | character | Host the entity's Yahoo alias resolves against (e.g., "sports.yahoo.com"). |
| `previous_champions` | character | JSON-encoded list of the champions of the previous edition of the tennis event. |
| `venue_country` | character | Country the venue is located in. |
| `venue_city` | character | Venue city. |
| `venue_state` | character | Venue state / region. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_tennis_tournaments()
```

_Last validated n/a._

## `yahoo_tennis_tournaments_by_date`

Yahoo shangrila persisted query `tennisTournamentsByDate` -> one row per `tennisTournaments` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/tennisTournamentsByDate`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/tennisTournamentsByDate](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/tennisTournamentsByDate)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `date` | `date` |  |  | `Y` | date query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_yahoo_shangrila`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_tennis_tournaments_by_date()
```

_Last validated n/a._

## `yahoo_trending_event_ids`

Yahoo shangrila persisted query `trendingEventIds` -> one row per `trendingEvents` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/trendingEventIds`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/trendingEventIds](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/trendingEventIds)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `count` | `count` |  |  | `Y` | count query parameter. |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `dateFlipOffset` | `date_flip_offset` |  |  | `Y` | dateFlipOffset query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_yahoo_shangrila`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_trending_event_ids()
```

_Last validated n/a._

## `yahoo_trending_game_ids`

Yahoo shangrila persisted query `trendingGameIds` -> one row per `trendingGames` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/trendingGameIds`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/trendingGameIds](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/trendingGameIds)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `count` | `count` |  |  | `Y` | count query parameter. |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `dateFlipOffset` | `date_flip_offset` |  |  | `Y` | dateFlipOffset query parameter. |
| `dates` | `dates` |  |  | `Y` | Date or date range filter (YYYYMMDD or YYYYMMDD-YYYYMMDD). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_yahoo_shangrila`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_trending_game_ids()
```

_Last validated n/a._

## `yahoo_editorial_boxscore`

Full game box score + play-by-play (normalized stat dictionaries)

**Endpoint URL:** `GET https://api-secure.sports.yahoo.com/v1/editorial/s/boxscore/{game_id}`

**Valid URL:** [https://api-secure.sports.yahoo.com/v1/editorial/s/boxscore/ncaaf.g.202509200023](https://api-secure.sports.yahoo.com/v1/editorial/s/boxscore/ncaaf.g.202509200023)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | game_id path parameter. |
| `v` | `v` |  |  | `Y` | v query parameter. |
| `polling` | `polling` |  |  | `Y` | polling query parameter. |

### Returns

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the feed's id-keyed collections (representative columns below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**player_stats**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `sub_id` | character | Second-level key of an id-keyed editorial collection, present when one entity holds many sub-records — a play id, a scoring-play id, or a stat variation such as "ncaaf.stat_variation.2". |
| `ncaaf_stat_type_102` | character | Value recorded for the "Completions" statistic in Yahoo's Passing category (stat type ncaaf.stat_type.102, abbreviated "Comp"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_103` | character | Value recorded for the "Attempts" statistic in Yahoo's Passing category (stat type ncaaf.stat_type.103, abbreviated "Att"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_105` | character | Value recorded for the "Yards" statistic in Yahoo's Passing category (stat type ncaaf.stat_type.105, abbreviated "Yds"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_104` | character | Value recorded for the "Completion Percentage" statistic in Yahoo's Passing category (stat type ncaaf.stat_type.104, abbreviated "Pct"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_106` | character | Value recorded for the "Yards per Attempt" statistic in Yahoo's Passing category (stat type ncaaf.stat_type.106, abbreviated "Y/A"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_111` | character | Value recorded for the "Sacks" statistic in Yahoo's Passing category (stat type ncaaf.stat_type.111, abbreviated "Sack"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_112` | character | Value recorded for the "Yards Lost" statistic in Yahoo's Passing category (stat type ncaaf.stat_type.112, abbreviated "YdsL"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_108` | character | Value recorded for the "Touchdowns" statistic in Yahoo's Passing category (stat type ncaaf.stat_type.108, abbreviated "TD"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_109` | character | Value recorded for the "Interceptions" statistic in Yahoo's Passing category (stat type ncaaf.stat_type.109, abbreviated "Int"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_113` | character | Value recorded for the "QB Rating" statistic in Yahoo's Passing category (stat type ncaaf.stat_type.113, abbreviated "QBRat"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_202` | character | Value recorded for the "Rushes" statistic in Yahoo's Rushing category (stat type ncaaf.stat_type.202, abbreviated "Rush"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_203` | character | Value recorded for the "Yards" statistic in Yahoo's Rushing category (stat type ncaaf.stat_type.203, abbreviated "Yds"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_205` | character | Value recorded for the "Average" statistic in Yahoo's Rushing category (stat type ncaaf.stat_type.205, abbreviated "Avg"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_206` | character | Value recorded for the "Longest" statistic in Yahoo's Rushing category (stat type ncaaf.stat_type.206, abbreviated "Long"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_207` | character | Value recorded for the "Touchdowns" statistic in Yahoo's Rushing category (stat type ncaaf.stat_type.207, abbreviated "TD"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_302` | character | Value recorded for the "Receptions" statistic in Yahoo's Receiving category (stat type ncaaf.stat_type.302, abbreviated "Rec"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_303` | character | Value recorded for the "Yards" statistic in Yahoo's Receiving category (stat type ncaaf.stat_type.303, abbreviated "Yds"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_305` | character | Value recorded for the "Average" statistic in Yahoo's Receiving category (stat type ncaaf.stat_type.305, abbreviated "Avg"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_306` | character | Value recorded for the "Longest" statistic in Yahoo's Receiving category (stat type ncaaf.stat_type.306, abbreviated "Long"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_309` | character | Value recorded for the "Touchdowns" statistic in Yahoo's Receiving category (stat type ncaaf.stat_type.309, abbreviated "TD"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_502` | character | Value recorded for the "Kickoff Returns" statistic in Yahoo's Returns category (stat type ncaaf.stat_type.502, abbreviated "KR"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_503` | character | Value recorded for the "Yards" statistic in Yahoo's Returns category (stat type ncaaf.stat_type.503, abbreviated "Yds"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_505` | character | Value recorded for the "Average" statistic in Yahoo's Returns category (stat type ncaaf.stat_type.505, abbreviated "Avg"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_506` | character | Value recorded for the "Longest" statistic in Yahoo's Returns category (stat type ncaaf.stat_type.506, abbreviated "Long"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_507` | character | Value recorded for the "Touchdowns" statistic in Yahoo's Returns category (stat type ncaaf.stat_type.507, abbreviated "TD"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_508` | character | Value recorded for the "Punt Returns" statistic in Yahoo's Returns category (stat type ncaaf.stat_type.508, abbreviated "PR"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_509` | character | Value recorded for the "Yards" statistic in Yahoo's Returns category (stat type ncaaf.stat_type.509, abbreviated "Yds"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_511` | character | Value recorded for the "Average" statistic in Yahoo's Returns category (stat type ncaaf.stat_type.511, abbreviated "Avg"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_512` | character | Value recorded for the "Longest" statistic in Yahoo's Returns category (stat type ncaaf.stat_type.512, abbreviated "Long"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_513` | character | Value recorded for the "Touchdowns" statistic in Yahoo's Returns category (stat type ncaaf.stat_type.513, abbreviated "TD"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_411` | character | Value recorded for the "Extra Points Made" statistic in Yahoo's Kicking category (stat type ncaaf.stat_type.411, abbreviated "XPM"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_412` | character | Value recorded for the "Extra Points Attempted" statistic in Yahoo's Kicking category (stat type ncaaf.stat_type.412, abbreviated "XPA"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_407` | character | Value recorded for the "Total Made" statistic in Yahoo's Kicking category (stat type ncaaf.stat_type.407, abbreviated "FGM"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_408` | character | Value recorded for the "Total Attempted" statistic in Yahoo's Kicking category (stat type ncaaf.stat_type.408, abbreviated "FGA"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_410` | character | Value recorded for the "Long" statistic in Yahoo's Kicking category (stat type ncaaf.stat_type.410, abbreviated "Long"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_409` | character | Value recorded for the "Percent" statistic in Yahoo's Kicking category (stat type ncaaf.stat_type.409, abbreviated "Pct"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_602` | character | Value recorded for the "Punts" statistic in Yahoo's Punting category (stat type ncaaf.stat_type.602, abbreviated "Punt"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_604` | character | Value recorded for the "Average" statistic in Yahoo's Punting category (stat type ncaaf.stat_type.604, abbreviated "Avg"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_608` | character | Value recorded for the "Longest" statistic in Yahoo's Punting category (stat type ncaaf.stat_type.608, abbreviated "Long"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_702` | character | Value recorded for the "Solo Tackles" statistic in Yahoo's Defense category (stat type ncaaf.stat_type.702, abbreviated "Solo"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_703` | character | Value recorded for the "Tackle Assists" statistic in Yahoo's Defense category (stat type ncaaf.stat_type.703, abbreviated "Ast"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_705` | character | Value recorded for the "Sacks" statistic in Yahoo's Defense category (stat type ncaaf.stat_type.705, abbreviated "Sack"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_706` | character | Value recorded for the "Yards Lost" statistic in Yahoo's Defense category (stat type ncaaf.stat_type.706, abbreviated "YdsL"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_710` | character | Value recorded for the "Passes Defended" statistic in Yahoo's Defense category (stat type ncaaf.stat_type.710, abbreviated "PD"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_707` | character | Value recorded for the "Interceptions" statistic in Yahoo's Defense category (stat type ncaaf.stat_type.707, abbreviated "Int"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_708` | character | Value recorded for the "Yards" statistic in Yahoo's Defense category (stat type ncaaf.stat_type.708, abbreviated "Yds"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_709` | character | Value recorded for the "Interception Touchdowns" statistic in Yahoo's Defense category (stat type ncaaf.stat_type.709, abbreviated "IntTD"), for the player or team on this boxscore row. |

**team_stats**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `sub_id` | character | Second-level key of an id-keyed editorial collection, present when one entity holds many sub-records — a play id, a scoring-play id, or a stat variation such as "ncaaf.stat_variation.2". |
| `ncaaf_stat_type_919` | character | Value recorded for the "First Downs" statistic in Yahoo's Team category (stat type ncaaf.stat_type.919, abbreviated "Firsts"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_945` | character | Value recorded for the "Total Yards" statistic in Yahoo's Team category (stat type ncaaf.stat_type.945, abbreviated "TOTYDS"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_950` | character | Value recorded for the "Turnovers" statistic in Yahoo's Team category (stat type ncaaf.stat_type.950, abbreviated "TO"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_937` | character | Value recorded for the "Passes for First" statistic in Yahoo's Team category (stat type ncaaf.stat_type.937, abbreviated "PASSF"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_936` | character | Value recorded for the "Rushes for First" statistic in Yahoo's Team category (stat type ncaaf.stat_type.936, abbreviated "RUSHF"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_938` | character | Value recorded for the "Penalties for First" statistic in Yahoo's Team category (stat type ncaaf.stat_type.938, abbreviated "PENF"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_941` | character | Value recorded for the "Third Down Efficiency" statistic in Yahoo's Team category (stat type ncaaf.stat_type.941, abbreviated "3DE"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_944` | character | Value recorded for the "Fourth Down Efficiency" statistic in Yahoo's Team category (stat type ncaaf.stat_type.944, abbreviated "4DE"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_946` | character | Value recorded for the "Total Plays" statistic in Yahoo's Team category (stat type ncaaf.stat_type.946, abbreviated "TOTPLAYS"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_952` | character | Value recorded for the "Avg Gain Per Play" statistic in Yahoo's Team category (stat type ncaaf.stat_type.952, abbreviated "AVGPYDS"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_921` | character | Value recorded for the "Net Yards Rushing" statistic in Yahoo's Team category (stat type ncaaf.stat_type.921, abbreviated "RYDS"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_920` | character | Value recorded for the "Rushes" statistic in Yahoo's Team category (stat type ncaaf.stat_type.920, abbreviated "Rushes"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_949` | character | Value recorded for the "Yards Per Rush" statistic in Yahoo's Team category (stat type ncaaf.stat_type.949, abbreviated "AVGRYDS"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_947` | character | Value recorded for the "Net Yards Passing" statistic in Yahoo's Team category (stat type ncaaf.stat_type.947, abbreviated "NETPYDS"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_951` | character | Value recorded for the "Comp-Att" statistic in Yahoo's Team category (stat type ncaaf.stat_type.951, abbreviated "PASSEFF"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_948` | character | Value recorded for the "Yards Per Pass" statistic in Yahoo's Team category (stat type ncaaf.stat_type.948, abbreviated "AVGPYDS"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_927` | character | Value recorded for the "Times Sacked" statistic in Yahoo's Team category (stat type ncaaf.stat_type.927, abbreviated "SACKS"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_928` | character | Value recorded for the "Yds Lost To Sacks" statistic in Yahoo's Team category (stat type ncaaf.stat_type.928, abbreviated "SACKYD"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_926` | character | Value recorded for the "Interceptions" statistic in Yahoo's Team category (stat type ncaaf.stat_type.926, abbreviated "INTS"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_929` | character | Value recorded for the "Punts" statistic in Yahoo's Team category (stat type ncaaf.stat_type.929, abbreviated "PUNTS"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_930` | character | Value recorded for the "Punt Average" statistic in Yahoo's Team category (stat type ncaaf.stat_type.930, abbreviated "PUNTAVG"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_933` | character | Value recorded for the "Penalties" statistic in Yahoo's Team category (stat type ncaaf.stat_type.933, abbreviated "PEN"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_934` | character | Value recorded for the "Penalty Yards" statistic in Yahoo's Team category (stat type ncaaf.stat_type.934, abbreviated "PENYD"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_931` | character | Value recorded for the "Fumbles" statistic in Yahoo's Team category (stat type ncaaf.stat_type.931, abbreviated "FUMB"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_932` | character | Value recorded for the "Fumbles Lost" statistic in Yahoo's Team category (stat type ncaaf.stat_type.932, abbreviated "FUMBLOST"), for the player or team on this boxscore row. |

**aliases**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `stats` | character | Stats. |

**stat_categories**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `name` | character | Display name. |
| `sort` | character | Yahoo composite stat-type id the category sorts on by default (e.g., "ncaaf.stat_type.105"). |
| `stats` | character | Stats. |

**stat_variations**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `name` | character | Display name. |

**stat_types**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `name` | character | Display name. |
| `short_name` | character | Short display name. |

**stat_cut_types**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `name` | character | Display name. |

**games**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `gameid` | character | Date-encoded Yahoo composite game id for this row (e.g., "ncaaf.g.202509200023"). |
| `global_gameid` | character | Yahoo cross-provider game id, distinct from the date-encoded gameid (e.g., "ncaaf.g.13556882"). |
| `start_time` | character | Kickoff time in eastern time zone. |
| `is_time_tba` | logical | Flag indicating that the scheduled start time has not yet been announced. |
| `season_phase_id` | character | Identifier of the season phase the game falls in (e.g., "season.phase.season"). |
| `game_type` | character | The most recent game type of that season that a player appeared on the roster. |
| `winning_team_id` | character | Composite Yahoo team id of the side that won the game (e.g., "ncaaf.t.29"). |
| `is_rank_upset` | character | Flag indicating that the lower-ranked side won, judged against the teams' poll rankings. |
| `is_spread_upset` | logical | Flag indicating that the winning side was the betting underdog against the closing spread. |
| `outcome_type` | character | Outcome classification for a completed game (e.g., "outcome.type.won", "outcome.type.tied"). |
| `home_team_id` | character | Unique identifier for the home team. |
| `away_team_id` | character | Unique identifier for the away team. |
| `week_number` | character | Week number. |
| `sportacular_url` | character | Deep link into the Yahoo Sportacular mobile app for this game (a "ysportacular://" URL). |
| `status_display_name` | character | Short game or event status as shown on the scoreboard (e.g., "Final", "12:00 pm ET"). |
| `status_description` | character | Roster status description (e.g. 'Active'). |
| `status_type` | character | Status type. |
| `total_away_points` | character | Points scored by the away team in the game. |
| `current_period_id` | character | Ordinal number of the period currently in progress, counting from 1. |
| `total_home_points` | character | Points scored by the home team in the game. |
| `total_away_shootout_points` | character | Shootout goals converted by the away team, populated only for sports that break ties by shootout. |
| `total_home_shootout_points` | character | Shootout goals converted by the home team, populated only for sports that break ties by shootout. |
| `home_team_stats` | character | JSON-encoded team-stat block for the home team, populated once the game is under way. |
| `away_team_stats` | character | JSON-encoded team-stat block for the away team, populated once the game is under way. |
| `game_period_balls` | character | Balls in the count for the at-bat in progress; baseball only. |
| `game_period_strikes` | character | Strikes in the count for the at-bat in progress; baseball only. |
| `game_period_outs` | character | Outs recorded so far in the current half-inning; baseball only. |
| `yards_to_endzone` | character | Distance from the current ball spot to the opponent's goal line, in yards. |
| `start_yardline` | character | Yard line at the drive start. |
| `distance` | character | Distance value (in feet for shot data; otherwise context-dependent). |
| `down` | character | The down for the given play. |
| `team_in_possession` | character | Yahoo team id of the side currently in possession of the ball. |
| `power_play_strength_home` | character | Number of skaters the home team has on the ice during special-teams play; hockey only. |
| `power_play_strength_away` | character | Number of skaters the away team has on the ice during special-teams play; hockey only. |
| `game_time_elapsed` | character | Playing time elapsed in the game, in seconds. |
| `game_time_elapsed_display` | character | Playing time elapsed formatted for display (e.g., "67:12"), used by sports whose clock counts up. |
| `inning_status` | character | Half-inning indicator for a game in progress (e.g., "Top", "Bottom"); baseball only. |
| `away_timeouts` | character | Away-team timeouts remaining. |
| `home_timeouts` | character | Home-team timeouts remaining. |
| `is_halftime` | character | Flag indicating that the game is currently stopped at halftime. |
| `minimum_periods` | integer | Number of periods a game of this sport runs before overtime is required (4 for football, 9 for baseball). |
| `game_periods` | character | JSON-encoded list of the game's period nodes, each carrying a period number and its display names. |
| `baserunners` | character | JSON-encoded baserunner occupancy for the game in progress; baseball only. |
| `season` | character | Season year. |
| `subleague` | character | Sub-league the game belongs to, for leagues split into constituent circuits. |
| `subleague_display_name` | character | Display name of the sub-league the game belongs to. |
| `agg_score` | character | Aggregate score across the legs of a two-leg tie, populated only for competitions decided on aggregate. |
| `leg_number` | character | Ordinal of this leg within a multi-leg tie, counting from 1. |
| `tv_coverage` | character | Network carrying the game, as a short broadcast abbreviation (e.g., "CBS", "ESPN"). |
| `seatgeek_id` | character | SeatGeek performer or event identifier used to build the ticket-purchase link. |
| `last_updated` | character | Last-updated timestamp. |
| `teams` | character | Nested list of member-team membership spans. |
| `play_by_play` | character | JSON-encoded data-island pointer to the game's play-by-play collection in the same editorial payload. |
| `pitches` | character | JSON-encoded data-island pointer to the game's pitch-level feed; baseball only. |
| `at_bat` | character | JSON-encoded data-island pointer to the game's current at-bat feed; baseball only. |
| `penalty_summary` | character | Whether penalty summary data is available. |
| `scoring_summary` | character | Whether scoring summary data is available. |
| `stat_categories` | character | JSON-encoded pointer to the stat-category dictionary that groups this feed's statistics. |
| `stadium` | character | Name of the stadium |
| `stadium_id` | character | ID of the stadium the game was played in. (Source: Pro-Football-Reference) |
| `stadium_image` | character | JSON-encoded data-island pointer to the venue photograph used on the game page. |
| `attendance` | character | Reported attendance. |
| `lineups` | character | JSON-encoded data-island pointer to the game's lineup collection. |
| `top_performer` | character | JSON-encoded data-island pointer to the game's top-performing players. |
| `players` | character | Nested list of per-player box scores. |
| `byline` | character | News article byline / author. |
| `highlight` | character | JSON-encoded data-island pointer to the game's highlight video. |
| `highlights` | character | Game highlight urls. |
| `live_video` | character | JSON-encoded data-island pointer to the live video stream for the game. |
| `odds` | character | JSON-encoded data-island pointer to the game's odds collection. |
| `current_players` | character | JSON-encoded data-island pointer to the players currently on the field, ice or court. |
| `last_play` | character | Free-text description of the most recent play. |
| `series_type` | character | JSON-encoded data-island pointer to the kind of series the game belongs to. |
| `series_status` | character | JSON-encoded data-island pointer to the current state of the series the game belongs to. |
| `games` | character | Games played. |
| `series_games` | character | JSON-encoded data-island pointer to the games making up the series. |
| `game_details` | character | JSON-encoded data-island pointer to supplementary detail notes for the game. |
| `section_notes` | character | JSON-encoded data-island pointer to editorial section notes attached to the game page. |
| `articles` | character | JSON-encoded data-island pointer to the editorial articles attached to the game. |
| `tweets` | character | JSON-encoded data-island pointer to the social posts attached to the game page. |
| `playoff_round` | character | Playoff round identifier. |
| `media_stream` | character | JSON-encoded data-island pointer to the game's media-stream collection. |
| `playoff_series_status` | character | JSON-encoded data-island pointer to the current state of the playoff series the game belongs to. |
| `playoff_series_details` | character | JSON-encoded data-island pointer to detail about the playoff series the game belongs to. |
| `drives` | character | JSON-encoded data-island pointer to the game's drive collection; football only. |
| `user_teams_game` | character | JSON-encoded data-island pointer to the viewer's followed-team context for the game. |
| `page_metadata` | character | JSON-encoded data-island pointer to the SEO and page metadata for the entity. |
| `penalty_box` | character | JSON-encoded data-island pointer to the game's penalty-box feed; hockey only. |
| `starting_pitchers` | character | JSON-encoded data-island pointer to the game's announced starting pitchers; baseball only. |
| `unrestricted_streams` | character | JSON-encoded data-island pointer to the streams viewable without a subscription. |
| `tv_details` | character | JSON-encoded list of broadcast entries for the game, each carrying a network abbreviation and full channel name (e.g., [{"abbr": "NBC", "name": "NBC/Peacock"}]). |
| `away_seed` | character | Away team's seed. |
| `home_seed` | character | Home team's seed. |
| `navigation_links_tickets_url` | character | Affiliate ticket-purchase URL for the game, pointing at the SeatGeek marketplace. |
| `navigation_links_boxscore_url` | character | Site-relative URL of the game's boxscore page on sports.yahoo.com. |
| `navigation_links_match_page_url` | character | Site-relative URL of the game's match page on sports.yahoo.com. |
| `navigation_links_recap_url` | character | Site-relative URL of the editorial recap article written for the game. |
| `navigation_links_league_home_url` | character | Site-relative URL of the league's home page on sports.yahoo.com. |
| `navigation_links_league_scores_url` | character | Site-relative URL of the league's scoreboard page on sports.yahoo.com. |
| `provider_coverage_score_update_frequency_in_minutes` | character | How often, in minutes, the data provider refreshes the score for this game. |
| `provider_coverage_has_plays` | character | Flag indicating that the data provider supplies play-by-play for this game. |
| `provider_coverage_has_stats` | character | Flag indicating that the data provider supplies box-score statistics for this game. |
| `provider_coverage_has_extended_stats` | character | Flag indicating that the data provider supplies extended statistics beyond the standard box score. |
| `provider_coverage_has_final_stats` | character | Flag indicating that the data provider has published final, official statistics for the game. |

**gameplayoff_round**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `value` | character | Numeric or string value field. |

**gameplayoff_series_status**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `value` | character | Numeric or string value field. |

**gameplayoff_series_details**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `value` | character | Numeric or string value field. |

**gamescore**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `value` | character | Numeric or string value field. |

**gamecurrent_players**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `current_batter` | character | Yahoo composite player id of the batter at the plate; baseball only. |
| `current_pitcher` | character | Yahoo composite player id of the pitcher on the mound; baseball only. |
| `base_runners` | character | JSON-encoded list of runners currently occupying bases; baseball only. |
| `due_ups` | character | JSON-encoded list of the batters due up next; baseball only. |

**gamelast_play**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `play_id` | character | Numeric play id that when used with game_id and drive provides the unique identifier for a single play. |
| `play_type` | character | String indicating the type of play: pass (includes sacks), run (includes scrambles), punt, field_goal, kickoff, extra_point, qb_kneel, qb_spike, no_play (timeouts and penalties), and missing for rows indicating end of play. |
| `play_text` | character | Free-form text description of the play from the CFBD feed. |
| `period` | character | Period of the game (1-4 quarters; 5+ for OT). |
| `clock` | character | Game clock value. |
| `team` | character | Team-side label or team identifier. |
| `is_scoring_play` | integer | Flag indicating that the play put points on the board (1 = scoring play, 0 = not). |

**gamepenalty_box**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `value` | character | Numeric or string value field. |

**gameplay_by_play**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `sub_id` | character | Second-level key of an id-keyed editorial collection, present when one entity holds many sub-records — a play id, a scoring-play id, or a stat variation such as "ncaaf.stat_variation.2". |
| `play_id` | character | Numeric play id that when used with game_id and drive provides the unique identifier for a single play. |
| `period` | character | Period of the game (1-4 quarters; 5+ for OT). |
| `clock` | character | Game clock value. |
| `down` | character | The down for the given play. |
| `distance` | character | Distance value (in feet for shot data; otherwise context-dependent). |
| `team` | character | Team-side label or team identifier. |
| `yardline` | character | Ball spot at the snap as rendered on the scoreboard (e.g., "MICH 35"). |
| `yards_to_endzone` | character | Distance from the current ball spot to the opponent's goal line, in yards. |
| `type` | character | Record type / category. |
| `yards` | character | Total yards gained on the drive. |
| `text` | character | Text description of the play / record. |
| `play_time` | character | Wall-clock instant the play was recorded, as a Unix epoch timestamp in seconds. |

**gameat_bat**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `value` | character | Numeric or string value field. |

**gamescoring_summary**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `sub_id` | character | Second-level key of an id-keyed editorial collection, present when one entity holds many sub-records — a play id, a scoring-play id, or a stat variation such as "ncaaf.stat_variation.2". |
| `play_id` | character | Numeric play id that when used with game_id and drive provides the unique identifier for a single play. |
| `period` | character | Period of the game (1-4 quarters; 5+ for OT). |
| `clock` | character | Game clock value. |
| `away_score` | character | Away team score at the time of the play. |
| `home_score` | character | Home team score at the time of the play. |
| `team` | character | Team-side label or team identifier. |
| `score_type` | character | Kind of score produced by the scoring play (e.g., "TD" touchdown, "FG" field goal, "SF" safety). |
| `xp_type` | character | Conversion attempted after the touchdown ("EP" for an extra point, "2PT" for a two-point try, "0" when none was attempted). |
| `players` | character | Nested list of per-player box scores. |
| `text` | character | Text description of the play / record. |

**gamemedia_stream**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `media_type` | character | Kind of item carried in the media stream (e.g., "play", "video"). |
| `media_source` | character | Feed the media item was produced from (e.g., "play_by_play"). |
| `sequence_id` | integer | Monotonic sequence number that orders items within the game's media stream. |
| `external_id` | character | Provider-side identifier for the media item, matching the play id it accompanies. |
| `timestamp` | character | Response timestamp (ISO 8601). |
| `official` | logical | Flag indicating that the media item comes from the official league feed rather than an editorial source. |

**gamedrives**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `id` | character | ID of the player in the 'name' column. |
| `team` | character | Team-side label or team identifier. |
| `time` | character | Time at start of play provided in string format as minutes:seconds remaining in the quarter. |
| `num_plays` | character | Number of plays the offense ran on the drive. |
| `yards_covered` | character | Net yards the offense gained over the course of the drive. |
| `start_yardline` | character | Yard line at the drive start. |
| `yardline_text` | character | Ball spot where the drive started, rendered as it appears on the scoreboard (e.g., "NEB 25"). |
| `plays` | character | Total qualifying passing plays included in the WEPA calculation. |
| `result` | character | Result. |
| `start_time_clock` | character | Game clock reading when the drive began, as MM:SS remaining in its period. |
| `start_time_period` | character | Period number in which the drive began. |
| `end_time_clock` | character | Game clock reading when the drive ended, as MM:SS remaining in its period. |
| `end_time_period` | integer | Period number in which the drive ended. |

**gamelineups**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `home_lineup_order` | character | JSON-encoded batting or lineup order for the home team, empty for sports without a fixed order. |
| `away_lineup_order` | character | JSON-encoded batting or lineup order for the away team, empty for sports without a fixed order. |
| `home_lineup_all_ncaaf_p_457863_player_id` | character | Yahoo composite player id ncaaf.p.457863, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_333433_player_id` | character | Yahoo composite player id ncaaf.p.333433, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_461405_player_id` | character | Yahoo composite player id ncaaf.p.461405, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_457834_player_id` | character | Yahoo composite player id ncaaf.p.457834, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_469568_player_id` | character | Yahoo composite player id ncaaf.p.469568, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_333434_player_id` | character | Yahoo composite player id ncaaf.p.333434, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_333286_player_id` | character | Yahoo composite player id ncaaf.p.333286, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_404124_player_id` | character | Yahoo composite player id ncaaf.p.404124, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_322801_player_id` | character | Yahoo composite player id ncaaf.p.322801, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_457875_player_id` | character | Yahoo composite player id ncaaf.p.457875, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_469559_player_id` | character | Yahoo composite player id ncaaf.p.469559, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_322827_player_id` | character | Yahoo composite player id ncaaf.p.322827, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_451229_player_id` | character | Yahoo composite player id ncaaf.p.451229, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_471801_player_id` | character | Yahoo composite player id ncaaf.p.471801, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_299700_player_id` | character | Yahoo composite player id ncaaf.p.299700, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_322308_player_id` | character | Yahoo composite player id ncaaf.p.322308, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_322802_player_id` | character | Yahoo composite player id ncaaf.p.322802, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_333347_player_id` | character | Yahoo composite player id ncaaf.p.333347, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_334637_player_id` | character | Yahoo composite player id ncaaf.p.334637, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_404052_player_id` | character | Yahoo composite player id ncaaf.p.404052, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_406379_player_id` | character | Yahoo composite player id ncaaf.p.406379, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_457838_player_id` | character | Yahoo composite player id ncaaf.p.457838, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_457853_player_id` | character | Yahoo composite player id ncaaf.p.457853, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_457880_player_id` | character | Yahoo composite player id ncaaf.p.457880, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_461399_player_id` | character | Yahoo composite player id ncaaf.p.461399, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_469436_player_id` | character | Yahoo composite player id ncaaf.p.469436, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_403962_player_id` | character | Yahoo composite player id ncaaf.p.403962, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_404392_player_id` | character | Yahoo composite player id ncaaf.p.404392, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_457591_player_id` | character | Yahoo composite player id ncaaf.p.457591, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_323602_player_id` | character | Yahoo composite player id ncaaf.p.323602, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_340496_player_id` | character | Yahoo composite player id ncaaf.p.340496, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_403924_player_id` | character | Yahoo composite player id ncaaf.p.403924, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_457613_player_id` | character | Yahoo composite player id ncaaf.p.457613, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_333922_player_id` | character | Yahoo composite player id ncaaf.p.333922, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_404008_player_id` | character | Yahoo composite player id ncaaf.p.404008, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_338368_player_id` | character | Yahoo composite player id ncaaf.p.338368, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_457602_player_id` | character | Yahoo composite player id ncaaf.p.457602, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_327406_player_id` | character | Yahoo composite player id ncaaf.p.327406, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_327412_player_id` | character | Yahoo composite player id ncaaf.p.327412, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_327421_player_id` | character | Yahoo composite player id ncaaf.p.327421, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_333312_player_id` | character | Yahoo composite player id ncaaf.p.333312, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_333324_player_id` | character | Yahoo composite player id ncaaf.p.333324, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_333423_player_id` | character | Yahoo composite player id ncaaf.p.333423, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_340505_player_id` | character | Yahoo composite player id ncaaf.p.340505, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_403634_player_id` | character | Yahoo composite player id ncaaf.p.403634, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_403958_player_id` | character | Yahoo composite player id ncaaf.p.403958, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_404304_player_id` | character | Yahoo composite player id ncaaf.p.404304, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_405415_player_id` | character | Yahoo composite player id ncaaf.p.405415, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_405652_player_id` | character | Yahoo composite player id ncaaf.p.405652, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_451303_player_id` | character | Yahoo composite player id ncaaf.p.451303, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_457606_player_id` | character | Yahoo composite player id ncaaf.p.457606, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_474363_player_id` | character | Yahoo composite player id ncaaf.p.474363, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_474366_player_id` | character | Yahoo composite player id ncaaf.p.474366, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_editorial_boxscore(game_id='ncaaf.g.202509200023')
```

_Last validated n/a._

## `yahoo_editorial_scoreboard`

Scoreboard: games + teams + leagues + odds (fat payload)

**Endpoint URL:** `GET https://api-secure.sports.yahoo.com/v1/editorial/s/scoreboard`

**Valid URL:** [https://api-secure.sports.yahoo.com/v1/editorial/s/scoreboard](https://api-secure.sports.yahoo.com/v1/editorial/s/scoreboard)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `leagues` | `leagues` |  |  | `Y` | leagues query parameter. |
| `week` | `week` |  |  | `Y` | Week number within the season. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `conferences` | `conferences` |  |  | `Y` | conferences query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `v` | `v` |  |  | `Y` | v query parameter. |

### Returns

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the feed's id-keyed collections (representative columns below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**games**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `gameid` | character | Date-encoded Yahoo composite game id for this row (e.g., "ncaaf.g.202509200023"). |
| `global_gameid` | character | Yahoo cross-provider game id, distinct from the date-encoded gameid (e.g., "ncaaf.g.13556882"). |
| `start_time` | character | Kickoff time in eastern time zone. |
| `is_time_tba` | logical | Flag indicating that the scheduled start time has not yet been announced. |
| `season_phase_id` | character | Identifier of the season phase the game falls in (e.g., "season.phase.season"). |
| `game_type` | character | The most recent game type of that season that a player appeared on the roster. |
| `winning_team_id` | character | Composite Yahoo team id of the side that won the game (e.g., "ncaaf.t.29"). |
| `is_rank_upset` | character | Flag indicating that the lower-ranked side won, judged against the teams' poll rankings. |
| `is_spread_upset` | character | Flag indicating that the winning side was the betting underdog against the closing spread. |
| `outcome_type` | character | Outcome classification for a completed game (e.g., "outcome.type.won", "outcome.type.tied"). |
| `home_team_id` | character | Unique identifier for the home team. |
| `away_team_id` | character | Unique identifier for the away team. |
| `week_number` | character | Week number. |
| `sportacular_url` | character | Deep link into the Yahoo Sportacular mobile app for this game (a "ysportacular://" URL). |
| `status_display_name` | character | Short game or event status as shown on the scoreboard (e.g., "Final", "12:00 pm ET"). |
| `status_description` | character | Roster status description (e.g. 'Active'). |
| `status_type` | character | Status type. |
| `total_away_points` | character | Points scored by the away team in the game. |
| `current_period_id` | character | Ordinal number of the period currently in progress, counting from 1. |
| `total_home_points` | character | Points scored by the home team in the game. |
| `total_away_shootout_points` | character | Shootout goals converted by the away team, populated only for sports that break ties by shootout. |
| `total_home_shootout_points` | character | Shootout goals converted by the home team, populated only for sports that break ties by shootout. |
| `home_team_stats` | character | JSON-encoded team-stat block for the home team, populated once the game is under way. |
| `away_team_stats` | character | JSON-encoded team-stat block for the away team, populated once the game is under way. |
| `game_period_balls` | character | Balls in the count for the at-bat in progress; baseball only. |
| `game_period_strikes` | character | Strikes in the count for the at-bat in progress; baseball only. |
| `game_period_outs` | character | Outs recorded so far in the current half-inning; baseball only. |
| `yards_to_endzone` | character | Distance from the current ball spot to the opponent's goal line, in yards. |
| `start_yardline` | character | Yard line at the drive start. |
| `distance` | character | Distance value (in feet for shot data; otherwise context-dependent). |
| `down` | character | The down for the given play. |
| `team_in_possession` | character | Yahoo team id of the side currently in possession of the ball. |
| `power_play_strength_home` | character | Number of skaters the home team has on the ice during special-teams play; hockey only. |
| `power_play_strength_away` | character | Number of skaters the away team has on the ice during special-teams play; hockey only. |
| `game_time_elapsed` | character | Playing time elapsed in the game, in seconds. |
| `game_time_elapsed_display` | character | Playing time elapsed formatted for display (e.g., "67:12"), used by sports whose clock counts up. |
| `inning_status` | character | Half-inning indicator for a game in progress (e.g., "Top", "Bottom"); baseball only. |
| `away_timeouts` | character | Away-team timeouts remaining. |
| `home_timeouts` | character | Home-team timeouts remaining. |
| `is_halftime` | character | Flag indicating that the game is currently stopped at halftime. |
| `minimum_periods` | character | Number of periods a game of this sport runs before overtime is required (4 for football, 9 for baseball). |
| `game_periods` | character | JSON-encoded list of the game's period nodes, each carrying a period number and its display names. |
| `baserunners` | character | JSON-encoded baserunner occupancy for the game in progress; baseball only. |
| `season` | character | Season year. |
| `subleague` | character | Sub-league the game belongs to, for leagues split into constituent circuits. |
| `subleague_display_name` | character | Display name of the sub-league the game belongs to. |
| `agg_score` | character | Aggregate score across the legs of a two-leg tie, populated only for competitions decided on aggregate. |
| `leg_number` | character | Ordinal of this leg within a multi-leg tie, counting from 1. |
| `tv_coverage` | character | Network carrying the game, as a short broadcast abbreviation (e.g., "CBS", "ESPN"). |
| `seatgeek_id` | character | SeatGeek performer or event identifier used to build the ticket-purchase link. |
| `last_updated` | logical | Last-updated timestamp. |
| `teams` | character | Nested list of member-team membership spans. |
| `play_by_play` | character | JSON-encoded data-island pointer to the game's play-by-play collection in the same editorial payload. |
| `pitches` | character | JSON-encoded data-island pointer to the game's pitch-level feed; baseball only. |
| `at_bat` | character | JSON-encoded data-island pointer to the game's current at-bat feed; baseball only. |
| `penalty_summary` | character | Whether penalty summary data is available. |
| `scoring_summary` | character | Whether scoring summary data is available. |
| `stat_categories` | character | JSON-encoded pointer to the stat-category dictionary that groups this feed's statistics. |
| `stadium` | character | Name of the stadium |
| `stadium_id` | character | ID of the stadium the game was played in. (Source: Pro-Football-Reference) |
| `stadium_image` | character | JSON-encoded data-island pointer to the venue photograph used on the game page. |
| `attendance` | character | Reported attendance. |
| `lineups` | character | JSON-encoded data-island pointer to the game's lineup collection. |
| `top_performer` | character | JSON-encoded data-island pointer to the game's top-performing players. |
| `players` | character | Nested list of per-player box scores. |
| `byline` | character | News article byline / author. |
| `highlight` | character | JSON-encoded data-island pointer to the game's highlight video. |
| `highlights` | character | Game highlight urls. |
| `live_video` | character | JSON-encoded data-island pointer to the live video stream for the game. |
| `odds` | character | JSON-encoded data-island pointer to the game's odds collection. |
| `current_players` | character | JSON-encoded data-island pointer to the players currently on the field, ice or court. |
| `last_play` | character | Free-text description of the most recent play. |
| `series_type` | character | JSON-encoded data-island pointer to the kind of series the game belongs to. |
| `series_status` | character | JSON-encoded data-island pointer to the current state of the series the game belongs to. |
| `games` | character | Games played. |
| `series_games` | character | JSON-encoded data-island pointer to the games making up the series. |
| `game_details` | character | JSON-encoded data-island pointer to supplementary detail notes for the game. |
| `section_notes` | character | JSON-encoded data-island pointer to editorial section notes attached to the game page. |
| `articles` | character | JSON-encoded data-island pointer to the editorial articles attached to the game. |
| `tweets` | character | JSON-encoded data-island pointer to the social posts attached to the game page. |
| `playoff_round` | character | Playoff round identifier. |
| `media_stream` | character | JSON-encoded data-island pointer to the game's media-stream collection. |
| `playoff_series_status` | character | JSON-encoded data-island pointer to the current state of the playoff series the game belongs to. |
| `playoff_series_details` | character | JSON-encoded data-island pointer to detail about the playoff series the game belongs to. |
| `drives` | character | JSON-encoded data-island pointer to the game's drive collection; football only. |
| `user_teams_game` | character | JSON-encoded data-island pointer to the viewer's followed-team context for the game. |
| `page_metadata` | character | JSON-encoded data-island pointer to the SEO and page metadata for the entity. |
| `penalty_box` | character | JSON-encoded data-island pointer to the game's penalty-box feed; hockey only. |
| `starting_pitchers` | character | JSON-encoded data-island pointer to the game's announced starting pitchers; baseball only. |
| `unrestricted_streams` | character | JSON-encoded data-island pointer to the streams viewable without a subscription. |
| `tv_details` | character | JSON-encoded list of broadcast entries for the game, each carrying a network abbreviation and full channel name (e.g., [{"abbr": "NBC", "name": "NBC/Peacock"}]). |
| `away_seed` | character | Away team's seed. |
| `home_seed` | character | Home team's seed. |
| `navigation_links_tickets_url` | character | Affiliate ticket-purchase URL for the game, pointing at the SeatGeek marketplace. |
| `navigation_links_boxscore_url` | character | Site-relative URL of the game's boxscore page on sports.yahoo.com. |
| `navigation_links_match_page_url` | character | Site-relative URL of the game's match page on sports.yahoo.com. |
| `navigation_links_league_home_url` | character | Site-relative URL of the league's home page on sports.yahoo.com. |
| `navigation_links_league_scores_url` | character | Site-relative URL of the league's scoreboard page on sports.yahoo.com. |
| `provider_coverage_score_update_frequency_in_minutes` | character | How often, in minutes, the data provider refreshes the score for this game. |
| `provider_coverage_has_plays` | character | Flag indicating that the data provider supplies play-by-play for this game. |
| `provider_coverage_has_stats` | character | Flag indicating that the data provider supplies box-score statistics for this game. |
| `provider_coverage_has_extended_stats` | character | Flag indicating that the data provider supplies extended statistics beyond the standard box score. |
| `provider_coverage_has_final_stats` | character | Flag indicating that the data provider has published final, official statistics for the game. |

**gameplayoff_round**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `value` | character | Numeric or string value field. |

**gameplayoff_series_status**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `value` | character | Numeric or string value field. |

**gameplayoff_series_details**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `value` | character | Numeric or string value field. |

**teams**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `team_id` | character | Unique team identifier. |
| `display_name` | character | Display name. |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `full_name` | character | Player's full name. |
| `abbr` | character | Short team abbreviation used on the scoreboard (e.g., "TCU", "UNC"). |
| `division_id` | character | Division MLBAM ID. |
| `division` | character | Team division. |
| `division_abbr` | character | Division abbreviation. |
| `subdivision_id` | character | Yahoo numeric identifier of the subdivision the team competes in. |
| `subdivision` | character | Name of the subdivision the team competes in, typically a conference division (e.g., "East Division"). |
| `conference_id` | character | Conference identifier. |
| `conference_abbr` | character | Conference abbreviation. |
| `conference_seed` | character | Seed the team holds within its conference for playoff purposes. |
| `conference` | character | Conference name. |
| `seatgeek_id` | character | SeatGeek performer or event identifier used to build the ticket-purchase link. |
| `sportacular_logo` | character | Data-island pointer to the team's Sportacular-app logo, JSON-encoded as ["teamsportacularLogo", <team id>]. |
| `sportacular_logo_dark` | character | Data-island pointer to the team's dark-mode Sportacular-app logo, JSON-encoded as ["teamsportacularLogoDark", <team id>]. |
| `logo` | character | Team or league logo URL. |
| `logo_dark` | character | Dark-mode logo URL. |
| `color_primary` | character | Data-island pointer to the team's primary brand color, JSON-encoded as ["teamcolorPrimary", <team id>]. |
| `color_secondary` | character | Data-island pointer to the team's secondary brand color, JSON-encoded as ["teamColorSecondary", <team id>]. |
| `record` | character | Team win-loss record for the season. |
| `players` | character | Nested list of per-player box scores. |
| `rankings` | character | Data-island pointer to the team's poll rankings, JSON-encoded as ["teamrankings", <team id>]. |
| `stat_categories` | character | JSON-encoded pointer to the stat-category dictionary that groups this feed's statistics. |
| `page_metadata` | character | JSON-encoded data-island pointer to the SEO and page metadata for the entity. |
| `team_home_link` | character | Site-relative URL of the team's home page (e.g., "/ncaaf/teams/tcu"). |
| `team_schedule_link` | character | Site-relative URL of the team's schedule page (e.g., "/ncaaf/teams/tcu/schedule"). |
| `conference_position` | character | Rank of the team within its conference standings; empty when the league does not order by conference. |
| `group_position` | character | Rank of the team within its scoreboard grouping; an empty string for leagues that do not group. |
| `playoff_seed` | character | Current playoff seed. |

**teamsportacular_logo**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `value` | character | Numeric or string value field. |

**teamsportacular_logo_dark**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `value` | character | Numeric or string value field. |

**team_logo**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `value` | character | Numeric or string value field. |

**team_logo_dark**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `value` | character | Numeric or string value field. |

**teamrecord**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `value` | character | Numeric or string value field. |

**teamrankings**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `sub_id` | character | Second-level key of an id-keyed editorial collection, present when one entity holds many sub-records — a play id, a scoring-play id, or a stat variation such as "ncaaf.stat_variation.2". |
| `date` | character | Date in YYYY-MM-DD format. |
| `poll_key` | character | Yahoo composite poll id the ranking row was taken from (e.g., "ncaaf.poll.9"). |
| `rank` | character | Position of the school within the poll for the given week (1 = top-ranked). |
| `previous_rank` | character | Team's rank in the prior release of this poll. |
| `points` | character | Points scored. |
| `source` | character | News source. |
| `primary` | character | Flag indicating that this poll is the one shown as the team's headline ranking. |
| `relevant` | character | Flag indicating that the poll ranking is currently relevant enough to display. |
| `teams` | character | Nested list of member-team membership spans. |

**leagues**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `league_id` | character | League identifier ('10' = WNBA). |
| `name` | character | Display name. |
| `display_name` | character | Display name. |
| `link` | character | API link to the game feed. |
| `season_year` | character | Season year string ('YYYY-YY' format). |
| `season_display_year` | character | Season year shown in the league's scoreboard header, as a four-digit year. |
| `season_week_number` | integer | Week number the league feed is currently anchored on. |
| `season_season_current_date` | character | Date the league feed treats as "today", in YYYY-MM-DD form. |
| `season_season_next_real_game_date` | character | Date of the next non-exhibition game on the league calendar, in YYYY-MM-DD form. |
| `season_season_next_game_date` | character | Date of the next scheduled game on the league calendar, in YYYY-MM-DD form. |
| `season_season_month` | character | Two-digit calendar month the league feed is currently anchored on. |
| `season_display_schedule_period` | character | Season phase the scoreboard is currently displaying (e.g., "season.phase.offseason"). |
| `season_display_schedule_period_id` | character | Numeric identifier of the season phase the scoreboard is currently displaying. |
| `season_current_phase` | character | Phase the league season is currently in, as Yahoo's season-phase key. |
| `season_current_sched_state` | character | Numeric scheduling state matching the current phase (2 regular season, 3 postseason, 4 offseason). |
| `season_suspended` | character | Flag indicating that league play is currently suspended (1 = suspended, 0 = normal). |
| `season_current_stat_state_season` | integer | Season the stats graph is currently serving statistics for, as a four-digit year. |
| `season_current_stat_state_week` | character | Week the stats graph is currently serving statistics for. |
| `season_current_stat_state_graphite_phase` | character | Phase key the stats graph is currently serving statistics for (e.g., "REGULAR_SEASON"). |
| `season_phases_2_phase_id` | character | Season-phase key Yahoo assigns the league's regular season phase (e.g., "season.phase.season"). |
| `season_phases_2_name` | character | Display label Yahoo gives the league's regular season phase. |
| `season_phases_2_sched_state` | character | Numeric scheduling state Yahoo assigns the league's regular season phase (2). |
| `season_phases_2_phase_start` | character | Start of the league's regular season phase as an RFC 1123 timestamp, or "TBD" when Yahoo has not fixed it yet. |
| `season_phases_2_phase_end` | character | End of the league's regular season phase as an RFC 1123 timestamp, or "TBD" when Yahoo has not fixed it yet. |
| `season_phases_2_phase_start_week` | character | Week number the league's regular season phase begins on, or false when the phase is not organised into weeks. |
| `season_phases_2_phase_end_week` | character | Week number the league's regular season phase ends on, or false when the phase is not organised into weeks. |
| `season_phases_2_structures` | character | JSON-encoded pointer to the league structures (divisions and conferences) that apply during the regular season phase. |
| `season_phases_3_phase_id` | character | Season-phase key Yahoo assigns the league's post season phase (e.g., "season.phase.postseason"). |
| `season_phases_3_name` | character | Display label Yahoo gives the league's post season phase. |
| `season_phases_3_sched_state` | character | Numeric scheduling state Yahoo assigns the league's post season phase (3). |
| `season_phases_3_phase_start` | character | Start of the league's post season phase as an RFC 1123 timestamp, or "TBD" when Yahoo has not fixed it yet. |
| `season_phases_3_phase_end` | character | End of the league's post season phase as an RFC 1123 timestamp, or "TBD" when Yahoo has not fixed it yet. |
| `season_phases_3_phase_start_week` | logical | Week number the league's post season phase begins on, or false when the phase is not organised into weeks. |
| `season_phases_3_phase_end_week` | logical | Week number the league's post season phase ends on, or false when the phase is not organised into weeks. |
| `season_phases_3_structures` | character | JSON-encoded pointer to the league structures (divisions and conferences) that apply during the post season phase. |
| `season_phases_4_phase_id` | character | Season-phase key Yahoo assigns the league's off season phase (e.g., "season.phase.offseason"). |
| `season_phases_4_name` | character | Display label Yahoo gives the league's off season phase. |
| `season_phases_4_sched_state` | character | Numeric scheduling state Yahoo assigns the league's off season phase (4). |
| `season_phases_4_phase_start` | character | Start of the league's off season phase as an RFC 1123 timestamp, or "TBD" when Yahoo has not fixed it yet. |
| `season_phases_4_phase_end` | character | End of the league's off season phase as an RFC 1123 timestamp, or "TBD" when Yahoo has not fixed it yet. |
| `season_phases_4_phase_start_week` | logical | Week number the league's off season phase begins on, or false when the phase is not organised into weeks. |
| `season_phases_4_phase_end_week` | logical | Week number the league's off season phase ends on, or false when the phase is not organised into weeks. |
| `season_phases_4_structures` | character | JSON-encoded pointer to the league structures (divisions and conferences) that apply during the off season phase. |

**divisions**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `sub_id` | character | Second-level key of an id-keyed editorial collection, present when one entity holds many sub-records — a play id, a scoring-play id, or a stat variation such as "ncaaf.stat_variation.2". |
| `id` | integer | ID of the player in the 'name' column. |
| `name` | character | Display name. |
| `type` | character | Record type / category. |
| `conferences_1_id` | numeric | Yahoo numeric conference id carried for the Atlantic Coast conference in the league's division structure. |
| `conferences_1_name` | character | Conference name carried under Yahoo conference key 1, the Atlantic Coast conference. |
| `conferences_1_type` | character | Structure level of Yahoo conference key 1 (Atlantic Coast); "conference" for a full conference rather than a division within one. |
| `conferences_4_id` | numeric | Yahoo numeric conference id carried for the Big Ten conference in the league's division structure. |
| `conferences_4_name` | character | Conference name carried under Yahoo conference key 4, the Big Ten conference. |
| `conferences_4_type` | character | Structure level of Yahoo conference key 4 (Big Ten); "conference" for a full conference rather than a division within one. |
| `conferences_6_id` | numeric | Yahoo numeric conference id carried for the Mid-American conference in the league's division structure. |
| `conferences_6_name` | character | Conference name carried under Yahoo conference key 6, the Mid-American conference. |
| `conferences_6_type` | character | Structure level of Yahoo conference key 6 (Mid-American); "conference" for a full conference rather than a division within one. |
| `conferences_7_id` | numeric | Yahoo numeric conference id carried for the Pac-12 conference in the league's division structure. |
| `conferences_7_name` | character | Conference name carried under Yahoo conference key 7, the Pac-12 conference. |
| `conferences_7_type` | character | Structure level of Yahoo conference key 7 (Pac-12); "conference" for a full conference rather than a division within one. |
| `conferences_8_id` | numeric | Yahoo numeric conference id carried for the SEC conference in the league's division structure. |
| `conferences_8_name` | character | Conference name carried under Yahoo conference key 8, the SEC conference. |
| `conferences_8_type` | character | Structure level of Yahoo conference key 8 (SEC); "conference" for a full conference rather than a division within one. |
| `conferences_11_id` | numeric | Yahoo numeric conference id carried for the Independents (FBS) conference in the league's division structure. |
| `conferences_11_name` | character | Conference name carried under Yahoo conference key 11, the Independents (FBS) conference. |
| `conferences_11_type` | character | Structure level of Yahoo conference key 11 (Independents (FBS)); "conference" for a full conference rather than a division within one. |
| `conferences_71_id` | numeric | Yahoo numeric conference id carried for the Big 12 conference in the league's division structure. |
| `conferences_71_name` | character | Conference name carried under Yahoo conference key 71, the Big 12 conference. |
| `conferences_71_type` | character | Structure level of Yahoo conference key 71 (Big 12); "conference" for a full conference rather than a division within one. |
| `conferences_72_id` | numeric | Yahoo numeric conference id carried for the Conference USA conference in the league's division structure. |
| `conferences_72_name` | character | Conference name carried under Yahoo conference key 72, the Conference USA conference. |
| `conferences_72_type` | character | Structure level of Yahoo conference key 72 (Conference USA); "conference" for a full conference rather than a division within one. |
| `conferences_87_id` | numeric | Yahoo numeric conference id carried for the Mountain West conference in the league's division structure. |
| `conferences_87_name` | character | Conference name carried under Yahoo conference key 87, the Mountain West conference. |
| `conferences_87_type` | character | Structure level of Yahoo conference key 87 (Mountain West); "conference" for a full conference rather than a division within one. |
| `conferences_90_id` | numeric | Yahoo numeric conference id carried for the Sun Belt conference in the league's division structure. |
| `conferences_90_name` | character | Conference name carried under Yahoo conference key 90, the Sun Belt conference. |
| `conferences_90_type` | character | Structure level of Yahoo conference key 90 (Sun Belt); "conference" for a full conference rather than a division within one. |
| `conferences_90_subdivisions_1_id` | numeric | Yahoo numeric subdivision id for the East Division of the Sun Belt conference. |
| `conferences_90_subdivisions_1_name` | character | Name of subdivision 1 within the Sun Belt conference, the East Division. |
| `conferences_90_subdivisions_1_type` | character | Structure level of subdivision 1 within the Sun Belt conference; "subdivision" for a division inside a conference. |
| `conferences_90_subdivisions_2_id` | numeric | Yahoo numeric subdivision id for the West Division of the Sun Belt conference. |
| `conferences_90_subdivisions_2_name` | character | Name of subdivision 2 within the Sun Belt conference, the West Division. |
| `conferences_90_subdivisions_2_type` | character | Structure level of subdivision 2 within the Sun Belt conference; "subdivision" for a division inside a conference. |
| `conferences_122_id` | numeric | Yahoo numeric conference id carried for the American Athletic conference in the league's division structure. |
| `conferences_122_name` | character | Conference name carried under Yahoo conference key 122, the American Athletic conference. |
| `conferences_122_type` | character | Structure level of Yahoo conference key 122 (American Athletic); "conference" for a full conference rather than a division within one. |
| `conferences_13_id` | numeric | Yahoo numeric conference id carried for the Big Sky conference in the league's division structure. |
| `conferences_13_name` | character | Conference name carried under Yahoo conference key 13, the Big Sky conference. |
| `conferences_13_type` | character | Structure level of Yahoo conference key 13 (Big Sky); "conference" for a full conference rather than a division within one. |
| `conferences_14_id` | numeric | Yahoo numeric conference id carried for the Missouri Valley conference in the league's division structure. |
| `conferences_14_name` | character | Conference name carried under Yahoo conference key 14, the Missouri Valley conference. |
| `conferences_14_type` | character | Structure level of Yahoo conference key 14 (Missouri Valley); "conference" for a full conference rather than a division within one. |
| `conferences_15_id` | numeric | Yahoo numeric conference id carried for the Ivy League conference in the league's division structure. |
| `conferences_15_name` | character | Conference name carried under Yahoo conference key 15, the Ivy League conference. |
| `conferences_15_type` | character | Structure level of Yahoo conference key 15 (Ivy League); "conference" for a full conference rather than a division within one. |
| `conferences_17_id` | numeric | Yahoo numeric conference id carried for the Mid-Eastern Athletic conference in the league's division structure. |
| `conferences_17_name` | character | Conference name carried under Yahoo conference key 17, the Mid-Eastern Athletic conference. |
| `conferences_17_type` | character | Structure level of Yahoo conference key 17 (Mid-Eastern Athletic); "conference" for a full conference rather than a division within one. |
| `conferences_19_id` | numeric | Yahoo numeric conference id carried for the Patriot League conference in the league's division structure. |
| `conferences_19_name` | character | Conference name carried under Yahoo conference key 19, the Patriot League conference. |
| `conferences_19_type` | character | Structure level of Yahoo conference key 19 (Patriot League); "conference" for a full conference rather than a division within one. |
| `conferences_20_id` | numeric | Yahoo numeric conference id carried for the Pioneer League conference in the league's division structure. |
| `conferences_20_name` | character | Conference name carried under Yahoo conference key 20, the Pioneer League conference. |
| `conferences_20_type` | character | Structure level of Yahoo conference key 20 (Pioneer League); "conference" for a full conference rather than a division within one. |
| `conferences_21_id` | numeric | Yahoo numeric conference id carried for the Southern conference in the league's division structure. |
| `conferences_21_name` | character | Conference name carried under Yahoo conference key 21, the Southern conference. |
| `conferences_21_type` | character | Structure level of Yahoo conference key 21 (Southern); "conference" for a full conference rather than a division within one. |
| `conferences_22_id` | numeric | Yahoo numeric conference id carried for the Southland conference in the league's division structure. |
| `conferences_22_name` | character | Conference name carried under Yahoo conference key 22, the Southland conference. |
| `conferences_22_type` | character | Structure level of Yahoo conference key 22 (Southland); "conference" for a full conference rather than a division within one. |
| `conferences_23_id` | numeric | Yahoo numeric conference id carried for the Southwestern Athletic conference in the league's division structure. |
| `conferences_23_name` | character | Conference name carried under Yahoo conference key 23, the Southwestern Athletic conference. |
| `conferences_23_type` | character | Structure level of Yahoo conference key 23 (Southwestern Athletic); "conference" for a full conference rather than a division within one. |
| `conferences_23_subdivisions_1_id` | numeric | Yahoo numeric subdivision id for the East Division of the Southwestern Athletic conference. |
| `conferences_23_subdivisions_1_name` | character | Name of subdivision 1 within the Southwestern Athletic conference, the East Division. |
| `conferences_23_subdivisions_1_type` | character | Structure level of subdivision 1 within the Southwestern Athletic conference; "subdivision" for a division inside a conference. |
| `conferences_23_subdivisions_2_id` | numeric | Yahoo numeric subdivision id for the West Division of the Southwestern Athletic conference. |
| `conferences_23_subdivisions_2_name` | character | Name of subdivision 2 within the Southwestern Athletic conference, the West Division. |
| `conferences_23_subdivisions_2_type` | character | Structure level of subdivision 2 within the Southwestern Athletic conference; "subdivision" for a division inside a conference. |
| `conferences_73_id` | numeric | Yahoo numeric conference id carried for the Northeast conference in the league's division structure. |
| `conferences_73_name` | character | Conference name carried under Yahoo conference key 73, the Northeast conference. |
| `conferences_73_type` | character | Structure level of Yahoo conference key 73 (Northeast); "conference" for a full conference rather than a division within one. |
| `conferences_74_id` | numeric | Yahoo numeric conference id carried for the Independents (FCS) conference in the league's division structure. |
| `conferences_74_name` | character | Conference name carried under Yahoo conference key 74, the Independents (FCS) conference. |
| `conferences_74_type` | character | Structure level of Yahoo conference key 74 (Independents (FCS)); "conference" for a full conference rather than a division within one. |
| `conferences_98_id` | numeric | Yahoo numeric conference id carried for the Coastal Athletic conference in the league's division structure. |
| `conferences_98_name` | character | Conference name carried under Yahoo conference key 98, the Coastal Athletic conference. |
| `conferences_98_type` | character | Structure level of Yahoo conference key 98 (Coastal Athletic); "conference" for a full conference rather than a division within one. |
| `conferences_282_id` | numeric | Yahoo numeric conference id carried for the United Athletic Conference conference in the league's division structure. |
| `conferences_282_name` | character | Conference name carried under Yahoo conference key 282, the United Athletic Conference conference. |
| `conferences_282_type` | character | Structure level of Yahoo conference key 282 (United Athletic Conference); "conference" for a full conference rather than a division within one. |
| `conferences_283_id` | numeric | Yahoo numeric conference id carried for the Big South-OVC conference in the league's division structure. |
| `conferences_283_name` | character | Conference name carried under Yahoo conference key 283, the Big South-OVC conference. |
| `conferences_283_type` | character | Structure level of Yahoo conference key 283 (Big South-OVC); "conference" for a full conference rather than a division within one. |
| `conferences_25_id` | numeric | Yahoo numeric conference id carried for the CIAA conference in the league's division structure. |
| `conferences_25_name` | character | Conference name carried under Yahoo conference key 25, the CIAA conference. |
| `conferences_25_type` | character | Structure level of Yahoo conference key 25 (CIAA); "conference" for a full conference rather than a division within one. |
| `conferences_27_id` | numeric | Yahoo numeric conference id carried for the Gulf South conference in the league's division structure. |
| `conferences_27_name` | character | Conference name carried under Yahoo conference key 27, the Gulf South conference. |
| `conferences_27_type` | character | Structure level of Yahoo conference key 27 (Gulf South); "conference" for a full conference rather than a division within one. |
| `conferences_28_id` | numeric | Yahoo numeric conference id carried for the Lone Star conference in the league's division structure. |
| `conferences_28_name` | character | Conference name carried under Yahoo conference key 28, the Lone Star conference. |
| `conferences_28_type` | character | Structure level of Yahoo conference key 28 (Lone Star); "conference" for a full conference rather than a division within one. |
| `conferences_29_id` | numeric | Yahoo numeric conference id carried for the Mid-America Intercollegiate Athletics Association conference in the league's division structure. |
| `conferences_29_name` | character | Conference name carried under Yahoo conference key 29, the Mid-America Intercollegiate Athletics Association conference. |
| `conferences_29_type` | character | Structure level of Yahoo conference key 29 (Mid-America Intercollegiate Athletics Association); "conference" for a full conference rather than a division within one. |
| `conferences_33_id` | numeric | Yahoo numeric conference id carried for the Northern Sun Intercollegiate conference in the league's division structure. |
| `conferences_33_name` | character | Conference name carried under Yahoo conference key 33, the Northern Sun Intercollegiate conference. |
| `conferences_33_type` | character | Structure level of Yahoo conference key 33 (Northern Sun Intercollegiate); "conference" for a full conference rather than a division within one. |
| `conferences_34_id` | numeric | Yahoo numeric conference id carried for the Pennsylvania State Athletic Conference conference in the league's division structure. |
| `conferences_34_name` | character | Conference name carried under Yahoo conference key 34, the Pennsylvania State Athletic Conference conference. |
| `conferences_34_type` | character | Structure level of Yahoo conference key 34 (Pennsylvania State Athletic Conference); "conference" for a full conference rather than a division within one. |
| `conferences_35_id` | numeric | Yahoo numeric conference id carried for the Rocky Mountain Athletic conference in the league's division structure. |
| `conferences_35_name` | character | Conference name carried under Yahoo conference key 35, the Rocky Mountain Athletic conference. |
| `conferences_35_type` | character | Structure level of Yahoo conference key 35 (Rocky Mountain Athletic); "conference" for a full conference rather than a division within one. |
| `conferences_36_id` | numeric | Yahoo numeric conference id carried for the South Atlantic conference in the league's division structure. |
| `conferences_36_name` | character | Conference name carried under Yahoo conference key 36, the South Atlantic conference. |
| `conferences_36_type` | character | Structure level of Yahoo conference key 36 (South Atlantic); "conference" for a full conference rather than a division within one. |
| `conferences_37_id` | numeric | Yahoo numeric conference id carried for the Southern Intercollegiate Athletic conference in the league's division structure. |
| `conferences_37_name` | character | Conference name carried under Yahoo conference key 37, the Southern Intercollegiate Athletic conference. |
| `conferences_37_type` | character | Structure level of Yahoo conference key 37 (Southern Intercollegiate Athletic); "conference" for a full conference rather than a division within one. |
| `conferences_97_id` | numeric | Yahoo numeric conference id carried for the Great Lakes Intercollegiate Athletic conference in the league's division structure. |
| `conferences_97_name` | character | Conference name carried under Yahoo conference key 97, the Great Lakes Intercollegiate Athletic conference. |
| `conferences_97_type` | character | Structure level of Yahoo conference key 97 (Great Lakes Intercollegiate Athletic); "conference" for a full conference rather than a division within one. |
| `conferences_99_id` | numeric | Yahoo numeric conference id carried for the Northeast 10 conference in the league's division structure. |
| `conferences_99_name` | character | Conference name carried under Yahoo conference key 99, the Northeast 10 conference. |
| `conferences_99_type` | character | Structure level of Yahoo conference key 99 (Northeast 10); "conference" for a full conference rather than a division within one. |
| `conferences_102_id` | numeric | Yahoo numeric conference id carried for the Atlantic Central Conference conference in the league's division structure. |
| `conferences_102_name` | character | Conference name carried under Yahoo conference key 102, the Atlantic Central Conference conference. |
| `conferences_102_type` | character | Structure level of Yahoo conference key 102 (Atlantic Central Conference); "conference" for a full conference rather than a division within one. |
| `conferences_107_id` | numeric | Yahoo numeric conference id carried for the Great Northwest Athletic Conference conference in the league's division structure. |
| `conferences_107_name` | character | Conference name carried under Yahoo conference key 107, the Great Northwest Athletic Conference conference. |
| `conferences_107_type` | character | Structure level of Yahoo conference key 107 (Great Northwest Athletic Conference); "conference" for a full conference rather than a division within one. |
| `conferences_121_id` | numeric | Yahoo numeric conference id carried for the Great American Conference conference in the league's division structure. |
| `conferences_121_name` | character | Conference name carried under Yahoo conference key 121, the Great American Conference conference. |
| `conferences_121_type` | character | Structure level of Yahoo conference key 121 (Great American Conference); "conference" for a full conference rather than a division within one. |
| `conferences_123_id` | numeric | Yahoo numeric conference id carried for the Great Midwest Athletic Conference conference in the league's division structure. |
| `conferences_123_name` | character | Conference name carried under Yahoo conference key 123, the Great Midwest Athletic Conference conference. |
| `conferences_123_type` | character | Structure level of Yahoo conference key 123 (Great Midwest Athletic Conference); "conference" for a full conference rather than a division within one. |
| `conferences_127_id` | numeric | Yahoo numeric conference id carried for the Great Lakes Valley Conference conference in the league's division structure. |
| `conferences_127_name` | character | Conference name carried under Yahoo conference key 127, the Great Lakes Valley Conference conference. |
| `conferences_127_type` | character | Structure level of Yahoo conference key 127 (Great Lakes Valley Conference); "conference" for a full conference rather than a division within one. |
| `conferences_128_id` | numeric | Yahoo numeric conference id carried for the Mountain East Conference conference in the league's division structure. |
| `conferences_128_name` | character | Conference name carried under Yahoo conference key 128, the Mountain East Conference conference. |
| `conferences_128_type` | character | Structure level of Yahoo conference key 128 (Mountain East Conference); "conference" for a full conference rather than a division within one. |
| `conferences_285_id` | numeric | Yahoo numeric conference id carried for the Conference Carolinas conference in the league's division structure. |
| `conferences_285_name` | character | Conference name carried under Yahoo conference key 285, the Conference Carolinas conference. |
| `conferences_285_type` | character | Structure level of Yahoo conference key 285 (Conference Carolinas); "conference" for a full conference rather than a division within one. |
| `conferences_26_id` | numeric | Yahoo numeric conference id carried for the Eastern Collegiate conference in the league's division structure. |
| `conferences_26_name` | character | Conference name carried under Yahoo conference key 26, the Eastern Collegiate conference. |
| `conferences_26_type` | character | Structure level of Yahoo conference key 26 (Eastern Collegiate); "conference" for a full conference rather than a division within one. |
| `conferences_30_id` | numeric | Yahoo numeric conference id carried for the Midwest Intercollegiate conference in the league's division structure. |
| `conferences_30_name` | character | Conference name carried under Yahoo conference key 30, the Midwest Intercollegiate conference. |
| `conferences_30_type` | character | Structure level of Yahoo conference key 30 (Midwest Intercollegiate); "conference" for a full conference rather than a division within one. |
| `conferences_41_id` | numeric | Yahoo numeric conference id carried for the Centennial Football conference in the league's division structure. |
| `conferences_41_name` | character | Conference name carried under Yahoo conference key 41, the Centennial Football conference. |
| `conferences_41_type` | character | Structure level of Yahoo conference key 41 (Centennial Football); "conference" for a full conference rather than a division within one. |
| `conferences_46_id` | numeric | Yahoo numeric conference id carried for the Michigan Intercollegiate Athletic Association conference in the league's division structure. |
| `conferences_46_name` | character | Conference name carried under Yahoo conference key 46, the Michigan Intercollegiate Athletic Association conference. |
| `conferences_46_type` | character | Structure level of Yahoo conference key 46 (Michigan Intercollegiate Athletic Association); "conference" for a full conference rather than a division within one. |
| `conferences_49_id` | numeric | Yahoo numeric conference id carried for the Minnesota Intercollegiate Athletic conference in the league's division structure. |
| `conferences_49_name` | character | Conference name carried under Yahoo conference key 49, the Minnesota Intercollegiate Athletic conference. |
| `conferences_49_type` | character | Structure level of Yahoo conference key 49 (Minnesota Intercollegiate Athletic); "conference" for a full conference rather than a division within one. |
| `conferences_51_id` | numeric | Yahoo numeric conference id carried for the New England Football conference in the league's division structure. |
| `conferences_51_name` | character | Conference name carried under Yahoo conference key 51, the New England Football conference. |
| `conferences_51_type` | character | Structure level of Yahoo conference key 51 (New England Football); "conference" for a full conference rather than a division within one. |
| `conferences_53_id` | numeric | Yahoo numeric conference id carried for the North Coast Athletic conference in the league's division structure. |
| `conferences_53_name` | character | Conference name carried under Yahoo conference key 53, the North Coast Athletic conference. |
| `conferences_53_type` | character | Structure level of Yahoo conference key 53 (North Coast Athletic); "conference" for a full conference rather than a division within one. |
| `conferences_55_id` | numeric | Yahoo numeric conference id carried for the Old Dominion Athletic conference in the league's division structure. |
| `conferences_55_name` | character | Conference name carried under Yahoo conference key 55, the Old Dominion Athletic conference. |
| `conferences_55_type` | character | Structure level of Yahoo conference key 55 (Old Dominion Athletic); "conference" for a full conference rather than a division within one. |
| `conferences_56_id` | numeric | Yahoo numeric conference id carried for the Presidents' Athletic conference in the league's division structure. |
| `conferences_56_name` | character | Conference name carried under Yahoo conference key 56, the Presidents' Athletic conference. |
| `conferences_56_type` | character | Structure level of Yahoo conference key 56 (Presidents' Athletic); "conference" for a full conference rather than a division within one. |
| `conferences_57_id` | numeric | Yahoo numeric conference id carried for the Southern California Intercollegiate Athletic conference in the league's division structure. |
| `conferences_57_name` | character | Conference name carried under Yahoo conference key 57, the Southern California Intercollegiate Athletic conference. |
| `conferences_57_type` | character | Structure level of Yahoo conference key 57 (Southern California Intercollegiate Athletic); "conference" for a full conference rather than a division within one. |
| `conferences_76_id` | numeric | Yahoo numeric conference id carried for the Independents (III) conference in the league's division structure. |
| `conferences_76_name` | character | Conference name carried under Yahoo conference key 76, the Independents (III) conference. |
| `conferences_76_type` | character | Structure level of Yahoo conference key 76 (Independents (III)); "conference" for a full conference rather than a division within one. |
| `conferences_79_id` | numeric | Yahoo numeric conference id carried for the Liberty conference in the league's division structure. |
| `conferences_79_name` | character | Conference name carried under Yahoo conference key 79, the Liberty conference. |
| `conferences_79_type` | character | Structure level of Yahoo conference key 79 (Liberty); "conference" for a full conference rather than a division within one. |
| `conferences_93_id` | numeric | Yahoo numeric conference id carried for the American Southwest conference in the league's division structure. |
| `conferences_93_name` | character | Conference name carried under Yahoo conference key 93, the American Southwest conference. |
| `conferences_93_type` | character | Structure level of Yahoo conference key 93 (American Southwest); "conference" for a full conference rather than a division within one. |
| `conferences_105_id` | numeric | Yahoo numeric conference id carried for the Heartland Collegiate Athletic conference in the league's division structure. |
| `conferences_105_name` | character | Conference name carried under Yahoo conference key 105, the Heartland Collegiate Athletic conference. |
| `conferences_105_type` | character | Structure level of Yahoo conference key 105 (Heartland Collegiate Athletic); "conference" for a full conference rather than a division within one. |
| `conferences_106_id` | numeric | Yahoo numeric conference id carried for the Wisconsin Intercollegiate Athletic Conference conference in the league's division structure. |
| `conferences_106_name` | character | Conference name carried under Yahoo conference key 106, the Wisconsin Intercollegiate Athletic Conference conference. |
| `conferences_106_type` | character | Structure level of Yahoo conference key 106 (Wisconsin Intercollegiate Athletic Conference); "conference" for a full conference rather than a division within one. |
| `conferences_109_id` | numeric | Yahoo numeric conference id carried for the Empire Eight conference in the league's division structure. |
| `conferences_109_name` | character | Conference name carried under Yahoo conference key 109, the Empire Eight conference. |
| `conferences_109_type` | character | Structure level of Yahoo conference key 109 (Empire Eight); "conference" for a full conference rather than a division within one. |
| `conferences_113_id` | numeric | Yahoo numeric conference id carried for the Upper Midwest Athletic Conference conference in the league's division structure. |
| `conferences_113_name` | character | Conference name carried under Yahoo conference key 113, the Upper Midwest Athletic Conference conference. |
| `conferences_113_type` | character | Structure level of Yahoo conference key 113 (Upper Midwest Athletic Conference); "conference" for a full conference rather than a division within one. |
| `conferences_114_id` | numeric | Yahoo numeric conference id carried for the USA South Athletic Conference conference in the league's division structure. |
| `conferences_114_name` | character | Conference name carried under Yahoo conference key 114, the USA South Athletic Conference conference. |
| `conferences_114_type` | character | Structure level of Yahoo conference key 114 (USA South Athletic Conference); "conference" for a full conference rather than a division within one. |
| `conferences_129_id` | numeric | Yahoo numeric conference id carried for the Massachusetts State Collegiate Athletic Conference conference in the league's division structure. |
| `conferences_129_name` | character | Conference name carried under Yahoo conference key 129, the Massachusetts State Collegiate Athletic Conference conference. |
| `conferences_129_type` | character | Structure level of Yahoo conference key 129 (Massachusetts State Collegiate Athletic Conference); "conference" for a full conference rather than a division within one. |
| `conferences_130_id` | numeric | Yahoo numeric conference id carried for the Southern Athletic Association conference in the league's division structure. |
| `conferences_130_name` | character | Conference name carried under Yahoo conference key 130, the Southern Athletic Association conference. |
| `conferences_130_type` | character | Structure level of Yahoo conference key 130 (Southern Athletic Association); "conference" for a full conference rather than a division within one. |
| `conferences_132_id` | numeric | Yahoo numeric conference id carried for the Central Atlantic Collegiate Conference conference in the league's division structure. |
| `conferences_132_name` | character | Conference name carried under Yahoo conference key 132, the Central Atlantic Collegiate Conference conference. |
| `conferences_132_type` | character | Structure level of Yahoo conference key 132 (Central Atlantic Collegiate Conference); "conference" for a full conference rather than a division within one. |
| `conferences_48_id` | numeric | Yahoo numeric conference id carried for the Midwest Collegiate Athletic conference in the league's division structure. |
| `conferences_48_name` | character | Conference name carried under Yahoo conference key 48, the Midwest Collegiate Athletic conference. |
| `conferences_48_type` | character | Structure level of Yahoo conference key 48 (Midwest Collegiate Athletic); "conference" for a full conference rather than a division within one. |
| `conferences_62_id` | numeric | Yahoo numeric conference id carried for the Frontier conference in the league's division structure. |
| `conferences_62_name` | character | Conference name carried under Yahoo conference key 62, the Frontier conference. |
| `conferences_62_type` | character | Structure level of Yahoo conference key 62 (Frontier); "conference" for a full conference rather than a division within one. |
| `conferences_68_id` | numeric | Yahoo numeric conference id carried for the Mid-South conference in the league's division structure. |
| `conferences_68_name` | character | Conference name carried under Yahoo conference key 68, the Mid-South conference. |
| `conferences_68_type` | character | Structure level of Yahoo conference key 68 (Mid-South); "conference" for a full conference rather than a division within one. |
| `conferences_69_id` | numeric | Yahoo numeric conference id carried for the Chicagoland Collegiate conference in the league's division structure. |
| `conferences_69_name` | character | Conference name carried under Yahoo conference key 69, the Chicagoland Collegiate conference. |
| `conferences_69_type` | character | Structure level of Yahoo conference key 69 (Chicagoland Collegiate); "conference" for a full conference rather than a division within one. |
| `conferences_70_id` | numeric | Yahoo numeric conference id carried for the American Midwest conference in the league's division structure. |
| `conferences_70_name` | character | Conference name carried under Yahoo conference key 70, the American Midwest conference. |
| `conferences_70_type` | character | Structure level of Yahoo conference key 70 (American Midwest); "conference" for a full conference rather than a division within one. |
| `conferences_77_id` | numeric | Yahoo numeric conference id carried for the Independents (NAIA-I) conference in the league's division structure. |
| `conferences_77_name` | character | Conference name carried under Yahoo conference key 77, the Independents (NAIA-I) conference. |
| `conferences_77_type` | character | Structure level of Yahoo conference key 77 (Independents (NAIA-I)); "conference" for a full conference rather than a division within one. |
| `conferences_103_id` | numeric | Yahoo numeric conference id carried for the Cascade Collegiate Conference conference in the league's division structure. |
| `conferences_103_name` | character | Conference name carried under Yahoo conference key 103, the Cascade Collegiate Conference conference. |
| `conferences_103_type` | character | Structure level of Yahoo conference key 103 (Cascade Collegiate Conference); "conference" for a full conference rather than a division within one. |
| `conferences_104_id` | numeric | Yahoo numeric conference id carried for the Mid-States Football Association conference in the league's division structure. |
| `conferences_104_name` | character | Conference name carried under Yahoo conference key 104, the Mid-States Football Association conference. |
| `conferences_104_type` | character | Structure level of Yahoo conference key 104 (Mid-States Football Association); "conference" for a full conference rather than a division within one. |
| `conferences_115_id` | numeric | Yahoo numeric conference id carried for the Great Plains Athletic Conference conference in the league's division structure. |
| `conferences_115_name` | character | Conference name carried under Yahoo conference key 115, the Great Plains Athletic Conference conference. |
| `conferences_115_type` | character | Structure level of Yahoo conference key 115 (Great Plains Athletic Conference); "conference" for a full conference rather than a division within one. |
| `conferences_116_id` | numeric | Yahoo numeric conference id carried for the Heart of America Athletic Conference conference in the league's division structure. |
| `conferences_116_name` | character | Conference name carried under Yahoo conference key 116, the Heart of America Athletic Conference conference. |
| `conferences_116_type` | character | Structure level of Yahoo conference key 116 (Heart of America Athletic Conference); "conference" for a full conference rather than a division within one. |
| `conferences_117_id` | numeric | Yahoo numeric conference id carried for the Kansas Collegiate Athletic Conference conference in the league's division structure. |
| `conferences_117_name` | character | Conference name carried under Yahoo conference key 117, the Kansas Collegiate Athletic Conference conference. |
| `conferences_117_type` | character | Structure level of Yahoo conference key 117 (Kansas Collegiate Athletic Conference); "conference" for a full conference rather than a division within one. |
| `conferences_119_id` | numeric | Yahoo numeric conference id carried for the Red River Athletic Conference conference in the league's division structure. |
| `conferences_119_name` | character | Conference name carried under Yahoo conference key 119, the Red River Athletic Conference conference. |
| `conferences_119_type` | character | Structure level of Yahoo conference key 119 (Red River Athletic Conference); "conference" for a full conference rather than a division within one. |
| `conferences_133_id` | numeric | Yahoo numeric conference id carried for the New England Women's and Men's Athletic Conference conference in the league's division structure. |
| `conferences_133_name` | character | Conference name carried under Yahoo conference key 133, the New England Women's and Men's Athletic Conference conference. |
| `conferences_133_type` | character | Structure level of Yahoo conference key 133 (New England Women's and Men's Athletic Conference); "conference" for a full conference rather than a division within one. |
| `conferences_135_id` | numeric | Yahoo numeric conference id carried for the Wolverine-Hoosier Athletic Conference conference in the league's division structure. |
| `conferences_135_name` | character | Conference name carried under Yahoo conference key 135, the Wolverine-Hoosier Athletic Conference conference. |
| `conferences_135_type` | character | Structure level of Yahoo conference key 135 (Wolverine-Hoosier Athletic Conference); "conference" for a full conference rather than a division within one. |
| `conferences_136_id` | numeric | Yahoo numeric conference id carried for the Sooner Athletic Conference conference in the league's division structure. |
| `conferences_136_name` | character | Conference name carried under Yahoo conference key 136, the Sooner Athletic Conference conference. |
| `conferences_136_type` | character | Structure level of Yahoo conference key 136 (Sooner Athletic Conference); "conference" for a full conference rather than a division within one. |
| `conferences_137_id` | numeric | Yahoo numeric conference id carried for the Southern States Athletic Conference conference in the league's division structure. |
| `conferences_137_name` | character | Conference name carried under Yahoo conference key 137, the Southern States Athletic Conference conference. |
| `conferences_137_type` | character | Structure level of Yahoo conference key 137 (Southern States Athletic Conference); "conference" for a full conference rather than a division within one. |
| `conferences_139_id` | numeric | Yahoo numeric conference id carried for the Midlands Collegiate Athletic Conference conference in the league's division structure. |
| `conferences_139_name` | character | Conference name carried under Yahoo conference key 139, the Midlands Collegiate Athletic Conference conference. |
| `conferences_139_type` | character | Structure level of Yahoo conference key 139 (Midlands Collegiate Athletic Conference); "conference" for a full conference rather than a division within one. |
| `conferences_141_id` | numeric | Yahoo numeric conference id carried for the Crossroads League conference in the league's division structure. |
| `conferences_141_name` | character | Conference name carried under Yahoo conference key 141, the Crossroads League conference. |
| `conferences_141_type` | character | Structure level of Yahoo conference key 141 (Crossroads League); "conference" for a full conference rather than a division within one. |
| `conferences_281_id` | numeric | Yahoo numeric conference id carried for the The Sun Conference conference in the league's division structure. |
| `conferences_281_name` | character | Conference name carried under Yahoo conference key 281, the The Sun Conference conference. |
| `conferences_281_type` | character | Structure level of Yahoo conference key 281 (The Sun Conference); "conference" for a full conference rather than a division within one. |
| `conferences_284_id` | numeric | Yahoo numeric conference id carried for the New South Athletic Conference conference in the league's division structure. |
| `conferences_284_name` | character | Conference name carried under Yahoo conference key 284, the New South Athletic Conference conference. |
| `conferences_284_type` | character | Structure level of Yahoo conference key 284 (New South Athletic Conference); "conference" for a full conference rather than a division within one. |
| `conferences_125_id` | numeric | Yahoo numeric conference id carried for the Independents (ASCAA) conference in the league's division structure. |
| `conferences_125_name` | character | Conference name carried under Yahoo conference key 125, the Independents (ASCAA) conference. |
| `conferences_125_type` | character | Structure level of Yahoo conference key 125 (Independents (ASCAA)); "conference" for a full conference rather than a division within one. |

**teamcolor_primary**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `value` | character | Numeric or string value field. |

**teamcolor_secondary**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `value` | character | Numeric or string value field. |

**gamehighlight**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `value` | character | Numeric or string value field. |

**gametv_details**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `tv_details` | character | JSON-encoded list of broadcast entries for the game, each carrying a network abbreviation and full channel name (e.g., [{"abbr": "NBC", "name": "NBC/Peacock"}]). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
yahoo_editorial_scoreboard()
```

_Last validated n/a._
