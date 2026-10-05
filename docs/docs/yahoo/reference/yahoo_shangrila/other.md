---
title: "YAHOO — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Other: oly_medal–tennis_tournaments"
sidebar_label: "Other: oly_medal–tennis_tournaments"
sidebar_position: 10
description: "YAHOO — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Other: oly_medal–tennis_tournaments — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# YAHOO — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Other: oly_medal–tennis_tournaments

## yahoo_oly_medal_count

Yahoo shangrila persisted query `OlyMedalCount` -> one row per `olympics` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/OlyMedalCount`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/OlyMedalCount](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/OlyMedalCount)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `sortMethod` | `sort_method` |  |  | `Y` | sortMethod query parameter. |

### Returns {#yahoo_oly_medal_count-returns}

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

### Example {#yahoo_oly_medal_count-example}

```python
yahoo_oly_medal_count()
```

_Last validated n/a._

## yahoo_oly_seasons

Yahoo shangrila persisted query `OlySeasons` -> one row per `olympics` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/OlySeasons`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/OlySeasons](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/OlySeasons)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `seasons` | `seasons` |  |  | `Y` | seasons query parameter. |

### Returns {#yahoo_oly_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `season` | integer | Season year. |
| `display_name` | character | Display name. |
| `type` | character | Record type / category. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_oly_seasons-example}

```python
yahoo_oly_seasons()
```

_Last validated n/a._

## yahoo_alias

Yahoo shangrila persisted query `alias` -> one row per `pageMetaData` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/alias`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/alias](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/alias)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `alias` | `alias` |  |  | `Y` | alias query parameter. |

### Returns {#yahoo_alias-returns}

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

### Example {#yahoo_alias-example}

```python
yahoo_alias()
```

_Last validated n/a._

## yahoo_article_list_card_players

Yahoo shangrila persisted query `articleListCardPlayers` -> one row per `players` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/articleListCardPlayers`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/articleListCardPlayers](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/articleListCardPlayers)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `playerIds` | `player_ids` |  |  | `Y` | playerIds query parameter. |

### Returns {#yahoo_article_list_card_players-returns}

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

### Example {#yahoo_article_list_card_players-example}

```python
yahoo_article_list_card_players()
```

_Last validated n/a._

## yahoo_article_list_card_teams

Yahoo shangrila persisted query `articleListCardTeams` -> one row per `teams` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/articleListCardTeams`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/articleListCardTeams](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/articleListCardTeams)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `teamIds` | `team_ids` |  |  | `Y` | teamIds query parameter. |

### Returns {#yahoo_article_list_card_teams-returns}

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

### Example {#yahoo_article_list_card_teams-example}

```python
yahoo_article_list_card_teams()
```

_Last validated n/a._

## yahoo_basic_players

Yahoo shangrila persisted query `basicPlayers` -> one row per `players` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/basicPlayers`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/basicPlayers](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/basicPlayers)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `players` | `players` |  |  | `Y` | players query parameter. |

### Returns {#yahoo_basic_players-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_id` | character | Unique player identifier. |
| `display_name` | character | Display name. |
| `suggested_headshot` | character | JSON-encoded image node for the headshot Yahoo recommends for this player. |
| `alias_url` | character | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_basic_players-example}

```python
yahoo_basic_players()
```

_Last validated n/a._

## yahoo_betting_disclaimer

Yahoo shangrila persisted query `bettingDisclaimer` -> one row per `bettingDisclaimers` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/bettingDisclaimer`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/bettingDisclaimer](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/bettingDisclaimer)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `bettingDisclaimerId` | `betting_disclaimer_id` |  |  | `Y` | bettingDisclaimerId query parameter. |

### Returns {#yahoo_betting_disclaimer-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `disclaimer_id` | character | Identifier of the responsible-gambling disclaimer block to render alongside the odds. |
| `text` | character | Text description of the play / record. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_betting_disclaimer-example}

```python
yahoo_betting_disclaimer()
```

_Last validated n/a._

## yahoo_combat_event_fights

Yahoo shangrila persisted query `combatEventFights` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/combatEventFights`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/combatEventFights](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/combatEventFights)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `eventGroupId` | `event_group_id` |  |  | `Y` | eventGroupId query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `league` | `league` |  |  | `Y` | league query parameter. |

### Returns {#yahoo_combat_event_fights-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `short_name` | character | Short display name. |
| `full_name` | character | Player's full name. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_combat_event_fights-example}

```python
yahoo_combat_event_fights()
```

_Last validated n/a._

## yahoo_combat_schedule

Yahoo shangrila persisted query `combatSchedule` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/combatSchedule`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/combatSchedule](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/combatSchedule)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `league` | `league` |  |  | `Y` | league query parameter. |

### Returns {#yahoo_combat_schedule-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `short_name` | character | Short display name. |
| `full_name` | character | Player's full name. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_combat_schedule-example}

```python
yahoo_combat_schedule()
```

_Last validated n/a._

## yahoo_common_pills

Yahoo shangrila persisted query `common/pills` (response body not captured; shape unknown)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/common/pills`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/common/pills](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/common/pills)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `addTeamLogos` | `add_team_logos` |  |  | `Y` | addTeamLogos query parameter. |
| `date` | `date` |  |  | `Y` | date query parameter. |
| `teamIds` | `team_ids` |  |  | `Y` | teamIds query parameter. |

### Returns {#yahoo_common_pills-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_yahoo_shangrila`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_common_pills-example}

```python
yahoo_common_pills()
```

_Last validated n/a._

## yahoo_consensus_rankings_php

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

### Returns {#yahoo_consensus_rankings_php-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_yahoo_shangrila`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_consensus_rankings_php-example}

```python
yahoo_consensus_rankings_php()
```

_Last validated n/a._

## yahoo_draft

Yahoo shangrila persisted query `draft` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/draft`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/draft](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/draft)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | league query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#yahoo_draft-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_yahoo_shangrila`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_draft-example}

```python
yahoo_draft()
```

_Last validated n/a._

## yahoo_draft_prospects

Yahoo shangrila persisted query `draftProspects` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/draftProspects`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/draftProspects](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/draftProspects)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | league query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `imageHeight` | `image_height` |  |  | `Y` | imageHeight query parameter. |
| `imageWidth` | `image_width` |  |  | `Y` | imageWidth query parameter. |

### Returns {#yahoo_draft_prospects-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_yahoo_shangrila`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_draft_prospects-example}

```python
yahoo_draft_prospects()
```

_Last validated n/a._

## yahoo_driver_results

Yahoo shangrila persisted query `driverResults` -> one row per `players` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/driverResults`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/driverResults](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/driverResults)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `playerId` | `player_id` |  |  | `Y` | playerId query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#yahoo_driver_results-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_yahoo_shangrila`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_driver_results-example}

```python
yahoo_driver_results()
```

_Last validated n/a._

## yahoo_driver_splits

Yahoo shangrila persisted query `driverSplits` -> one row per `players` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/driverSplits`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/driverSplits](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/driverSplits)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `playerId` | `player_id` |  |  | `Y` | playerId query parameter. |

### Returns {#yahoo_driver_splits-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_yahoo_shangrila`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_driver_splits-example}

```python
yahoo_driver_splits()
```

_Last validated n/a._

## yahoo_featured_game_ids

Yahoo shangrila persisted query `featuredGameIds` -> one row per `featuredGames` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/featuredGameIds`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/featuredGameIds](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/featuredGameIds)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#yahoo_featured_game_ids-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_featured_game_ids-example}

```python
yahoo_featured_game_ids()
```

_Last validated n/a._

## yahoo_gametime_game

Yahoo shangrila persisted query `gametimeGame` -> one row per `games` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/gametimeGame`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/gametimeGame](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/gametimeGame)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameId` | `game_id` |  |  | `Y` | gameId query parameter. |

### Returns {#yahoo_gametime_game-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `gametime_ticket_url` | character | Gametime affiliate ticket-purchase URL for the event or team. |
| `game_ticket_price` | character | Lowest available ticket price for the game from the Gametime affiliate feed, in US dollars. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_gametime_game-example}

```python
yahoo_gametime_game()
```

_Last validated n/a._

## yahoo_gametime_team

Yahoo shangrila persisted query `gametimeTeam` -> one row per `teams` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/gametimeTeam`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/gametimeTeam](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/gametimeTeam)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `teamId` | `team_id` |  |  | `Y` | teamId query parameter. |

### Returns {#yahoo_gametime_team-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `gametime_ticket_url` | character | Gametime affiliate ticket-purchase URL for the event or team. |
| `team_id` | character | Unique team identifier. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_gametime_team-example}

```python
yahoo_gametime_team()
```

_Last validated n/a._

## yahoo_golf_tournament_seasons

Yahoo shangrila persisted query `golfTournamentSeasons` (response body not captured; shape unknown)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/golfTournamentSeasons`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/golfTournamentSeasons](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/golfTournamentSeasons)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `eventGroupId` | `event_group_id` |  |  | `Y` | eventGroupId query parameter. |

### Returns {#yahoo_golf_tournament_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_yahoo_shangrila`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_golf_tournament_seasons-example}

```python
yahoo_golf_tournament_seasons()
```

_Last validated n/a._

## yahoo_golf_tournaments

Yahoo shangrila persisted query `golfTournaments` -> one row per `golfTournaments` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/golfTournaments`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/golfTournaments](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/golfTournaments)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `association` | `association` |  |  | `Y` | association query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `showDefendingChamps` | `show_defending_champs` |  |  | `Y` | showDefendingChamps query parameter. |

### Returns {#yahoo_golf_tournaments-returns}

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

### Example {#yahoo_golf_tournaments-example}

```python
yahoo_golf_tournaments()
```

_Last validated n/a._

## yahoo_golf_tournaments_basic

Yahoo shangrila persisted query `golfTournamentsBasic` -> one row per `golfTournaments` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/golfTournamentsBasic`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/golfTournamentsBasic](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/golfTournamentsBasic)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `eventGroupId` | `event_group_id` |  |  | `Y` | eventGroupId query parameter. |
| `association` | `association` |  |  | `Y` | association query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#yahoo_golf_tournaments_basic-returns}

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

### Example {#yahoo_golf_tournaments_basic-example}

```python
yahoo_golf_tournaments_basic()
```

_Last validated n/a._

## yahoo_module_game

Yahoo shangrila persisted query `moduleGame` -> one row per `games` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/moduleGame`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/moduleGame](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/moduleGame)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameId` | `game_id` |  |  | `Y` | gameId query parameter. |
| `imageHeight` | `image_height` |  |  | `Y` | imageHeight query parameter. |
| `imageWidth` | `image_width` |  |  | `Y` | imageWidth query parameter. |

### Returns {#yahoo_module_game-returns}

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

### Example {#yahoo_module_game-example}

```python
yahoo_module_game()
```

_Last validated n/a._

## yahoo_motorsport_standings

Yahoo shangrila persisted query `motorsportStandings` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/motorsportStandings`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/motorsportStandings](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/motorsportStandings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | league query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#yahoo_motorsport_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `name` | character | Display name. |
| `full_name` | character | Player's full name. |
| `current_league_season` | character | Yahoo league-season identifier for the season currently in progress. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_motorsport_standings-example}

```python
yahoo_motorsport_standings()
```

_Last validated n/a._

## yahoo_nascar_drivers

Yahoo shangrila persisted query `nascarDrivers` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/nascarDrivers`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/nascarDrivers](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/nascarDrivers)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | league query parameter. |

### Returns {#yahoo_nascar_drivers-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `short_name` | character | Short display name. |
| `players` | character | Nested list of per-player box scores. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_nascar_drivers-example}

```python
yahoo_nascar_drivers()
```

_Last validated n/a._

## yahoo_nav_dropdown_tray

Yahoo shangrila persisted query `navDropdownTray` -> tables: nfl, nhl, nba, mlb, wnba, ncaab, ncaaf, ncaaw, sportsbook_legal_states

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/navDropdownTray`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/navDropdownTray](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/navDropdownTray)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `getSoccerData` | `get_soccer_data` |  |  | `Y` | getSoccerData query parameter. |
| `soccerLeagueIds` | `soccer_league_ids` |  |  | `Y` | soccerLeagueIds query parameter. |
| `soccerTeamIds` | `soccer_team_ids` |  |  | `Y` | soccerTeamIds query parameter. |

### Returns {#yahoo_nav_dropdown_tray-returns}

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

### Example {#yahoo_nav_dropdown_tray-example}

```python
yahoo_nav_dropdown_tray()
```

_Last validated n/a._

## yahoo_pick_distribution

Yahoo shangrila persisted query `pickDistribution` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/pickDistribution`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/pickDistribution](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/pickDistribution)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | league query parameter. |
| `dates` | `dates` |  |  | `Y` | Date or date range filter (YYYYMMDD or YYYYMMDD-YYYYMMDD). |
| `count` | `count` |  |  | `Y` | count query parameter. |

### Returns {#yahoo_pick_distribution-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `ncaaf_games` | character | JSON-encoded list of NCAAF game nodes carrying the pick or odds distribution for the slate. |
| `conferences` | character | JSON-encoded list of the league's conference nodes, each carrying an id, a name and its member teams. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_pick_distribution-example}

```python
yahoo_pick_distribution()
```

_Last validated n/a._

## yahoo_playoff_bracket

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

### Returns {#yahoo_playoff_bracket-returns}

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

### Example {#yahoo_playoff_bracket-example}

```python
yahoo_playoff_bracket()
```

_Last validated n/a._

## yahoo_playoff_series_game

Yahoo shangrila persisted query `playoffSeriesGame` -> one row per `games` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playoffSeriesGame`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playoffSeriesGame](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playoffSeriesGame)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameId` | `game_id` |  |  | `Y` | gameId query parameter. |

### Returns {#yahoo_playoff_series_game-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `playoff_series` | character | JSON-encoded playoff-series node the game belongs to. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_playoff_series_game-example}

```python
yahoo_playoff_series_game()
```

_Last validated n/a._

## yahoo_polymarket_game

Yahoo shangrila persisted query `polymarketGame` -> one row per `games` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/polymarketGame`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/polymarketGame](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/polymarketGame)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameId` | `game_id` |  |  | `Y` | gameId query parameter. |

### Returns {#yahoo_polymarket_game-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `polymarket_url` | character | Polymarket prediction-market URL for wagering on the game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_polymarket_game-example}

```python
yahoo_polymarket_game()
```

_Last validated n/a._

## yahoo_racing_schedule

Yahoo shangrila persisted query `racingSchedule` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/racingSchedule`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/racingSchedule](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/racingSchedule)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | league query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `today` | `today` |  |  | `Y` | today query parameter. |
| `hasSeries` | `has_series` |  |  | `Y` | hasSeries query parameter. |

### Returns {#yahoo_racing_schedule-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `league_seasons` | character | JSON-encoded list of the seasons for which Yahoo carries data for this league. |
| `current_season` | integer | Season the league is currently playing, as the four-digit starting year. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_racing_schedule-example}

```python
yahoo_racing_schedule()
```

_Last validated n/a._

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

### Example {#yahoo_scoreboard_game-example}

```python
yahoo_scoreboard_game()
```

_Last validated n/a._

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

### Example {#yahoo_tennis_matches_by_date-example}

```python
yahoo_tennis_matches_by_date()
```

_Last validated n/a._

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

### Example {#yahoo_tennis_tournament-example}

```python
yahoo_tennis_tournament()
```

_Last validated n/a._

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

### Example {#yahoo_tennis_tournaments-example}

```python
yahoo_tennis_tournaments()
```

_Last validated n/a._
