# Yahoo Sports — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Other: oly_medal–racing_schedule

> Yahoo Sports — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Other: oly_medal–racing_schedule — function reference in sdv-py, the SportsDataverse Python package.

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
| `display_name` | String |  |
| `short_display_name` | String |  |
| `start_date` | String |  |
| `end_date` | String |  |
| `season` | Int64 |  |
| `alias` | String | JSON-encoded Yahoo alias object for the entity, carrying the site URL, path and subpage routing used to build links to its page. |
| `olympic_team` | String | JSON-encoded national team node whose medal count this row reports. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_oly_medal_count-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_oly_medal_count-example}

```python
yahoo_oly_medal_count()
```

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
| `season` | Int64 |  |
| `display_name` | String |  |
| `type` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_oly_seasons-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_oly_seasons-example}

```python
yahoo_oly_seasons()
```

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
| `page_type` | String |  |
| `league_short_name` | String |  |
| `league_display_name` | String | Display name of the league as rendered on the aliased page. |
| `entity_type` | String | Kind of entity the alias record points at (e.g., "team", "player", "league"). |
| `subpage_translation` | String | Localized display label for the subpage this alias record describes. |
| `entity_alias` | String | Alias template Yahoo serves the entity's own page under. |
| `subpage_alias` | String | Alias template for the subpage this alias record describes. |
| `hotlist_data_desktop_space_id` | String | Content-space identifier for the desktop hotlist module rendered on the aliased page. |
| `hotlist_data_tablet_space_id` | String | Content-space identifier for the tablet hotlist module rendered on the aliased page. |
| `hotlist_data_mobile_space_id` | String | Content-space identifier for the mobile hotlist module rendered on the aliased page. |
| `entity_list_id_desktop_list_id` | String | Identifier of the curated desktop entity list rendered on the aliased page. |
| `entity_list_id_mobile_list_id` | String | Identifier of the curated mobile entity list rendered on the aliased page. |
| `entity_list_id_tablet_list_id` | String | Identifier of the curated tablet entity list rendered on the aliased page. |
| `game` | String |  |
| `match` | String | Alias template Yahoo serves the match page under for this league. |
| `race` | String | Alias template Yahoo serves the motorsport race page under for this league. |
| `league` | String |  |
| `team` | String |  |
| `golf_tournament` | String | Alias template Yahoo serves the golf-tournament page under for this league. |
| `tennis_tournament` | String | Alias template Yahoo serves the tennis-tournament page under for this league. |
| `player` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_alias-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_alias-example}

```python
yahoo_alias()
```

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
| `alias_path` | String | Site-relative path portion of the entity's Yahoo alias (e.g., "/ncaaf/teams/tcu/"). |
| `alias_lang` | String | Language/locale tag attached to the entity's Yahoo alias (e.g., "en-US"). |
| `alias_url` | String | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `alias_domain` | String | Host the entity's Yahoo alias resolves against (e.g., "sports.yahoo.com"). |
| `player_id` | String |  |
| `display_name` | String |  |
| `short_display_name` | String |  |
| `player_cutout` | String | JSON-encoded image node for the player's transparent cut-out portrait. |
| `team_alias` | String | JSON-encoded alias object for the entity's team, carrying its Yahoo page URL and path. |
| `team_display_name` | String |  |
| `team_primary_color` | String | Primary brand color of the entity's team, as a hex RGB string without the leading hash. |
| `team_secondary_color` | String | Secondary brand color of the entity's team, as a hex RGB string without the leading hash. |
| `team_team_id` | String |  |
| `team_team_logo_white` | String | JSON-encoded image node for the team's white knockout logo. |
| `team_team_logo` | String | JSON-encoded image node for the team's standard logo. |
| `team_gametime_ticket_url` | String | Gametime affiliate ticket-purchase URL for the team's home games. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_article_list_card_players-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_article_list_card_players-example}

```python
yahoo_article_list_card_players()
```

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
| `alias_path` | String | Site-relative path portion of the entity's Yahoo alias (e.g., "/ncaaf/teams/tcu/"). |
| `alias_lang` | String | Language/locale tag attached to the entity's Yahoo alias (e.g., "en-US"). |
| `alias_url` | String | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `alias_domain` | String | Host the entity's Yahoo alias resolves against (e.g., "sports.yahoo.com"). |
| `display_name` | String |  |
| `nickname` | String |  |
| `primary_color` | String |  |
| `secondary_color` | String |  |
| `team_id` | String |  |
| `team_logo_white_width` | String | Pixel width of the team's white knockout logo image. |
| `team_logo_white_last_updated` | String | Timestamp at which the team's white knockout logo asset was last refreshed. |
| `team_logo_white_image_type` | String | File format of the team's white knockout logo asset (e.g., "png"). |
| `team_logo_white_url` | String | Absolute URL of the team's white knockout logo, the variant used on dark backgrounds. |
| `team_logo_white_height` | String | Pixel height of the team's white knockout logo image. |
| `team_logo_white_team_id` | String | Yahoo composite team id the white knockout logo asset belongs to. |
| `team_logo_width` | String | Pixel width of the team's standard logo image. |
| `team_logo_last_updated` | String | Timestamp at which the team's standard logo asset was last refreshed. |
| `team_logo_image_type` | String | File format of the team's standard logo asset (e.g., "png"). |
| `team_logo_url` | String | Absolute URL of the team's standard logo image on Yahoo's image CDN. |
| `team_logo_height` | String | Pixel height of the team's standard logo image. |
| `team_logo_team_id` | String | Yahoo composite team id the standard logo asset belongs to. |
| `gametime_ticket_url` | String | Gametime affiliate ticket-purchase URL for the event or team. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_article_list_card_teams-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_article_list_card_teams-example}

```python
yahoo_article_list_card_teams()
```

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
| `player_id` | String |  |
| `display_name` | String |  |
| `suggested_headshot` | String | JSON-encoded image node for the headshot Yahoo recommends for this player. |
| `alias_url` | String | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_basic_players-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_basic_players-example}

```python
yahoo_basic_players()
```

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
| `disclaimer_id` | String | Identifier of the responsible-gambling disclaimer block to render alongside the odds. |
| `text` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_betting_disclaimer-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_betting_disclaimer-example}

```python
yahoo_betting_disclaimer()
```

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
| `short_name` | String |  |
| `full_name` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_combat_event_fights-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_combat_event_fights-example}

```python
yahoo_combat_event_fights()
```

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
| `short_name` | String |  |
| `full_name` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_combat_schedule-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_combat_schedule-example}

```python
yahoo_combat_schedule()
```

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

### Raises {#yahoo_common_pills-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_common_pills-example}

```python
yahoo_common_pills()
```

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

### Raises {#yahoo_consensus_rankings_php-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_consensus_rankings_php-example}

```python
yahoo_consensus_rankings_php()
```

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

### Raises {#yahoo_draft-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_draft-example}

```python
yahoo_draft()
```

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

### Raises {#yahoo_draft_prospects-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_draft_prospects-example}

```python
yahoo_draft_prospects()
```

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

### Raises {#yahoo_driver_results-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_driver_results-example}

```python
yahoo_driver_results()
```

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

### Raises {#yahoo_driver_splits-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_driver_splits-example}

```python
yahoo_driver_splits()
```

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
| `game_id` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_featured_game_ids-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_featured_game_ids-example}

```python
yahoo_featured_game_ids()
```

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
| `game_id` | String |  |
| `gametime_ticket_url` | String | Gametime affiliate ticket-purchase URL for the event or team. |
| `game_ticket_price` | String | Lowest available ticket price for the game from the Gametime affiliate feed, in US dollars. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_gametime_game-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_gametime_game-example}

```python
yahoo_gametime_game()
```

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
| `gametime_ticket_url` | String | Gametime affiliate ticket-purchase URL for the event or team. |
| `team_id` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_gametime_team-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_gametime_team-example}

```python
yahoo_gametime_team()
```

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

### Raises {#yahoo_golf_tournament_seasons-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_golf_tournament_seasons-example}

```python
yahoo_golf_tournament_seasons()
```

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
| `alias_url` | String | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `alias_path` | String | Site-relative path portion of the entity's Yahoo alias (e.g., "/ncaaf/teams/tcu/"). |
| `event_group_id` | String | Yahoo identifier that groups the rounds or legs making up a single tournament. |
| `name` | String |  |
| `display_name` | String |  |
| `start_time` | String |  |
| `start_date` | String |  |
| `end_date` | String |  |
| `status` | String |  |
| `status_display_name` | String | Short game or event status as shown on the scoreboard (e.g., "Final", "12:00 pm ET"). |
| `player_tournament_stats` | String | JSON-encoded per-player statistics recorded at the golf tournament. |
| `purse` | String | Total prize money on offer at the tournament, in US dollars. |
| `major` | Boolean | Flag indicating that the golf tournament is one of the sport's majors. |
| `venue_display_name` | String | Name of the venue hosting the event. |
| `venue_country` | String | Country the venue is located in. |
| `venue_city` | String |  |
| `venue_state` | String |  |
| `par` | Int64 | Par of the golf course in play for the tournament. |
| `yardage` | Int64 | Total yardage of the golf course in play for the tournament. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_golf_tournaments-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_golf_tournaments-example}

```python
yahoo_golf_tournaments()
```

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
| `event_group_id` | String | Yahoo identifier that groups the rounds or legs making up a single tournament. |
| `start_time` | String |  |
| `start_date` | String |  |
| `end_date` | String |  |
| `season` | Int64 |  |
| `clubs` | String | JSON-encoded list of the golf clubs hosting the tournament. |
| `courses` | String | JSON-encoded list of the courses in play at the tournament, with their par and yardage. |
| `name` | String |  |
| `status` | String |  |
| `alias_url` | String | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `alias_path` | String | Site-relative path portion of the entity's Yahoo alias (e.g., "/ncaaf/teams/tcu/"). |
| `association` | String | Governing tour that sanctions the event (e.g., "pga"). |
| `league_short_name` | String |  |
| `league_full_name` | String | Full league name (e.g., "NCAA Football"). |
| `league_alias` | String | JSON-encoded Yahoo alias object for the league, carrying its site URL and path. |
| `purse` | String | Total prize money on offer at the tournament, in US dollars. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_golf_tournaments_basic-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_golf_tournaments_basic-example}

```python
yahoo_golf_tournaments_basic()
```

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
| `game_id` | String |  |
| `display_name` | String |  |
| `league_name` | String |  |
| `league_full_name` | String | Full league name (e.g., "NCAA Football"). |
| `league_display_abbr` | String | Compact league abbreviation used in dense UI alongside the game. |
| `league_display_short` | String | Short league label used in navigation and compact UI (e.g., "NCAA FB"). |
| `league_short_name` | String |  |
| `league_sport` | String | Sport the league belongs to (e.g., "football"). |
| `league_alias` | String | JSON-encoded Yahoo alias object for the league, carrying its site URL and path. |
| `alias_url` | String | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `away_team_id` | String |  |
| `away_team_record` | String |  |
| `away_team_full_name` | String |  |
| `away_team_team_id` | String | Yahoo composite team id of the away team (e.g., "ncaaf.t.29"). |
| `away_team_primary_color` | String | Primary brand color of the away team, as a hex RGB string without the leading hash. |
| `away_team_secondary_color` | String | Secondary brand color of the away team, as a hex RGB string without the leading hash. |
| `away_team_display_name` | String |  |
| `away_team_abbreviation` | String |  |
| `away_team_location` | String |  |
| `away_team_gametime_ticket_url` | String | Gametime affiliate ticket-purchase URL for the away team's games. |
| `away_team_alias` | String | JSON-encoded Yahoo alias object for the away team, carrying its site URL and path. |
| `away_team_nickname` | String |  |
| `away_team_last_games` | String | JSON-encoded list of the away team's most recently completed games. |
| `away_team_team_logo_white` | String | JSON-encoded image node for the away team's white knockout logo, used on dark backgrounds. |
| `away_team_team_logo` | String | JSON-encoded image node for the away team's standard logo. |
| `away_team_team_standings` | String | JSON-encoded standings node for the away team, carrying its record, position and streak. |
| `away_team_rank_polls` | String | JSON-encoded list of the poll rankings the away team currently holds. |
| `away_team_playoff_seeds` | String | JSON-encoded list of the away team's playoff-seed entries for the season. |
| `home_team_id` | String |  |
| `home_team_record` | String |  |
| `home_team_full_name` | String |  |
| `home_team_team_id` | String | Yahoo composite team id of the home team (e.g., "ncaaf.t.29"). |
| `home_team_primary_color` | String | Primary brand color of the home team, as a hex RGB string without the leading hash. |
| `home_team_secondary_color` | String | Secondary brand color of the home team, as a hex RGB string without the leading hash. |
| `home_team_display_name` | String |  |
| `home_team_abbreviation` | String |  |
| `home_team_location` | String |  |
| `home_team_gametime_ticket_url` | String | Gametime affiliate ticket-purchase URL for the home team's games. |
| `home_team_alias` | String | JSON-encoded Yahoo alias object for the home team, carrying its site URL and path. |
| `home_team_nickname` | String |  |
| `home_team_last_games` | String | JSON-encoded list of the home team's most recently completed games. |
| `home_team_team_logo_white` | String | JSON-encoded image node for the home team's white knockout logo, used on dark backgrounds. |
| `home_team_team_logo` | String | JSON-encoded image node for the home team's standard logo. |
| `home_team_team_standings` | String | JSON-encoded standings node for the home team, carrying its record, position and streak. |
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
| `gametime_ticket_url` | String | Gametime affiliate ticket-purchase URL for the event or team. |
| `game_ticket_price` | String | Lowest available ticket price for the game from the Gametime affiliate feed, in US dollars. |
| `playoff_series` | String | JSON-encoded playoff-series node the game belongs to. |
| `winning_team_id` | String | Composite Yahoo team id of the side that won the game (e.g., "ncaaf.t.29"). |
| `broadcast_channels` | String | JSON-encoded list of the channels broadcasting the event. |
| `news_break_subtext` | String | Secondary line of the news-break banner attached to the game. |
| `news_break_title` | String | Headline of the news-break banner attached to the game. |
| `news_break_url` | String | URL of the article behind the game's news-break banner. |
| `news_break_uuid` | String | Yahoo content UUID of the article behind the game's news-break banner. |
| `brief` | String | Short editorial blurb summarizing the game's state or result. |
| `event_extended_display_name` | String | Long-form event title used for marquee games, such as a bowl or rivalry name. |
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

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_module_game-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_module_game-example}

```python
yahoo_module_game()
```

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
| `name` | String |  |
| `full_name` | String |  |
| `current_league_season` | String | Yahoo league-season identifier for the season currently in progress. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_motorsport_standings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_motorsport_standings-example}

```python
yahoo_motorsport_standings()
```

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
| `short_name` | String |  |
| `players` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_nascar_drivers-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_nascar_drivers-example}

```python
yahoo_nascar_drivers()
```

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

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**nfl**

| col_name | type | description |
|---|---|---|
| `short_name` | String |  |
| `teams` | String |  |

**nhl**

| col_name | type | description |
|---|---|---|
| `short_name` | String |  |
| `teams` | String |  |

**nba**

| col_name | type | description |
|---|---|---|
| `short_name` | String |  |
| `teams` | String |  |

**mlb**

| col_name | type | description |
|---|---|---|
| `short_name` | String |  |
| `teams` | String |  |

**wnba**

| col_name | type | description |
|---|---|---|
| `short_name` | String |  |
| `teams` | String |  |

**ncaab**

| col_name | type | description |
|---|---|---|
| `poll_name` | String |  |
| `rank` | String |  |
| `team_id` | String |  |
| `team` | String |  |

**ncaaf**

| col_name | type | description |
|---|---|---|
| `poll_name` | String |  |
| `rank` | String |  |
| `team_id` | String |  |
| `team` | String |  |

**ncaaw**

| col_name | type | description |
|---|---|---|
| `poll_name` | String |  |
| `rank` | String |  |
| `team_id` | String |  |
| `team` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_nav_dropdown_tray-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_nav_dropdown_tray-example}

```python
yahoo_nav_dropdown_tray()
```

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
| `ncaaf_games` | String | JSON-encoded list of NCAAF game nodes carrying the pick or odds distribution for the slate. |
| `conferences` | String | JSON-encoded list of the league's conference nodes, each carrying an id, a name and its member teams. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_pick_distribution-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_pick_distribution-example}

```python
yahoo_pick_distribution()
```

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
| `conference` | String |  |
| `id` | String |  |
| `location` | String |  |
| `playoff_round` | String |  |
| `display_order` | String | Position of the bracket slot within its round, controlling top-to-bottom rendering. |
| `season` | String |  |
| `max_games` | String | Maximum number of games the playoff series can run to. |
| `winner_bracket_slot_id` | String | Identifier of the bracket slot the winner of this slot advances into. |
| `playoff_series` | String | JSON-encoded playoff-series node the game belongs to. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_playoff_bracket-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_playoff_bracket-example}

```python
yahoo_playoff_bracket()
```

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
| `game_id` | String |  |
| `playoff_series` | String | JSON-encoded playoff-series node the game belongs to. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_playoff_series_game-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_playoff_series_game-example}

```python
yahoo_playoff_series_game()
```

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
| `game_id` | String |  |
| `polymarket_url` | String | Polymarket prediction-market URL for wagering on the game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_polymarket_game-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_polymarket_game-example}

```python
yahoo_polymarket_game()
```

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
| `league_seasons` | String | JSON-encoded list of the seasons for which Yahoo carries data for this league. |
| `current_season` | Int64 | Season the league is currently playing, as the four-digit starting year. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_racing_schedule-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_racing_schedule-example}

```python
yahoo_racing_schedule()
```
