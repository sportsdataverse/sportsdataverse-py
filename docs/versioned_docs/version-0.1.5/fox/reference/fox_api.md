---
title: FOX — Fox Sports API (api.foxsports.com)
sidebar_label: Fox Sports API (api.foxsports.com)
description: "FOX — Fox Sports API (api.foxsports.com) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
toc_max_heading_level: 2
---
# FOX — Fox Sports API (api.foxsports.com)

`sportsdataverse.fox` — 33 endpoints.

## fox_api_scoreboard

GET /bifrost/v1/{sport}/scoreboard/main -- Fox Sports API scoreboard.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/scoreboard/main`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/scoreboard/main?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1](https://api.foxsports.com/bifrost/v1/nfl/scoreboard/main?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |
| `groupId` | `group_id` |  |  | `Y` | Conference / group id filter. |

### Returns {#fox_api_scoreboard-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | character |  |
| `parameters_season_type` | character |  |
| `parameters_week` | character |  |
| `subtitle` | character |  |
| `title` | character |  |
| `uri` | character |  |
| `web_url` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_scoreboard-example}

```python
fox_api_scoreboard(sport='nfl')
```

_Last validated n/a._

## fox_api_scorechip

GET /bifrost/v1/{sport}/scorechip/{chip_id} -- one game's score chip (this route 400s if api-version is sent).

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/scorechip/{chip_id}`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/scorechip/nfl11195?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq](https://api.foxsports.com/bifrost/v1/nfl/scorechip/nfl11195?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `chip_id` | `chip_id` |  | `Y` |  | Score-chip id: the league slug plus the numeric game id, e.g. ``nfl11195``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |

### Returns {#fox_api_scorechip-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `alt_sort_key` | character |  |
| `content_type` | character |  |
| `content_uri` | character |  |
| `entity_link_analytics_sport` | character |  |
| `entity_link_content_type` | character |  |
| `entity_link_content_uri` | character |  |
| `entity_link_layout_path` | character |  |
| `entity_link_layout_tokens_away_uri` | character |  |
| `entity_link_layout_tokens_event_uri` | character |  |
| `entity_link_layout_tokens_home_uri` | character |  |
| `entity_link_layout_tokens_id` | character |  |
| `entity_link_type` | character |  |
| `entity_link_web_url` | character |  |
| `event_headline` | character |  |
| `event_status` | integer |  |
| `event_time` | character |  |
| `favorite_entities` | character |  |
| `id` | character |  |
| `importance` | integer |  |
| `is_tba` | logical |  |
| `league` | character |  |
| `lower_team_alternate_logo_url` | character |  |
| `lower_team_has_possession` | logical |  |
| `lower_team_image_alt_text` | character |  |
| `lower_team_image_type` | character |  |
| `lower_team_is_loser` | logical |  |
| `lower_team_logo_url` | character |  |
| `lower_team_long_name` | character |  |
| `lower_team_name` | character |  |
| `lower_team_record` | character |  |
| `lower_team_score` | integer |  |
| `lower_team_stacked_name_bottom` | character |  |
| `lower_team_stacked_name_top` | character |  |
| `lower_team_uri` | character |  |
| `odds_line` | character |  |
| `over_under_line` | character |  |
| `sort_key` | character |  |
| `status_line` | character |  |
| `template` | character |  |
| `upper_team_alternate_logo_url` | character |  |
| `upper_team_has_possession` | logical |  |
| `upper_team_image_alt_text` | character |  |
| `upper_team_image_type` | character |  |
| `upper_team_is_loser` | logical |  |
| `upper_team_logo_url` | character |  |
| `upper_team_long_name` | character |  |
| `upper_team_name` | character |  |
| `upper_team_record` | character |  |
| `upper_team_score` | integer |  |
| `upper_team_stacked_name_bottom` | character |  |
| `upper_team_stacked_name_top` | character |  |
| `upper_team_uri` | character |  |
| `uri` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_scorechip-example}

```python
fox_api_scorechip(sport='nfl', chip_id='nfl11195')
```

_Last validated n/a._

## fox_api_topevents_scoreboard_segment

GET /bifrost/v1/topevents/scoreboard/segment/{segment} -- Fox Sports API topevents scoreboard segment.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/topevents/scoreboard/segment/{segment}`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/topevents/scoreboard/segment/1?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1](https://api.foxsports.com/bifrost/v1/topevents/scoreboard/segment/1?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `segment` | `segment` |  | `Y` |  | Top-events scoreboard segment id (``0`` / ``1`` ... as listed by ``topevents/scoreboard/main``); NOT a league ``<season>-<week>-<type>`` id. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_topevents_scoreboard_segment-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `segment_id` | character |  |
| `section_id` | character |  |
| `section_title` | character |  |
| `game_id` | character |  |
| `chip_id` | character |  |
| `league` | character |  |
| `date` | character |  |
| `event_status` | integer |  |
| `status` | character |  |
| `tv_station` | character |  |
| `headline` | character |  |
| `odds_line` | character |  |
| `over_under_line` | character |  |
| `home_team` | character |  |
| `home_team_id` | character |  |
| `home_score` | integer |  |
| `home_record` | character |  |
| `away_team` | character |  |
| `away_team_id` | character |  |
| `away_score` | integer |  |
| `away_record` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_topevents_scoreboard_segment-example}

```python
fox_api_topevents_scoreboard_segment(segment='1')
```

_Last validated n/a._

## fox_api_league_conferences

GET /bifrost/v1/{sport}/league/conferences -- Fox Sports API league conferences.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/league/conferences`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/cfb/league/conferences?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1](https://api.foxsports.com/bifrost/v1/cfb/league/conferences?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_league_conferences-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `group` | character |  |
| `fox_id` | character |  |
| `abbreviation` | character |  |
| `name` | character |  |
| `content_uri` | character |  |
| `content_type` | character |  |
| `web_url` | character |  |
| `color` | character |  |
| `logo_url` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_league_conferences-example}

```python
fox_api_league_conferences(sport='cfb')
```

_Last validated n/a._

## fox_api_league_header

GET /bifrost/v1/{sport}/league/header -- Fox Sports API league header.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/league/header`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/league/header?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1](https://api.foxsports.com/bifrost/v1/nfl/league/header?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_league_header-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `template` | character |  |
| `title` | character |  |
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `content_uri` | character |  |
| `content_type` | character |  |
| `color` | character |  |
| `logo_url` | character |  |
| `image_alt_text` | character |  |
| `rank` | character |  |
| `details` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_league_header-example}

```python
fox_api_league_header(sport='nfl')
```

_Last validated n/a._

## fox_api_league_odds

GET /bifrost/v1/{sport}/league/odds -- Fox Sports API league odds.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/league/odds`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/league/odds?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1](https://api.foxsports.com/bifrost/v1/nfl/league/odds?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |
| `groupId` | `group_id` |  |  | `Y` | Conference / group id filter. |

### Returns {#fox_api_league_odds-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `model_gambling_text_bet_text` | character |  |
| `model_gambling_text_winnings_text` | character |  |
| `model_image_alt_text` | character |  |
| `model_image_alt_url` | character |  |
| `model_image_type` | character |  |
| `model_image_url` | character |  |
| `model_info_date` | character |  |
| `model_info_sub_text` | character |  |
| `model_main_text` | character |  |
| `model_odds_bet_slip_bet_display` | character |  |
| `model_odds_bet_slip_bet_increment` | integer |  |
| `model_odds_bet_slip_bet_index` | integer |  |
| `model_odds_bet_slip_bet_max` | integer |  |
| `model_odds_bet_slip_bet_min` | integer |  |
| `model_odds_bet_slip_description` | character |  |
| `model_odds_bet_slip_disclaimer_text` | character |  |
| `model_odds_bet_slip_event_time` | character |  |
| `model_odds_bet_slip_event_title` | character |  |
| `model_odds_bet_slip_image_alt_text` | character |  |
| `model_odds_bet_slip_image_alt_url` | character |  |
| `model_odds_bet_slip_image_type` | character |  |
| `model_odds_bet_slip_image_url` | character |  |
| `model_odds_bet_slip_odds_display` | character |  |
| `model_odds_bet_slip_payout_multiplier` | double |  |
| `model_odds_bet_slip_tracking_data_bet_entity_name` | character |  |
| `model_odds_bet_slip_tracking_data_bet_entity_uri` | character |  |
| `model_odds_bet_slip_tracking_data_bet_event_status` | integer |  |
| `model_odds_bet_slip_tracking_data_bet_event_uri` | character |  |
| `model_odds_bet_slip_tracking_data_bet_outcome_line` | character |  |
| `model_odds_bet_slip_tracking_data_bet_outcome_title` | character |  |
| `model_odds_bet_slip_tracking_data_content_entity_uri` | character |  |
| `model_odds_bet_slip_tracking_data_page_start_source` | character |  |
| `model_odds_sub_text` | character |  |
| `model_odds_text` | character |  |
| `model_odds_title` | character |  |
| `model_sub_text` | character |  |
| `template` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_league_odds-example}

```python
fox_api_league_odds(sport='nfl')
```

_Last validated n/a._

## fox_api_league_playernews

GET /bifrost/v1/{sport}/league/playernews -- Fox Sports API league playernews.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/league/playernews`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/league/playernews?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1](https://api.foxsports.com/bifrost/v1/nfl/league/playernews?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_league_playernews-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `alternate_image_url` | character |  |
| `date` | character |  |
| `description` | character |  |
| `entity_link_alternate_image_url` | character |  |
| `entity_link_analytics_name` | character |  |
| `entity_link_analytics_sport` | character |  |
| `entity_link_color` | character |  |
| `entity_link_content_type` | character |  |
| `entity_link_content_uri` | character |  |
| `entity_link_image_alt_text` | character |  |
| `entity_link_image_type` | character |  |
| `entity_link_image_url` | character |  |
| `entity_link_layout_path` | character |  |
| `entity_link_layout_tokens_content_uri` | character |  |
| `entity_link_layout_tokens_id` | character |  |
| `entity_link_title` | character |  |
| `entity_link_type` | character |  |
| `entity_link_web_url` | character |  |
| `headline` | character |  |
| `image_alt_text` | character |  |
| `image_type` | character |  |
| `image_url` | character |  |
| `impact` | character |  |
| `impact_title` | character |  |
| `source` | character |  |
| `subtitle` | character |  |
| `title` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_league_playernews-example}

```python
fox_api_league_playernews(sport='nfl')
```

_Last validated n/a._

## fox_api_league_polls

GET /bifrost/v1/{sport}/league/polls -- Fox Sports API league polls.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/league/polls`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/cfb/league/polls?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1](https://api.foxsports.com/bifrost/v1/cfb/league/polls?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_league_polls-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `section` | character |  |
| `ranking` | character |  |
| `v1` | character |  |
| `v2` | character |  |
| `pts` | character |  |
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `team` | character |  |
| `rank_change` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_league_polls-example}

```python
fox_api_league_polls(sport='cfb')
```

_Last validated n/a._

## fox_api_league_schedule

GET /bifrost/v1/{sport}/league/schedule -- Fox Sports API league schedule.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/league/schedule`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/league/schedule?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1](https://api.foxsports.com/bifrost/v1/nfl/league/schedule?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_league_schedule-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | character |  |
| `parameters_season_type` | character |  |
| `parameters_week` | character |  |
| `subtitle` | character |  |
| `title` | character |  |
| `uri` | character |  |
| `web_url` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_league_schedule-example}

```python
fox_api_league_schedule(sport='nfl')
```

_Last validated n/a._

## fox_api_league_scores

GET /bifrost/v1/{sport}/league/scores -- Fox Sports API league scores.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/league/scores`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/league/scores?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1](https://api.foxsports.com/bifrost/v1/nfl/league/scores?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_league_scores-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | character |  |
| `parameters_season_type` | character |  |
| `parameters_week` | character |  |
| `subtitle` | character |  |
| `title` | character |  |
| `uri` | character |  |
| `web_url` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_league_scores-example}

```python
fox_api_league_scores(sport='nfl')
```

_Last validated n/a._

## fox_api_league_scores_segment

GET /bifrost/v1/{sport}/league/scores-segment/{segment_id} -- Fox Sports API league scores segment.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/league/scores-segment/{segment_id}`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/league/scores-segment/2026-3-1?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1](https://api.foxsports.com/bifrost/v1/nfl/league/scores-segment/2026-3-1?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `segment_id` | `segment_id` |  | `Y` |  | Scores segment id, ``<season>-<week>-<seasonTypeCode>``, e.g. ``2026-3-1``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |
| `groupId` | `group_id` |  |  | `Y` | Conference / group id filter. |

### Returns {#fox_api_league_scores_segment-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `segment_id` | character |  |
| `section_id` | character |  |
| `section_title` | character |  |
| `game_id` | character |  |
| `chip_id` | character |  |
| `league` | character |  |
| `date` | character |  |
| `event_status` | integer |  |
| `status` | character |  |
| `tv_station` | character |  |
| `headline` | character |  |
| `odds_line` | character |  |
| `over_under_line` | character |  |
| `home_team` | character |  |
| `home_team_id` | character |  |
| `home_score` | integer |  |
| `home_record` | character |  |
| `away_team` | character |  |
| `away_team_id` | character |  |
| `away_score` | integer |  |
| `away_record` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_league_scores_segment-example}

```python
fox_api_league_scores_segment(sport='nfl', segment_id='2026-3-1')
```

_Last validated n/a._

## fox_api_league_standings

GET /bifrost/v1/{sport}/league/standings -- Fox Sports API league standings.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/league/standings`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/league/standings?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1](https://api.foxsports.com/bifrost/v1/nfl/league/standings?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_league_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `section` | character |  |
| `afc_east` | character |  |
| `v1` | character |  |
| `w_l_t` | character |  |
| `pct` | character |  |
| `pf` | character |  |
| `pa` | character |  |
| `home` | character |  |
| `away` | character |  |
| `conf` | character |  |
| `div` | character |  |
| `strk` | character |  |
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `afc_north` | character |  |
| `afc_south` | character |  |
| `afc_west` | character |  |
| `nfc_east` | character |  |
| `nfc_north` | character |  |
| `nfc_south` | character |  |
| `nfc_west` | character |  |
| `american_football_conference` | character |  |
| `national_football_conference` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_league_standings-example}

```python
fox_api_league_standings(sport='nfl')
```

_Last validated n/a._

## fox_api_league_stats

GET /bifrost/v1/{sport}/league/stats -- Fox Sports API league stats.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/league/stats`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/league/stats?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1](https://api.foxsports.com/bifrost/v1/nfl/league/stats?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_league_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `condensed_uri` | character |  |
| `image_alt_text` | character |  |
| `image_type` | character |  |
| `image_url` | character |  |
| `name` | character |  |
| `selection_id` | character |  |
| `short_name` | character |  |
| `stat_abbreviation` | character |  |
| `stat_value` | character |  |
| `team_abbreviation` | character |  |
| `template` | character |  |
| `title` | character |  |
| `web_url` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_league_stats-example}

```python
fox_api_league_stats(sport='nfl')
```

_Last validated n/a._

## fox_api_league_stats_con

GET /bifrost/v1/{sport}/league/stats-con/{who}/{category}/{page} -- Fox Sports API league stats con.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/league/stats-con/{who}/{category}/{page}`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/league/stats-con/player/passing/1?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1](https://api.foxsports.com/bifrost/v1/nfl/league/stats-con/player/passing/1?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `who` | `who` |  | `Y` |  | Stat subject, e.g. ``player`` or ``team``. |
| `category` | `category` |  | `Y` |  | Stat category slug, e.g. ``passing``. |
| `page` | `page` |  | `Y` |  | Results page number. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |
| `groupId` | `group_id` |  |  | `Y` | Conference / group id filter. |

### Returns {#fox_api_league_stats_con-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `columns` | character |  |
| `entity_link_alternate_image_url` | character |  |
| `entity_link_analytics_name` | character |  |
| `entity_link_analytics_sport` | character |  |
| `entity_link_color` | character |  |
| `entity_link_content_type` | character |  |
| `entity_link_content_uri` | character |  |
| `entity_link_image_alt_text` | character |  |
| `entity_link_image_type` | character |  |
| `entity_link_image_url` | character |  |
| `entity_link_layout_path` | character |  |
| `entity_link_layout_tokens_content_uri` | character |  |
| `entity_link_layout_tokens_id` | character |  |
| `entity_link_title` | character |  |
| `entity_link_type` | character |  |
| `entity_link_web_url` | character |  |
| `is_cutoff` | logical |  |
| `is_faded` | logical |  |
| `selected` | logical |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_league_stats_con-example}

```python
fox_api_league_stats_con(sport='nfl', who='player', category='passing', page='1')
```

_Last validated n/a._

## fox_api_league_teamnav

GET /bifrost/v1/{sport}/league/teamnav -- Fox Sports API league teamnav.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/league/teamnav`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/league/teamnav?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1](https://api.foxsports.com/bifrost/v1/nfl/league/teamnav?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_league_teamnav-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `group` | character |  |
| `fox_id` | character |  |
| `abbreviation` | character |  |
| `name` | character |  |
| `content_uri` | character |  |
| `content_type` | character |  |
| `web_url` | character |  |
| `color` | character |  |
| `logo_url` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_league_teamnav-example}

```python
fox_api_league_teamnav(sport='nfl')
```

_Last validated n/a._

## fox_api_event_data

GET /bifrost/v1/{sport}/event/{event_id}/data -- Fox Sports API event data.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/event/{event_id}/data`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/event/11195/data?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1](https://api.foxsports.com/bifrost/v1/nfl/event/11195/data?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `event_id` | `event_id` |  | `Y` |  | Numeric Fox game (event) id, e.g. ``11195``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_event_data-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `columns` | character |  |
| `entity_link_alternate_image_url` | character |  |
| `entity_link_analytics_name` | character |  |
| `entity_link_analytics_sport` | character |  |
| `entity_link_color` | character |  |
| `entity_link_content_type` | character |  |
| `entity_link_content_uri` | character |  |
| `entity_link_image_alt_text` | character |  |
| `entity_link_image_type` | character |  |
| `entity_link_image_url` | character |  |
| `entity_link_layout_path` | character |  |
| `entity_link_layout_tokens_content_uri` | character |  |
| `entity_link_layout_tokens_id` | character |  |
| `entity_link_title` | character |  |
| `entity_link_type` | character |  |
| `entity_link_web_url` | character |  |
| `is_cutoff` | logical |  |
| `is_faded` | logical |  |
| `selected` | logical |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_event_data-example}

```python
fox_api_event_data(sport='nfl', event_id='11195')
```

_Last validated n/a._

## fox_api_event_matchup

GET /bifrost/v1/{sport}/event/{event_id}/matchup -- Fox Sports API event matchup.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/event/{event_id}/matchup`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/event/11195/matchup?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1](https://api.foxsports.com/bifrost/v1/nfl/event/11195/matchup?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `event_id` | `event_id` |  | `Y` |  | Numeric Fox game (event) id, e.g. ``11195``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_event_matchup-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `entity_image_alt_text` | character |  |
| `entity_image_alt_url` | character |  |
| `entity_image_type` | character |  |
| `entity_image_url` | character |  |
| `entity_link_alternate_image_url` | character |  |
| `entity_link_analytics_name` | character |  |
| `entity_link_analytics_sport` | character |  |
| `entity_link_color` | character |  |
| `entity_link_content_type` | character |  |
| `entity_link_content_uri` | character |  |
| `entity_link_image_alt_text` | character |  |
| `entity_link_image_type` | character |  |
| `entity_link_image_url` | character |  |
| `entity_link_layout_path` | character |  |
| `entity_link_layout_tokens_content_uri` | character |  |
| `entity_link_layout_tokens_id` | character |  |
| `entity_link_title` | character |  |
| `entity_link_type` | character |  |
| `entity_link_web_url` | character |  |
| `text` | character |  |
| `title` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_event_matchup-example}

```python
fox_api_event_matchup(sport='nfl', event_id='11195')
```

_Last validated n/a._

## fox_api_event_odds

GET /bifrost/v1/{sport}/event/{event_id}/odds -- Fox Sports API event odds.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/event/{event_id}/odds`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/event/11195/odds?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1](https://api.foxsports.com/bifrost/v1/nfl/event/11195/odds?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `event_id` | `event_id` |  | `Y` |  | Numeric Fox game (event) id, e.g. ``11195``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_event_odds-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `odds1` | character |  |
| `odds2` | character |  |
| `point` | integer |  |
| `text` | character |  |
| `text1` | character |  |
| `text2` | character |  |
| `timestamp` | character |  |
| `annotation_color` | character |  |
| `annotation_text` | character |  |
| `annotation_type` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_event_odds-example}

```python
fox_api_event_odds(sport='nfl', event_id='11195')
```

_Last validated n/a._

## fox_api_event_recap

GET /bifrost/v1/{sport}/event/{event_id}/recap -- Fox Sports API event recap.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/event/{event_id}/recap`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/event/11195/recap?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1](https://api.foxsports.com/bifrost/v1/nfl/event/11195/recap?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `event_id` | `event_id` |  | `Y` |  | Numeric Fox game (event) id, e.g. ``11195``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_event_recap-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `entity_image_alt_text` | character |  |
| `entity_image_alt_url` | character |  |
| `entity_image_type` | character |  |
| `entity_image_url` | character |  |
| `entity_link_alternate_image_url` | character |  |
| `entity_link_analytics_name` | character |  |
| `entity_link_analytics_sport` | character |  |
| `entity_link_color` | character |  |
| `entity_link_content_type` | character |  |
| `entity_link_content_uri` | character |  |
| `entity_link_image_alt_text` | character |  |
| `entity_link_image_type` | character |  |
| `entity_link_image_url` | character |  |
| `entity_link_layout_path` | character |  |
| `entity_link_layout_tokens_content_uri` | character |  |
| `entity_link_layout_tokens_id` | character |  |
| `entity_link_title` | character |  |
| `entity_link_type` | character |  |
| `entity_link_web_url` | character |  |
| `text` | character |  |
| `title` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_event_recap-example}

```python
fox_api_event_recap(sport='nfl', event_id='11195')
```

_Last validated n/a._

## fox_api_event_standings

GET /bifrost/v1/{sport}/event/{event_id}/standings -- Fox Sports API event standings.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/event/{event_id}/standings`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/event/11195/standings?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1](https://api.foxsports.com/bifrost/v1/nfl/event/11195/standings?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `event_id` | `event_id` |  | `Y` |  | Numeric Fox game (event) id, e.g. ``11195``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_event_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `section` | character |  |
| `nfc_north` | character |  |
| `v1` | character |  |
| `w_l_t` | character |  |
| `div` | character |  |
| `strk` | character |  |
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `nfc_south` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_event_standings-example}

```python
fox_api_event_standings(sport='nfl', event_id='11195')
```

_Last validated n/a._

## fox_api_team_gamelog

GET /bifrost/v1/{sport}/team/{team_id}/gamelog -- Fox Sports API team gamelog.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/team/{team_id}/gamelog`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/team/25/gamelog?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1](https://api.foxsports.com/bifrost/v1/nfl/team/25/gamelog?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `team_id` | `team_id` |  | `Y` |  | Numeric Fox team id, e.g. ``25``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_team_gamelog-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `emphasized` | logical |  |
| `index` | integer |  |
| `priority` | integer |  |
| `sortable` | character |  |
| `template` | character |  |
| `text` | character |  |
| `weight` | double |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_team_gamelog-example}

```python
fox_api_team_gamelog(sport='nfl', team_id='25')
```

_Last validated n/a._

## fox_api_team_header

GET /bifrost/v1/{sport}/team/{team_id}/header -- Fox Sports API team header.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/team/{team_id}/header`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/team/25/header?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1](https://api.foxsports.com/bifrost/v1/nfl/team/25/header?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `team_id` | `team_id` |  | `Y` |  | Numeric Fox team id, e.g. ``25``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_team_header-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `template` | character |  |
| `title` | character |  |
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `content_uri` | character |  |
| `content_type` | character |  |
| `color` | character |  |
| `logo_url` | character |  |
| `image_alt_text` | character |  |
| `rank` | character |  |
| `details` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_team_header-example}

```python
fox_api_team_header(sport='nfl', team_id='25')
```

_Last validated n/a._

## fox_api_team_roster

GET /bifrost/v1/{sport}/team/{team_id}/roster -- Fox Sports API team roster.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/team/{team_id}/roster`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/team/25/roster?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1](https://api.foxsports.com/bifrost/v1/nfl/team/25/roster?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `team_id` | `team_id` |  | `Y` |  | Numeric Fox team id, e.g. ``25``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_team_roster-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `position_group` | character |  |
| `player` | character |  |
| `pos` | character |  |
| `age` | character | Athlete age in years. |
| `ht` | character |  |
| `wt` | character |  |
| `college` | character |  |
| `athlete_id` | character | ESPN numeric identifier for the athlete. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_team_roster-example}

```python
fox_api_team_roster(sport='nfl', team_id='25')
```

_Last validated n/a._

## fox_api_team_standings

GET /bifrost/v1/{sport}/team/{team_id}/standings -- Fox Sports API team standings.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/team/{team_id}/standings`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/team/25/standings?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1](https://api.foxsports.com/bifrost/v1/nfl/team/25/standings?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `team_id` | `team_id` |  | `Y` |  | Numeric Fox team id, e.g. ``25``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_team_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `section` | character |  |
| `nfc_south` | character |  |
| `v1` | character |  |
| `w_l_t` | character |  |
| `pct` | character |  |
| `pf` | character |  |
| `pa` | character |  |
| `home` | character |  |
| `away` | character |  |
| `conf` | character |  |
| `div` | character |  |
| `strk` | character |  |
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `nfc_east` | character |  |
| `nfc_north` | character |  |
| `nfc_west` | character |  |
| `afc_east` | character |  |
| `afc_north` | character |  |
| `afc_south` | character |  |
| `afc_west` | character |  |
| `national_football_conference` | character |  |
| `american_football_conference` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_team_standings-example}

```python
fox_api_team_standings(sport='nfl', team_id='25')
```

_Last validated n/a._

## fox_api_team_stats

GET /bifrost/v1/{sport}/team/{team_id}/stats -- Fox Sports API team stats.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/team/{team_id}/stats`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/team/25/stats?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1](https://api.foxsports.com/bifrost/v1/nfl/team/25/stats?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `team_id` | `team_id` |  | `Y` |  | Numeric Fox team id, e.g. ``25``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_team_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `condensed_uri` | character |  |
| `image_alt_text` | character |  |
| `image_type` | character |  |
| `image_url` | character |  |
| `name` | character |  |
| `selection_id` | character |  |
| `short_name` | character |  |
| `stat_abbreviation` | character |  |
| `stat_value` | character |  |
| `template` | character |  |
| `title` | character |  |
| `web_url` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_team_stats-example}

```python
fox_api_team_stats(sport='nfl', team_id='25')
```

_Last validated n/a._

## fox_api_explore_browse

GET /bifrost/v1/explore/browse/{section}/main -- Fox Sports API explore browse.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/explore/browse/{section}/main`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/explore/browse/sports/main?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1](https://api.foxsports.com/bifrost/v1/explore/browse/sports/main?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `section` | `section` |  | `Y` |  | Browse section: ``sports``, ``players``, ``shows``, ``personalities`` or ``topics``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_explore_browse-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `group` | character |  |
| `fox_id` | character |  |
| `abbreviation` | character |  |
| `name` | character |  |
| `content_uri` | character |  |
| `content_type` | character |  |
| `web_url` | character |  |
| `color` | character |  |
| `logo_url` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_explore_browse-example}

```python
fox_api_explore_browse(section='sports')
```

_Last validated n/a._

## fox_api_explore_odds

GET /bifrost/v1/explore/odds/main -- Fox Sports API explore odds.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/explore/odds/main`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/explore/odds/main?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1](https://api.foxsports.com/bifrost/v1/explore/odds/main?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_explore_odds-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `external_id` | character | Provider-side identifier for the media item, matching the play id it accompanies. |
| `external_uri` | character |  |
| `isa_value` | character |  |
| `path` | character |  |
| `short_title` | character |  |
| `slug` | character |  |
| `spark_id` | character |  |
| `tag_type` | character |  |
| `title` | character |  |
| `type` | character |  |
| `uuidv5` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_explore_odds-example}

```python
fox_api_explore_odds()
```

_Last validated n/a._

## fox_api_search_content

GET /bifrost/v1/search/content -- Fox Sports API search content.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/search/content`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/search/content?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1&text=mahomes](https://api.foxsports.com/bifrost/v1/search/content?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1&text=mahomes)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |
| `text` | `text` |  |  | `Y` | Search text. |

### Returns {#fox_api_search_content-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `group` | character |  |
| `type` | character |  |
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `title` | character |  |
| `subtitle` | character |  |
| `content_type` | character |  |
| `content_uri` | character |  |
| `web_url` | character |  |
| `analytics_name` | character |  |
| `image_url` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_search_content-example}

```python
fox_api_search_content(text='mahomes')
```

_Last validated n/a._

## fox_api_search_entities

GET /bifrost/v1/search/entities -- Fox Sports API search entities.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/search/entities`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/search/entities?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1&text=mahomes](https://api.foxsports.com/bifrost/v1/search/entities?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1&text=mahomes)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |
| `text` | `text` |  |  | `Y` | Search text. |

### Returns {#fox_api_search_entities-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `group` | character |  |
| `type` | character |  |
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `title` | character |  |
| `subtitle` | character |  |
| `content_type` | character |  |
| `content_uri` | character |  |
| `web_url` | character |  |
| `analytics_name` | character |  |
| `image_url` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_search_entities-example}

```python
fox_api_search_entities(text='mahomes')
```

_Last validated n/a._

## fox_api_search_popular

GET /bifrost/v1/search/popular -- Fox Sports API search popular.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/search/popular`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/search/popular?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1](https://api.foxsports.com/bifrost/v1/search/popular?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_search_popular-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `group` | character |  |
| `type` | character |  |
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `title` | character |  |
| `subtitle` | character |  |
| `content_type` | character |  |
| `content_uri` | character |  |
| `web_url` | character |  |
| `analytics_name` | character |  |
| `image_url` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_search_popular-example}

```python
fox_api_search_popular()
```

_Last validated n/a._

## fox_api_trending_articles

GET /bifrost/v1/general/trending/articles -- Fox Sports API trending articles.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/general/trending/articles`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/general/trending/articles?apikey=SuNgfBgmTGS2xozZbnV6FcjGGRQrR8cg&api-version=1.1&duration=4](https://api.foxsports.com/bifrost/v1/general/trending/articles?apikey=SuNgfBgmTGS2xozZbnV6FcjGGRQrR8cg&api-version=1.1&duration=4)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports feed-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |
| `duration` | `duration` |  |  | `Y` | Trending look-back window. |
| `tags` | `tags` |  |  | `Y` | Comma-separated tag filter. |

### Returns {#fox_api_trending_articles-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | character |  |
| `spark_id` | character |  |
| `title` | character |  |
| `description` | character |  |
| `content_type` | character |  |
| `component_type` | character |  |
| `publication_date` | character |  |
| `last_published_date` | character |  |
| `canonical_url` | character |  |
| `thumbnail_url` | character |  |
| `playback_url` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_trending_articles-example}

```python
fox_api_trending_articles()
```

_Last validated n/a._

## fox_api_trending_videos

GET /bifrost/v1/general/trending/videos -- Fox Sports API trending videos.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/general/trending/videos`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/general/trending/videos?apikey=SuNgfBgmTGS2xozZbnV6FcjGGRQrR8cg&api-version=1.1&duration=4&maxItems=12](https://api.foxsports.com/bifrost/v1/general/trending/videos?apikey=SuNgfBgmTGS2xozZbnV6FcjGGRQrR8cg&api-version=1.1&duration=4&maxItems=12)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports feed-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |
| `duration` | `duration` |  |  | `Y` | Trending look-back window. |
| `maxItems` | `max_items` |  |  | `Y` | Maximum items to return. |

### Returns {#fox_api_trending_videos-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | character |  |
| `spark_id` | character |  |
| `title` | character |  |
| `description` | character |  |
| `content_type` | character |  |
| `component_type` | character |  |
| `publication_date` | character |  |
| `last_published_date` | character |  |
| `canonical_url` | character |  |
| `thumbnail_url` | character |  |
| `playback_url` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_trending_videos-example}

```python
fox_api_trending_videos()
```

_Last validated n/a._

## fox_api_foxpolls

GET /foxpolls/v1/polls -- Fox Sports API foxpolls.

**Endpoint URL:** `GET https://api.foxsports.com/foxpolls/v1/polls`

**Valid URL:** [https://api.foxsports.com/foxpolls/v1/polls?apikey=SuNgfBgmTGS2xozZbnV6FcjGGRQrR8cg&includeAnswers=true](https://api.foxsports.com/foxpolls/v1/polls?apikey=SuNgfBgmTGS2xozZbnV6FcjGGRQrR8cg&includeAnswers=true)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports feed-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `associatedEntityIds` | `associated_entity_ids` |  |  | `Y` | Comma-separated Fox entity ids the polls are associated with. |
| `includeAnswers` | `include_answers` |  |  | `Y` | Include poll answer options. |

### Returns {#fox_api_foxpolls-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `poll_id` | integer |  |
| `uri` | character |  |
| `title` | character |  |
| `type` | character |  |
| `background_image_url` | character |  |
| `background_image_version` | integer |  |
| `background_image_type` | character |  |
| `background_image_alt_text` | character |  |
| `background_image_additional_urls` | character |  |
| `min_submissions_needed` | integer |  |
| `max_submissions_allowed` | integer |  |
| `max_votes_per_user` | integer |  |
| `cta_text` | character |  |
| `cta_url` | character |  |
| `results_display_type` | character |  |
| `results_display_text` | character |  |
| `results_commentary` | character |  |
| `results_displayed_at` | character |  |
| `authorization_type` | character |  |
| `starts_at` | character |  |
| `ends_at` | character |  |
| `locks_at` | character |  |
| `answers` | character |  |
| `votes` | integer |  |
| `min_votes_to_display_post_lock` | integer |  |
| `ads_enabled` | logical |  |
| `background_image_additional_urls_resized_url` | character |  |
| `background_image` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_foxpolls-example}

```python
fox_api_foxpolls()
```

_Last validated n/a._
