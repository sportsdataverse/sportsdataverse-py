---
title: "Fox Sports — Fox Sports API (api.foxsports.com) — League"
sidebar_label: "League"
sidebar_position: 2
description: "Fox Sports — Fox Sports API (api.foxsports.com) — League — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# Fox Sports — Fox Sports API (api.foxsports.com) — League

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
| `group` | String |  |
| `fox_id` | String |  |
| `abbreviation` | String |  |
| `name` | String |  |
| `content_uri` | String |  |
| `content_type` | String |  |
| `web_url` | String |  |
| `color` | String |  |
| `logo_url` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fox_api_league_conferences-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fox_api_league_conferences-example}

```python
fox_api_league_conferences(sport='cfb')
```

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
| `template` | String |  |
| `title` | String |  |
| `entity_id` | String | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `content_uri` | String |  |
| `content_type` | String |  |
| `color` | String |  |
| `logo_url` | String |  |
| `image_alt_text` | String |  |
| `rank` | String |  |
| `details` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fox_api_league_header-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fox_api_league_header-example}

```python
fox_api_league_header(sport='nfl')
```

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
| `model_gambling_text_bet_text` | String |  |
| `model_gambling_text_winnings_text` | String |  |
| `model_image_alt_text` | String |  |
| `model_image_alt_url` | String |  |
| `model_image_type` | String |  |
| `model_image_url` | String |  |
| `model_info_date` | String |  |
| `model_info_sub_text` | String |  |
| `model_main_text` | String |  |
| `model_odds_bet_slip_bet_display` | String |  |
| `model_odds_bet_slip_bet_increment` | Int64 |  |
| `model_odds_bet_slip_bet_index` | Int64 |  |
| `model_odds_bet_slip_bet_max` | Int64 |  |
| `model_odds_bet_slip_bet_min` | Int64 |  |
| `model_odds_bet_slip_description` | String |  |
| `model_odds_bet_slip_disclaimer_text` | String |  |
| `model_odds_bet_slip_event_time` | String |  |
| `model_odds_bet_slip_event_title` | String |  |
| `model_odds_bet_slip_image_alt_text` | String |  |
| `model_odds_bet_slip_image_alt_url` | String |  |
| `model_odds_bet_slip_image_type` | String |  |
| `model_odds_bet_slip_image_url` | String |  |
| `model_odds_bet_slip_odds_display` | String |  |
| `model_odds_bet_slip_payout_multiplier` | Float64 |  |
| `model_odds_bet_slip_tracking_data_bet_entity_name` | String |  |
| `model_odds_bet_slip_tracking_data_bet_entity_uri` | String |  |
| `model_odds_bet_slip_tracking_data_bet_event_status` | Int64 |  |
| `model_odds_bet_slip_tracking_data_bet_event_uri` | String |  |
| `model_odds_bet_slip_tracking_data_bet_outcome_line` | String |  |
| `model_odds_bet_slip_tracking_data_bet_outcome_title` | String |  |
| `model_odds_bet_slip_tracking_data_content_entity_uri` | String |  |
| `model_odds_bet_slip_tracking_data_page_start_source` | String |  |
| `model_odds_sub_text` | String |  |
| `model_odds_text` | String |  |
| `model_odds_title` | String |  |
| `model_sub_text` | String |  |
| `template` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fox_api_league_odds-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fox_api_league_odds-example}

```python
fox_api_league_odds(sport='nfl')
```

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
| `alternate_image_url` | String |  |
| `date` | String |  |
| `description` | String |  |
| `entity_link_alternate_image_url` | String |  |
| `entity_link_analytics_name` | String |  |
| `entity_link_analytics_sport` | String |  |
| `entity_link_color` | String |  |
| `entity_link_content_type` | String |  |
| `entity_link_content_uri` | String |  |
| `entity_link_image_alt_text` | String |  |
| `entity_link_image_type` | String |  |
| `entity_link_image_url` | String |  |
| `entity_link_layout_path` | String |  |
| `entity_link_layout_tokens_content_uri` | String |  |
| `entity_link_layout_tokens_id` | String |  |
| `entity_link_title` | String |  |
| `entity_link_type` | String |  |
| `entity_link_web_url` | String |  |
| `headline` | String |  |
| `image_alt_text` | String |  |
| `image_type` | String |  |
| `image_url` | String |  |
| `impact` | String |  |
| `impact_title` | String |  |
| `source` | String |  |
| `subtitle` | String |  |
| `title` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fox_api_league_playernews-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fox_api_league_playernews-example}

```python
fox_api_league_playernews(sport='nfl')
```

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
| `section` | String |  |
| `ranking` | String |  |
| `v1` | String |  |
| `v2` | String |  |
| `pts` | String |  |
| `entity_id` | String | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `team` | String |  |
| `rank_change` | Int64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fox_api_league_polls-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fox_api_league_polls-example}

```python
fox_api_league_polls(sport='cfb')
```

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
| `id` | String |  |
| `parameters_season_type` | String |  |
| `parameters_week` | String |  |
| `subtitle` | String |  |
| `title` | String |  |
| `uri` | String |  |
| `web_url` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fox_api_league_schedule-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fox_api_league_schedule-example}

```python
fox_api_league_schedule(sport='nfl')
```

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
| `id` | String |  |
| `parameters_season_type` | String |  |
| `parameters_week` | String |  |
| `subtitle` | String |  |
| `title` | String |  |
| `uri` | String |  |
| `web_url` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fox_api_league_scores-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fox_api_league_scores-example}

```python
fox_api_league_scores(sport='nfl')
```

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
| `segment_id` | String |  |
| `section_id` | String |  |
| `section_title` | String |  |
| `game_id` | String |  |
| `chip_id` | String |  |
| `league` | String |  |
| `date` | String |  |
| `event_status` | Int64 |  |
| `status` | String |  |
| `tv_station` | String |  |
| `headline` | String |  |
| `odds_line` | String |  |
| `over_under_line` | String |  |
| `home_team` | String |  |
| `home_team_id` | String |  |
| `home_score` | Int64 |  |
| `home_record` | String |  |
| `away_team` | String |  |
| `away_team_id` | String |  |
| `away_score` | Int64 |  |
| `away_record` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fox_api_league_scores_segment-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fox_api_league_scores_segment-example}

```python
fox_api_league_scores_segment(sport='nfl', segment_id='2026-3-1')
```

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
| `section` | String |  |
| `afc_east` | String |  |
| `v1` | String |  |
| `w_l_t` | String |  |
| `pct` | String |  |
| `pf` | String |  |
| `pa` | String |  |
| `home` | String |  |
| `away` | String |  |
| `conf` | String |  |
| `div` | String |  |
| `strk` | String |  |
| `entity_id` | String | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `afc_north` | String |  |
| `afc_south` | String |  |
| `afc_west` | String |  |
| `nfc_east` | String |  |
| `nfc_north` | String |  |
| `nfc_south` | String |  |
| `nfc_west` | String |  |
| `american_football_conference` | String |  |
| `national_football_conference` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fox_api_league_standings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fox_api_league_standings-example}

```python
fox_api_league_standings(sport='nfl')
```

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
| `condensed_uri` | String |  |
| `image_alt_text` | String |  |
| `image_type` | String |  |
| `image_url` | String |  |
| `name` | String |  |
| `selection_id` | String |  |
| `short_name` | String |  |
| `stat_abbreviation` | String |  |
| `stat_value` | String |  |
| `team_abbreviation` | String |  |
| `template` | String |  |
| `title` | String |  |
| `web_url` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fox_api_league_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fox_api_league_stats-example}

```python
fox_api_league_stats(sport='nfl')
```

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
| `columns` | String |  |
| `entity_link_alternate_image_url` | String |  |
| `entity_link_analytics_name` | String |  |
| `entity_link_analytics_sport` | String |  |
| `entity_link_color` | String |  |
| `entity_link_content_type` | String |  |
| `entity_link_content_uri` | String |  |
| `entity_link_image_alt_text` | String |  |
| `entity_link_image_type` | String |  |
| `entity_link_image_url` | String |  |
| `entity_link_layout_path` | String |  |
| `entity_link_layout_tokens_content_uri` | String |  |
| `entity_link_layout_tokens_id` | String |  |
| `entity_link_title` | String |  |
| `entity_link_type` | String |  |
| `entity_link_web_url` | String |  |
| `is_cutoff` | Boolean |  |
| `is_faded` | Boolean |  |
| `selected` | Boolean |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fox_api_league_stats_con-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fox_api_league_stats_con-example}

```python
fox_api_league_stats_con(sport='nfl', who='player', category='passing', page='1')
```

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
| `group` | String |  |
| `fox_id` | String |  |
| `abbreviation` | String |  |
| `name` | String |  |
| `content_uri` | String |  |
| `content_type` | String |  |
| `web_url` | String |  |
| `color` | String |  |
| `logo_url` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fox_api_league_teamnav-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fox_api_league_teamnav-example}

```python
fox_api_league_teamnav(sport='nfl')
```
