# FOX — Fox Sports API (api.foxsports.com) — League

> FOX — Fox Sports API (api.foxsports.com) — League — function reference in sdv-py, the SportsDataverse Python package.

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

_Last validated n/a._
