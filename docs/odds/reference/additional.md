# Betting odds — additional Python functions

> Betting odds — additional Python functions — additional functions in sdv-py, the SportsDataverse Python package.

Hand-written wrappers, loaders, and helpers in `sportsdataverse.odds` that are not
generated API-endpoint wrappers (those have their own reference pages).

## The Odds API

### toa_event_markets {#toa_event_markets}

`toa_event_markets(sport: 'str', event_id: 'str', regions: 'str' = 'us', bookmakers: 'Optional[str]' = None, date_format: 'Optional[str]' = 'iso', api_key: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[DataFrameT, Dict]'`

Markets available for a single event

(`/v4/sports/{sport}/events/{eventId}/markets`). Quota: free.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sport` | `str` |  | Sport key from `toa_sports`. |
| `event_id` | `str` |  | Event id from `toa_sports_events`. |
| `regions` | `str` | `'us'` | Comma-separated bookmaker regions. |
| `bookmakers` | `Optional[str]` | `None` | Comma-separated bookmaker keys (takes precedence over `regions`). |
| `date_format` | `Optional[str]` | `'iso'` | `"iso"` or `"unix"`. |
| `api_key` | `Optional[str]` | `None` | The Odds API key (else `ODDS_API_KEY` env). |
| `return_parsed` | `bool` | `True` | Parse to a tidy DataFrame (default). `False` returns raw JSON. |
| `return_as_pandas` | `bool` | `False` | With `return_parsed`, return pandas instead of polars. |

**Returns**

A `polars`/`pandas` `DataFrame` (one row per bookmaker x available market) by default; raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `event_id` | String |  |
| `sport_key` | String |  |
| `sport_title` | String |  |
| `commence_time` | String |  |
| `home_team` | String |  |
| `away_team` | String |  |
| `bookmaker_key` | String |  |
| `bookmaker_title` | String |  |
| `market_key` | String |  |
| `market_last_update` | String |  |

**Example**

```python
from sportsdataverse.odds import toa_sports_events, toa_event_markets
eid = toa_sports_events(sport="americanfootball_nfl", return_parsed=False)[0]["id"]
toa_event_markets(sport="americanfootball_nfl", event_id=eid).head()
```

### toa_event_odds {#toa_event_odds}

`toa_event_odds(sport: 'str', event_id: 'str', regions: 'str' = 'us', markets: 'Optional[str]' = 'h2h', odds_format: 'Optional[str]' = 'american', date_format: 'Optional[str]' = 'iso', bookmakers: 'Optional[str]' = None, include_links: 'Optional[bool]' = None, include_sids: 'Optional[bool]' = None, include_bet_limits: 'Optional[bool]' = None, include_multipliers: 'Optional[bool]' = None, include_rotation_numbers: 'Optional[bool]' = None, api_key: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[DataFrameT, Dict]'`

Odds for a single event, incl. player-prop markets

(`/v4/sports/{sport}/events/{eventId}/odds`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sport` | `str` |  | Sport key from `toa_sports`. |
| `event_id` | `str` |  | Event id from `toa_sports_events`. |
| `regions` | `str` | `'us'` | Comma-separated bookmaker regions. |
| `markets` | `Optional[str]` | `'h2h'` | Comma-separated markets (event-level markets include player props). |
| `odds_format` | `Optional[str]` | `'american'` | `"american"` or `"decimal"`. |
| `date_format` | `Optional[str]` | `'iso'` | `"iso"` or `"unix"`. |
| `bookmakers` | `Optional[str]` | `None` | Comma-separated bookmaker keys (takes precedence over `regions`). |
| `include_links` | `Optional[bool]` | `None` | Include deep links. |
| `include_sids` | `Optional[bool]` | `None` | Include bookmaker source ids. |
| `include_bet_limits` | `Optional[bool]` | `None` | Include bet limits where available. |
| `include_multipliers` | `Optional[bool]` | `None` | Include SGP multipliers where available. |
| `include_rotation_numbers` | `Optional[bool]` | `None` | Include rotation numbers where available. |
| `api_key` | `Optional[str]` | `None` | The Odds API key (else `ODDS_API_KEY` env). |
| `return_parsed` | `bool` | `True` | Parse to a tidy DataFrame (default). `False` returns raw JSON. |
| `return_as_pandas` | `bool` | `False` | With `return_parsed`, return pandas instead of polars. |

**Returns**

A long-form `polars`/`pandas` `DataFrame` (one row per bookmaker x market x outcome) by default; raw JSON `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `event_id` | String |  |
| `sport_key` | String |  |
| `sport_title` | String |  |
| `commence_time` | String |  |
| `home_team` | String |  |
| `away_team` | String |  |
| `bookmaker_key` | String |  |
| `bookmaker_title` | String |  |
| `bookmaker_last_update` | String |  |
| `market_key` | String |  |
| `market_last_update` | String |  |
| `outcome_name` | String |  |
| `outcome_description` | String |  |
| `outcome_price` | Int64 |  |
| `outcome_point` | Float64 |  |
| `outcome_link` | String |  |
| `outcome_sid` | String |  |

**Example**

```python
from sportsdataverse.odds import toa_sports_events, toa_event_odds
eid = toa_sports_events(sport="americanfootball_nfl", return_parsed=False)[0]["id"]
toa_event_odds(sport="americanfootball_nfl", event_id=eid, markets="player_pass_tds").head()
```

### toa_event_odds_history {#toa_event_odds_history}

`toa_event_odds_history(sport: 'str', event_id: 'str', date: 'str' = '2023-11-29T22:45:00Z', regions: 'str' = 'us', markets: 'Optional[str]' = 'h2h', odds_format: 'Optional[str]' = 'american', date_format: 'Optional[str]' = 'iso', bookmakers: 'Optional[str]' = None, include_rotation_numbers: 'Optional[bool]' = None, include_multipliers: 'Optional[bool]' = None, api_key: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[DataFrameT, Dict]'`

Historical odds snapshot for a single event

(`/v4/historical/sports/{sport}/events/{eventId}/odds`). Paid plans only.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sport` | `str` |  | Sport key from `toa_sports`. |
| `event_id` | `str` |  | Event id from `toa_sports_events_history`. |
| `date` | `str` | `'2023-11-29T22:45:00Z'` | ISO8601 timestamp of the snapshot to fetch. |
| `regions` | `str` | `'us'` | Comma-separated bookmaker regions. |
| `markets` | `Optional[str]` | `'h2h'` | Comma-separated markets (event-level markets include player props). |
| `odds_format` | `Optional[str]` | `'american'` | `"american"` or `"decimal"`. |
| `date_format` | `Optional[str]` | `'iso'` | `"iso"` or `"unix"`. |
| `bookmakers` | `Optional[str]` | `None` | Comma-separated bookmaker keys. |
| `include_rotation_numbers` | `Optional[bool]` | `None` | Include rotation numbers where available. |
| `include_multipliers` | `Optional[bool]` | `None` | Include SGP multipliers where available. |
| `api_key` | `Optional[str]` | `None` | The Odds API key (else `ODDS_API_KEY` env). |
| `return_parsed` | `bool` | `True` | Parse to a tidy DataFrame (default). `False` returns raw JSON. |
| `return_as_pandas` | `bool` | `False` | With `return_parsed`, return pandas instead of polars. |

**Returns**

A long-form `polars`/`pandas` `DataFrame` (one row per bookmaker x market x outcome, stamped with the snapshot timestamps) by default; the raw JSON snapshot `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `snapshot_timestamp` | String |  |
| `previous_timestamp` | String |  |
| `next_timestamp` | String |  |
| `event_id` | String |  |
| `sport_key` | String |  |
| `sport_title` | String |  |
| `commence_time` | String |  |
| `home_team` | String |  |
| `away_team` | String |  |
| `bookmaker_key` | String |  |
| `bookmaker_title` | String |  |
| `bookmaker_last_update` | String |  |
| `market_key` | String |  |
| `market_last_update` | String |  |
| `outcome_name` | String |  |
| `outcome_description` | String |  |
| `outcome_price` | Int64 |  |
| `outcome_point` | Float64 |  |
| `outcome_link` | String |  |
| `outcome_sid` | String |  |

**Example**

```python
from sportsdataverse.odds import toa_event_odds_history
toa_event_odds_history(sport="americanfootball_nfl", event_id="...",
    date="2023-11-29T22:45:00Z").head()
```

### toa_sports {#toa_sports}

`toa_sports(all_sports: 'Optional[bool]' = None, api_key: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[DataFrameT, List]'`

List the sports/leagues available from The Odds API (`/v4/sports`). Quota: free.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `all_sports` | `Optional[bool]` | `None` | When `True`, include out-of-season sports too (default returns only in-season). Sent as the `all` query flag. |
| `api_key` | `Optional[str]` | `None` | The Odds API key (else `ODDS_API_KEY` env). |
| `return_parsed` | `bool` | `True` | Parse to a tidy DataFrame (default). `False` returns raw JSON. |
| `return_as_pandas` | `bool` | `False` | With `return_parsed`, return pandas instead of polars. |

**Returns**

A `polars`/`pandas` `DataFrame` (one row per sport) by default; the raw JSON `list` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `key` | String | The Odds API sport key, a lowercase slug such as 'americanfootball_ncaaf' or 'aussierules_afl', with futures keys ending in a suffix like '_championship_winner'; pass it as sport= to the other toa_* functions. |
| `group` | String |  |
| `title` | String |  |
| `description` | String |  |
| `active` | Boolean |  |
| `has_outrights` | Boolean | True for futures keys such as 'americanfootball_ncaaf_championship_winner' or 'americanfootball_nfl_super_bowl_winner', False for game leagues such as 'americanfootball_ncaaf'. |

**Example**

```python
from sportsdataverse.odds import toa_sports
toa_sports(all_sports=True).head()
```

### toa_sports_events {#toa_sports_events}

`toa_sports_events(sport: 'str' = 'americanfootball_nfl', date_format: 'Optional[str]' = 'iso', event_ids: 'Optional[str]' = None, commence_time_from: 'Optional[str]' = None, commence_time_to: 'Optional[str]' = None, include_rotation_numbers: 'Optional[bool]' = None, api_key: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[DataFrameT, List]'`

Upcoming + live events for a sport (`/v4/sports/{sport}/events`). Quota: free.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sport` | `str` | `'americanfootball_nfl'` | Sport key from `toa_sports`. |
| `date_format` | `Optional[str]` | `'iso'` | `"iso"` or `"unix"`. |
| `event_ids` | `Optional[str]` | `None` | Optional comma-separated event ids to filter to. |
| `commence_time_from` | `Optional[str]` | `None` | ISO8601 lower bound on event commence time. |
| `commence_time_to` | `Optional[str]` | `None` | ISO8601 upper bound on event commence time. |
| `include_rotation_numbers` | `Optional[bool]` | `None` | Include rotation numbers where available. |
| `api_key` | `Optional[str]` | `None` | The Odds API key (else `ODDS_API_KEY` env). |
| `return_parsed` | `bool` | `True` | Parse to a tidy DataFrame (default). `False` returns raw JSON. |
| `return_as_pandas` | `bool` | `False` | With `return_parsed`, return pandas instead of polars. |

**Returns**

A `polars`/`pandas` `DataFrame` (one row per event) by default; raw JSON `list` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `id` | String |  |
| `sport_key` | String | The Odds API sport key the event belongs to, the same slug toa_sports returns as key (sampled 'americanfootball_nfl', the function's default sport). |
| `sport_title` | String | Display title of the sport key, e.g. 'NFL' for americanfootball_nfl. |
| `commence_time` | String | Scheduled start of the event from The Odds API, an ISO-8601 UTC string with a trailing Z, kept as text. |
| `home_team` | String |  |
| `away_team` | String |  |

**Example**

```python
from sportsdataverse.odds import toa_sports_events
toa_sports_events(sport="americanfootball_nfl").head()
```

### toa_sports_events_history {#toa_sports_events_history}

`toa_sports_events_history(sport: 'str' = 'americanfootball_nfl', date: 'str' = '2023-11-29T22:45:00Z', date_format: 'Optional[str]' = 'iso', event_ids: 'Optional[str]' = None, commence_time_from: 'Optional[str]' = None, commence_time_to: 'Optional[str]' = None, include_rotation_numbers: 'Optional[bool]' = None, api_key: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[DataFrameT, Dict]'`

Historical events snapshot for a sport

(`/v4/historical/sports/{sport}/events`). Paid plans only.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sport` | `str` | `'americanfootball_nfl'` | Sport key from `toa_sports`. |
| `date` | `str` | `'2023-11-29T22:45:00Z'` | ISO8601 timestamp of the snapshot to fetch. |
| `date_format` | `Optional[str]` | `'iso'` | `"iso"` or `"unix"`. |
| `event_ids` | `Optional[str]` | `None` | Optional comma-separated event ids to filter to. |
| `commence_time_from` | `Optional[str]` | `None` | ISO8601 lower bound on event commence time. |
| `commence_time_to` | `Optional[str]` | `None` | ISO8601 upper bound on event commence time. |
| `include_rotation_numbers` | `Optional[bool]` | `None` | Include rotation numbers where available. |
| `api_key` | `Optional[str]` | `None` | The Odds API key (else `ODDS_API_KEY` env). |
| `return_parsed` | `bool` | `True` | Parse to a tidy DataFrame (default). `False` returns raw JSON. |
| `return_as_pandas` | `bool` | `False` | With `return_parsed`, return pandas instead of polars. |

**Returns**

A `polars`/`pandas` `DataFrame` (one row per event, stamped with the snapshot timestamps) by default; the raw JSON snapshot `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `snapshot_timestamp` | String | Time of the historical snapshot the API returned, an ISO-8601 UTC string with a trailing Z; the same on every row of a call (sampled '2023-11-29T22:40:39Z' for the default date '2023-11-29T22:45:00Z'). |
| `previous_timestamp` | String | Time of the snapshot immediately before the returned one, an ISO-8601 UTC string with a trailing Z; the same on every row of a call (sampled '2023-11-29T22:35:39Z'). |
| `next_timestamp` | String | Time of the snapshot immediately after the returned one, an ISO-8601 UTC string with a trailing Z; the same on every row of a call (sampled '2023-11-29T22:45:40Z'). |
| `id` | String |  |
| `sport_key` | String | The Odds API sport key the event belongs to, the same slug toa_sports returns as key (sampled 'americanfootball_nfl', the function's default sport). |
| `sport_title` | String | Display title of the sport key, e.g. 'NFL' for americanfootball_nfl. |
| `commence_time` | String | Scheduled start of the event from The Odds API, an ISO-8601 UTC string with a trailing Z, kept as text. |
| `home_team` | String |  |
| `away_team` | String |  |

**Example**

```python
from sportsdataverse.odds import toa_sports_events_history
toa_sports_events_history(sport="americanfootball_nfl", date="2023-11-29T22:45:00Z").head()
```

### toa_sports_odds {#toa_sports_odds}

`toa_sports_odds(sport: 'str' = 'americanfootball_nfl', regions: 'str' = 'us', markets: 'Optional[str]' = 'h2h', odds_format: 'Optional[str]' = 'american', date_format: 'Optional[str]' = 'iso', event_ids: 'Optional[str]' = None, bookmakers: 'Optional[str]' = None, commence_time_from: 'Optional[str]' = None, commence_time_to: 'Optional[str]' = None, include_links: 'Optional[bool]' = None, include_sids: 'Optional[bool]' = None, include_bet_limits: 'Optional[bool]' = None, include_rotation_numbers: 'Optional[bool]' = None, api_key: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[DataFrameT, List]'`

Current odds for a sport (`/v4/sports/{sport}/odds`), one row per outcome.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sport` | `str` | `'americanfootball_nfl'` | Sport key from `toa_sports` (e.g. `"americanfootball_nfl"`). |
| `regions` | `str` | `'us'` | Comma-separated bookmaker regions (`us`/`us2`/`uk`/`eu`/`au`). |
| `markets` | `Optional[str]` | `'h2h'` | Comma-separated markets (`h2h`, `spreads`, `totals`, `outrights`, ...). |
| `odds_format` | `Optional[str]` | `'american'` | `"american"` or `"decimal"`. |
| `date_format` | `Optional[str]` | `'iso'` | `"iso"` or `"unix"`. |
| `event_ids` | `Optional[str]` | `None` | Optional comma-separated event ids to filter to. |
| `bookmakers` | `Optional[str]` | `None` | Comma-separated bookmaker keys (takes precedence over `regions`). |
| `commence_time_from` | `Optional[str]` | `None` | ISO8601 lower bound on event commence time. |
| `commence_time_to` | `Optional[str]` | `None` | ISO8601 upper bound on event commence time. |
| `include_links` | `Optional[bool]` | `None` | Include bookmaker/market/outcome deep links. |
| `include_sids` | `Optional[bool]` | `None` | Include bookmaker-specific source ids. |
| `include_bet_limits` | `Optional[bool]` | `None` | Include bet limits where exchanges expose them. |
| `include_rotation_numbers` | `Optional[bool]` | `None` | Include rotation numbers where available. |
| `api_key` | `Optional[str]` | `None` | The Odds API key (else `ODDS_API_KEY` env). |
| `return_parsed` | `bool` | `True` | Parse to a tidy DataFrame (default). `False` returns raw JSON. |
| `return_as_pandas` | `bool` | `False` | With `return_parsed`, return pandas instead of polars. |

**Returns**

A long-form `polars`/`pandas` `DataFrame` (one row per event x bookmaker x market x outcome) by default; raw JSON `list` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `event_id` | String |  |
| `sport_key` | String | The Odds API sport key the event belongs to, the same slug toa_sports returns as key (sampled 'americanfootball_nfl', the function's default sport). |
| `sport_title` | String | Display title of the sport key, e.g. 'NFL' for americanfootball_nfl. |
| `commence_time` | String | Scheduled start of the event from The Odds API, an ISO-8601 UTC string with a trailing Z, kept as text. |
| `home_team` | String |  |
| `away_team` | String |  |
| `bookmaker_key` | String | The Odds API bookmaker key, a lowercase slug such as 'draftkings', 'fanduel' or 'williamhill_us'. |
| `bookmaker_title` | String | Bookmaker display name, e.g. 'DraftKings', 'BetOnline.ag', or 'Caesars' for williamhill_us. |
| `bookmaker_last_update` | String | When this bookmaker last updated its odds for the event, an ISO-8601 UTC string with a trailing Z. |
| `market_key` | String | Betting market of the outcome row; only 'h2h' (moneyline) appears in the sampled data, which used the default markets='h2h'. |
| `market_last_update` | String | When the bookmaker last updated this market for the event, an ISO-8601 UTC string with a trailing Z. |
| `outcome_name` | String | Outcome the price applies to; on the sampled h2h rows, a team's full name (e.g. 'Buffalo Bills'). |
| `outcome_description` | String | Copied from the outcome's description field; all null in the sampled data, which held only h2h outcomes. |
| `outcome_price` | Int64 | Price of the outcome as an integer in American odds under the default odds_format='american' (e.g. -238, 195). |
| `outcome_point` | Float64 | The outcome's point field (the spread or total line, e.g. -3.5) as a float; null for outcomes without one, such as h2h (moneyline), the only market in the sampled data. |
| `outcome_link` | String | Outcome deep link, only sent when include_links=True; all null in the sampled data, which did not request links. |
| `outcome_sid` | String | Bookmaker-specific source id for the outcome, only sent when include_sids=True; all null in the sampled data, which did not request sids. |

**Example**

```python
from sportsdataverse.odds import toa_sports_odds
toa_sports_odds(sport="americanfootball_nfl", regions="us", markets="h2h,spreads").head()
```

### toa_sports_odds_history {#toa_sports_odds_history}

`toa_sports_odds_history(sport: 'str' = 'americanfootball_nfl', date: 'str' = '2023-11-29T22:45:00Z', regions: 'str' = 'us', markets: 'Optional[str]' = 'h2h', odds_format: 'Optional[str]' = 'american', date_format: 'Optional[str]' = 'iso', event_ids: 'Optional[str]' = None, bookmakers: 'Optional[str]' = None, api_key: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[DataFrameT, Dict]'`

Historical odds snapshot for a sport

(`/v4/historical/sports/{sport}/odds`). Paid plans only.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sport` | `str` | `'americanfootball_nfl'` | Sport key from `toa_sports`. |
| `date` | `str` | `'2023-11-29T22:45:00Z'` | ISO8601 timestamp of the snapshot to fetch (the API returns the nearest snapshot at or before this time). |
| `regions` | `str` | `'us'` | Comma-separated bookmaker regions. |
| `markets` | `Optional[str]` | `'h2h'` | Comma-separated markets. |
| `odds_format` | `Optional[str]` | `'american'` | `"american"` or `"decimal"`. |
| `date_format` | `Optional[str]` | `'iso'` | `"iso"` or `"unix"`. |
| `event_ids` | `Optional[str]` | `None` | Optional comma-separated event ids to filter to. |
| `bookmakers` | `Optional[str]` | `None` | Comma-separated bookmaker keys. |
| `api_key` | `Optional[str]` | `None` | The Odds API key (else `ODDS_API_KEY` env). |
| `return_parsed` | `bool` | `True` | Parse to a tidy DataFrame (default). `False` returns raw JSON. |
| `return_as_pandas` | `bool` | `False` | With `return_parsed`, return pandas instead of polars. |

**Returns**

A long-form `polars`/`pandas` `DataFrame` (one row per outcome, stamped with the snapshot timestamps) by default; the raw JSON snapshot `dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `snapshot_timestamp` | String | Time of the historical snapshot the API returned, an ISO-8601 UTC string with a trailing Z; the same on every row of a call (sampled '2023-11-29T22:40:39Z' for the default date '2023-11-29T22:45:00Z'). |
| `previous_timestamp` | String | Time of the snapshot immediately before the returned one, an ISO-8601 UTC string with a trailing Z; the same on every row of a call (sampled '2023-11-29T22:35:39Z'). |
| `next_timestamp` | String | Time of the snapshot immediately after the returned one, an ISO-8601 UTC string with a trailing Z; the same on every row of a call (sampled '2023-11-29T22:45:40Z'). |
| `event_id` | String |  |
| `sport_key` | String | The Odds API sport key the event belongs to, the same slug toa_sports returns as key (sampled 'americanfootball_nfl', the function's default sport). |
| `sport_title` | String | Display title of the sport key, e.g. 'NFL' for americanfootball_nfl. |
| `commence_time` | String | Scheduled start of the event from The Odds API, an ISO-8601 UTC string with a trailing Z, kept as text. |
| `home_team` | String |  |
| `away_team` | String |  |
| `bookmaker_key` | String | The Odds API bookmaker key, a lowercase slug such as 'draftkings', 'fanduel' or 'williamhill_us'. |
| `bookmaker_title` | String | Bookmaker display name, e.g. 'DraftKings', 'BetOnline.ag', or 'Caesars' for williamhill_us. |
| `bookmaker_last_update` | String | When this bookmaker last updated its odds for the event, an ISO-8601 UTC string with a trailing Z. |
| `market_key` | String | Betting market of the outcome row; only 'h2h' (moneyline) appears in the sampled data, which used the default markets='h2h'. |
| `market_last_update` | String | When the bookmaker last updated this market for the event, an ISO-8601 UTC string with a trailing Z. |
| `outcome_name` | String | Outcome the price applies to; on the sampled h2h rows, a team's full name (e.g. 'Buffalo Bills'). |
| `outcome_description` | String | Copied from the outcome's description field; all null in the sampled data, which held only h2h outcomes. |
| `outcome_price` | Int64 | Price of the outcome as an integer in American odds under the default odds_format='american' (e.g. -238, 195). |
| `outcome_point` | Float64 | The outcome's point field (the spread or total line, e.g. -3.5) as a float; null for outcomes without one, such as h2h (moneyline), the only market in the sampled data. |
| `outcome_link` | String | Outcome deep link, only sent when include_links=True; all null in the sampled data, which did not request links. |
| `outcome_sid` | String | Bookmaker-specific source id for the outcome, only sent when include_sids=True; all null in the sampled data, which did not request sids. |

**Example**

```python
from sportsdataverse.odds import toa_sports_odds_history
toa_sports_odds_history(sport="americanfootball_nfl", date="2023-11-29T22:45:00Z").head()
```

### toa_sports_participants {#toa_sports_participants}

`toa_sports_participants(sport: 'str' = 'americanfootball_nfl', api_key: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[DataFrameT, List]'`

Teams / participants for a sport (`/v4/sports/{sport}/participants`). Quota: free.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sport` | `str` | `'americanfootball_nfl'` | Sport key from `toa_sports`. |
| `api_key` | `Optional[str]` | `None` | The Odds API key (else `ODDS_API_KEY` env). |
| `return_parsed` | `bool` | `True` | Parse to a tidy DataFrame (default). `False` returns raw JSON. |
| `return_as_pandas` | `bool` | `False` | With `return_parsed`, return pandas instead of polars. |

**Returns**

A `polars`/`pandas` `DataFrame` (one row per participant) by default; raw JSON `list` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `full_name` | String |  |
| `id` | String |  |

**Example**

```python
from sportsdataverse.odds import toa_sports_participants
toa_sports_participants(sport="americanfootball_nfl").head()
```

### toa_sports_scores {#toa_sports_scores}

`toa_sports_scores(sport: 'str' = 'americanfootball_nfl', days_from: 'Optional[int]' = None, date_format: 'Optional[str]' = 'iso', event_ids: 'Optional[str]' = None, api_key: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[DataFrameT, List]'`

Live + recently-completed scores for a sport (`/v4/sports/{sport}/scores`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sport` | `str` | `'americanfootball_nfl'` | Sport key from `toa_sports`. |
| `days_from` | `Optional[int]` | `None` | Include completed games from this many days ago (1-3). Omit for live + upcoming only. |
| `date_format` | `Optional[str]` | `'iso'` | `"iso"` or `"unix"`. |
| `event_ids` | `Optional[str]` | `None` | Optional comma-separated event ids to filter to. |
| `api_key` | `Optional[str]` | `None` | The Odds API key (else `ODDS_API_KEY` env). |
| `return_parsed` | `bool` | `True` | Parse to a tidy DataFrame (default). `False` returns raw JSON. |
| `return_as_pandas` | `bool` | `False` | With `return_parsed`, return pandas instead of polars. |

**Returns**

A `polars`/`pandas` `DataFrame` (one row per event) by default; raw JSON `list` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `id` | String |  |
| `sport_key` | String | The Odds API sport key the event belongs to, the same slug toa_sports returns as key (sampled 'americanfootball_nfl', the function's default sport). |
| `sport_title` | String | Display title of the sport key, e.g. 'NFL' for americanfootball_nfl. |
| `commence_time` | String | Scheduled start of the event from The Odds API, an ISO-8601 UTC string with a trailing Z, kept as text. |
| `completed` | Boolean |  |
| `home_team` | String |  |
| `away_team` | String |  |
| `scores` | String | Event scores as the parser's str() rendering of the API's list value (Python repr, not JSON); null on all 32 sampled events, none of which had started. |
| `last_update` | String | Time of the event's last score update; null on all 32 sampled events, none of which had started when the data was pulled. |

**Example**

```python
from sportsdataverse.odds import toa_sports_scores
toa_sports_scores(sport="americanfootball_nfl", days_from=3).head()
```

### toa_usage {#toa_usage}

`toa_usage(return_as_pandas: 'bool' = False) -> 'DataFrameT'`

Return the cached API-key quota from the most recent call (no network/quota cost).

Reads the `x-requests-remaining` / `x-requests-used` headers captured on the
last `sportsdataverse.odds` call; all values are `None` until a request
has been made in this session.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

A one-row `polars` (or `pandas`) `DataFrame` with `requests_remaining`, `requests_used` and `last_cost` (credits the last call consumed).

| col_name | type | description |
|---|---|---|
| `requests_remaining` | Int64 | Requests left on the API key, from the x-requests-remaining header of the most recent call in this Python session; null until a call has been made. |
| `requests_used` | Int64 | Requests the API key has used, from the x-requests-used header of the most recent call in this Python session; null until a call has been made. |
| `last_cost` | Int64 | Quota credits the most recent The Odds API call in this Python session used, from its x-requests-last header; 0 in the sample. Null until a call has been made. |

**Example**

```python
from sportsdataverse.odds import toa_sports, toa_usage
_ = toa_sports()
toa_usage()
```

## Kalshi

### kalshi_book_quotes {#kalshi_book_quotes}

`kalshi_book_quotes(book: 'dict | pl.DataFrame | pd.DataFrame', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Bid and ask ladders for both sides of a Kalshi market, asks derived from the opposite bids.

Kalshi's book holds bids only. A NO bid at `p` is a YES offer at `1 - p`, so the
YES asks are the NO bids flipped (and the NO asks the YES bids flipped), with the
same sizes.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `book` | `dict \| DataFrame \| DataFrame` |  | one market's order book: the raw `kalshi_orderbook(ticker, return_parsed=False)` dict, or the parsed frame `kalshi_orderbook(ticker)` returns (`side` / `price` / `size`). |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

One row per price level per side and quote, best level first (bids high to low, asks low to high): `side`, `quote`, `level`, `price`, `size`, `cum_size`. An empty book gives a zero-row frame with these columns.

| col_name | type | description |
|---|---|---|
| `side` | String | Contract side: yes (pays $1 if the event happens) or no. |
| `quote` | String | bid (the side's own resting bids) or ask (the opposite side's bids at 1 - price: a NO bid at p is a YES offer at 1 - p). |
| `level` | Int64 | Rank within the side's bid or ask ladder, 1 = best (highest bid, lowest ask). |
| `price` | Float64 | Price per contract in dollars from 0 to 1, rounded to the book's four decimals. |
| `size` | Float64 | Contracts resting at this price (Kalshi fixed-point counts, so fractions occur). |
| `cum_size` | Float64 | Contracts available at this level and every better one on the same ladder. |

**Raises**

| Exception | Description |
|---|---|
| `TypeError` | `book` is neither a dict nor a DataFrame. |
| `ValueError` | A DataFrame `book` lacks `side` / `price` / `size`, or a price falls outside 0 to 1 (a book in cents). |

**Example**

```python
from sportsdataverse.odds import kalshi_book_quotes, kalshi_orderbook

book = kalshi_orderbook(ticker="KXNFLGAME-26DEC07DALSEA-SEA")
quotes = kalshi_book_quotes(book)

# Pipeline next step (one line)

quotes.filter(level=1)
```

### kalshi_fill_price {#kalshi_fill_price}

`kalshi_fill_price(book: 'dict | pl.DataFrame | pd.DataFrame', size: 'float', *, side: 'str' = 'yes', action: 'str' = 'buy', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Average price to buy or sell `size` contracts against a Kalshi order book.

Buying walks that side's asks from the cheapest up; selling walks its bids from the
highest down. Each level fills up to its size before the next is touched. When the
book runs out first, the row reports the part that fills and `filled` is False.
Kalshi's trading fee is not included.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `book` | `dict \| DataFrame \| DataFrame` |  | one market's order book, raw dict or parsed frame, as for `kalshi_book_quotes`. |
| `size` | `float` |  | contracts to fill; must be positive. Kalshi sizes are fixed-point, so a fraction is allowed. |
| `side` | `str` | `'yes'` | `yes` or `no`. |
| `action` | `str` | `'buy'` | `buy` (lift the asks) or `sell` (hit the bids). |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

One row: `side`, `action`, `size_requested`, `size_filled`, `filled`, `avg_price`, `best_price`, `worst_price`, `slippage`, `cost` and `levels_used`. Prices are null when nothing fills.

| col_name | type | description |
|---|---|---|
| `side` | String | Contract side filled, yes or no. |
| `action` | String | buy (walks the side's asks from the cheapest up) or sell (walks its bids from the highest down). |
| `size_requested` | Float64 | Contracts asked for. |
| `size_filled` | Float64 | Contracts the book can fill; below size_requested when the ladder runs out. |
| `filled` | Boolean | True when the whole size_requested fills. |
| `avg_price` | Float64 | Volume-weighted price per contract in dollars over the filled contracts, before Kalshi's trading fee; null when nothing fills. |
| `best_price` | Float64 | Price of the ladder's best level (the touch); null when the ladder is empty. |
| `worst_price` | Float64 | Price of the last level the fill reaches; null when nothing fills. |
| `slippage` | Float64 | How much worse avg_price is than best_price, in dollars per contract (paid above the best ask, or received below the best bid); 0 when the touch fills it all. |
| `cost` | Float64 | Dollars paid (buy) or received (sell) for the filled contracts, before fees; null when nothing fills. |
| `levels_used` | Int64 | Price levels the fill touches. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | `size` is not a positive finite number, `side` / `action` is not one of the values above, or `book` fails as in `kalshi_book_quotes`. |
| `TypeError` | `book` is neither a dict nor a DataFrame. |

**Example**

```python
from sportsdataverse.odds import kalshi_fill_price, kalshi_orderbook

book = kalshi_orderbook(ticker="KXNFLGAME-26DEC07DALSEA-SEA")
fill = kalshi_fill_price(book, 500, side="yes", action="buy")

# Pipeline next step (one line)

fill.select("avg_price", "slippage", "filled")
```

## IDs and crosswalks

### MarketLeague {#MarketLeague}

`MarketLeague(espn_path: 'str', kalshi_series: 'str', gamma_series_id: 'str | None', draw_possible: 'bool' = False) -> None`

Where one league's games live on each venue.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `espn_path` | `str` |  | `{sport}/{league}` on ESPN's Site v2 API. |
| `kalshi_series` | `str` |  | the Kalshi series of game-winner markets. |
| `gamma_series_id` | `str \| None` |  | the Polymarket Gamma `series_id` of the league's game events. |
| `draw_possible` | `bool` | `False` | a game can end level, so a two-way price is incomplete. |

### join_espn_kalshi {#join_espn_kalshi}

`join_espn_kalshi(espn: 'pl.DataFrame', kalshi: 'pl.DataFrame', league: 'str', *, max_date_gap_days: 'int' = 3, method: 'str' = 'multiplicative', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Join ESPN games to Kalshi game-winner markets, one row per ESPN game.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `espn` | `DataFrame` |  | a parsed ESPN scoreboard (`espn_<league>_scoreboard(dates=...)`). |
| `kalshi` | `DataFrame` |  | parsed Kalshi markets of the league's winner series (`kalshi_markets(params={"series_ticker": MARKET_LEAGUES[league].kalshi_series, "status": "open"})`, every page). |
| `league` | `str` |  | a key of `MARKET_LEAGUES` (`nba`, `nfl`, `nhl`). |
| `max_date_gap_days` | `int` | `3` | the furthest a ticker's scheduled date may sit from the game's Eastern date and still match (a postponed game keeps its ticker). |
| `method` | `str` | `'multiplicative'` | de-vig method for `home_prob` / `away_prob` (`multiplicative` or `shin`, as in `sportsdataverse.wexp.market`). |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

One row per ESPN game: `game_id`, `game_date_et`, `away_abbreviation`, `home_abbreviation`, `kalshi_event_ticker`, `kalshi_scheduled_date`, `kalshi_away_ticker`, `kalshi_home_ticker`, the four `*_yes_bid` / `*_yes_ask` prices (0 to 1), `home_prob` / `away_prob` (de-vigged bid/ask midpoints), `prob_note` (why the probabilities are null) and `matched_by`. A game no market matched keeps its row with null market columns.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | ESPN game (event) id, in the dtype of the ESPN scoreboard frame passed in, so the result joins back to it. |
| `game_date_et` | Date | ESPN's start time as a US-Eastern calendar date. An exact match has the venue's date equal to it; a nearest_date or start_time match (see matched_by) can carry a different venue date. |
| `away_abbreviation` | String | ESPN abbreviation of the away team (the scoreboard's away side, even when the venue lists it as home). |
| `home_abbreviation` | String | ESPN abbreviation of the home team. |
| `kalshi_event_ticker` | String | Kalshi event ticker of the matched game, e.g. KXNBAGAME-26NOV22BOSGSW; null when no market matched. |
| `kalshi_scheduled_date` | Date | Date in the Kalshi ticker: the day the game was first scheduled for, kept when it is postponed. |
| `kalshi_away_ticker` | String | Kalshi market ticker that pays out if the away team wins. |
| `kalshi_home_ticker` | String | Kalshi market ticker that pays out if the home team wins. |
| `away_yes_bid` | Float64 | Best bid for YES on the away team's market, in dollars from 0 to 1 (Kalshi yes_bid_dollars). |
| `away_yes_ask` | Float64 | Best ask for YES on the away team's market, in dollars from 0 to 1 (Kalshi yes_ask_dollars). |
| `home_yes_bid` | Float64 | Best bid for YES on the home team's market, in dollars from 0 to 1 (Kalshi yes_bid_dollars). |
| `home_yes_ask` | Float64 | Best ask for YES on the home team's market, in dollars from 0 to 1 (Kalshi yes_ask_dollars). |
| `home_prob` | Float64 | Home win probability: the two bid/ask midpoints de-vigged together (multiplicative unless method='shin'). Kalshi pays $0.50 on each side of a tied NFL game, so there it counts a tie as half a win. Null when no market matched or a side has no two-sided book (see prob_note). |
| `away_prob` | Float64 | Away win probability, de-vigged with home_prob so the two sum to 1. |
| `prob_note` | String | Why home_prob and away_prob are null for a matched game (a missing or one-sided price, or a draw that is possible but unpriced); null when they are set, and null for an unmatched game (check matched_by). |
| `matched_by` | String | How the row was joined, '+'-separated: the team rule (code, alias or name), the date rule (scheduled_date, nearest_date, event_date or start_time) and 'swapped' when the venue lists the teams in the other order; null when nothing matched. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | `league` is not in `MARKET_LEAGUES`, or `espn` lacks the scoreboard columns. |

**Example**

```python
import polars as pl

from sportsdataverse.nba import espn_nba_scoreboard
from sportsdataverse.odds import join_espn_kalshi, kalshi_markets

games = espn_nba_scoreboard(dates="20261122")
markets = kalshi_markets(limit="1000", params={"series_ticker": "KXNBAGAME", "status": "open"})
joined = join_espn_kalshi(games, markets, "nba")

# Pipeline next step (one line)

joined.filter(pl.col("matched_by").is_null())
```

### join_espn_polymarket {#join_espn_polymarket}

`join_espn_polymarket(espn: 'pl.DataFrame', gamma_events: 'pl.DataFrame', league: 'str', *, max_start_gap_hours: 'float' = 6.0, return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Join ESPN games to Polymarket Gamma game events, one row per ESPN game.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `espn` | `DataFrame` |  | a parsed ESPN scoreboard (`espn_<league>_scoreboard(dates=...)`). |
| `gamma_events` | `DataFrame` |  | parsed Gamma events of the league's series (`polymarket_gamma_events(params={"series_id": MARKET_LEAGUES[league].gamma_series_id})`). Their `teams` column is the JSON list Gamma sends. |
| `league` | `str` |  | a key of `MARKET_LEAGUES`. |
| `max_start_gap_hours` | `float` | `6.0` | how far Gamma's `startTime` may sit from ESPN's start when the event date does not match. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

One row per ESPN game: `game_id`, `game_date_et`, `away_abbreviation`, `home_abbreviation`, `gamma_event_id`, `gamma_slug`, `polymarket_us_slug` (Gamma's `usId` as the Polymarket US gateway's market slug, `aec-nba-sa-sac-2026-11-06`) and `matched_by`. A game no event matched keeps its row with nulls.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | ESPN game (event) id, in the dtype of the ESPN scoreboard frame passed in, so the result joins back to it. |
| `game_date_et` | Date | ESPN's start time as a US-Eastern calendar date. An exact match has the venue's date equal to it; a nearest_date or start_time match (see matched_by) can carry a different venue date. |
| `away_abbreviation` | String | ESPN abbreviation of the away team (the scoreboard's away side, even when the venue lists it as home). |
| `home_abbreviation` | String | ESPN abbreviation of the home team. |
| `gamma_event_id` | String | Polymarket Gamma event id of the matched game, as text. |
| `gamma_slug` | String | Polymarket Gamma event slug, e.g. nba-sas-sac-2026-11-06. |
| `polymarket_us_slug` | String | Polymarket US gateway market slug built from the Gamma usId, e.g. aec-nba-sa-sac-2026-11-06; null when Gamma carries no usId. |
| `matched_by` | String | How the row was joined, '+'-separated: the team rule (code, alias or name), the date rule (scheduled_date, nearest_date, event_date or start_time) and 'swapped' when the venue lists the teams in the other order; null when nothing matched. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | `league` is not in `MARKET_LEAGUES`, or `espn` lacks the scoreboard columns. |

**Example**

```python
from sportsdataverse.nfl import espn_nfl_scoreboard
from sportsdataverse.odds import join_espn_polymarket, polymarket_gamma_events

games = espn_nfl_scoreboard(dates="20261122")
events = polymarket_gamma_events(closed="false", limit="100", params={"series_id": "12185"})
joined = join_espn_polymarket(games, events, "nfl")
```
