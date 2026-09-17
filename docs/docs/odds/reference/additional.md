---
title: ODDS — additional Python functions
sidebar_label: Additional functions
description: "ODDS — additional Python functions — additional functions in sdv-py, the SportsDataverse Python package."
sidebar_position: 50
---
# ODDS — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.odds`
not covered by the generated API-endpoint reference above.

## Other

### `toa_event_markets(sport: 'str', event_id: 'str', regions: 'str' = 'us', bookmakers: 'Optional[str]' = None, date_format: 'Optional[str]' = 'iso', api_key: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[DataFrameT, Dict]'` {#toa_event_markets}

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

**Example**

```python
from sportsdataverse.odds import toa_sports_events, toa_event_markets
eid = toa_sports_events(sport="americanfootball_nfl", return_parsed=False)[0]["id"]
toa_event_markets(sport="americanfootball_nfl", event_id=eid).head()
```

### `toa_event_odds(sport: 'str', event_id: 'str', regions: 'str' = 'us', markets: 'Optional[str]' = 'h2h', odds_format: 'Optional[str]' = 'american', date_format: 'Optional[str]' = 'iso', bookmakers: 'Optional[str]' = None, include_links: 'Optional[bool]' = None, include_sids: 'Optional[bool]' = None, include_bet_limits: 'Optional[bool]' = None, include_multipliers: 'Optional[bool]' = None, include_rotation_numbers: 'Optional[bool]' = None, api_key: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[DataFrameT, Dict]'` {#toa_event_odds}

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

**Example**

```python
from sportsdataverse.odds import toa_sports_events, toa_event_odds
eid = toa_sports_events(sport="americanfootball_nfl", return_parsed=False)[0]["id"]
toa_event_odds(sport="americanfootball_nfl", event_id=eid, markets="player_pass_tds").head()
```

### `toa_event_odds_history(sport: 'str', event_id: 'str', date: 'str' = '2023-11-29T22:45:00Z', regions: 'str' = 'us', markets: 'Optional[str]' = 'h2h', odds_format: 'Optional[str]' = 'american', date_format: 'Optional[str]' = 'iso', bookmakers: 'Optional[str]' = None, include_rotation_numbers: 'Optional[bool]' = None, include_multipliers: 'Optional[bool]' = None, api_key: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[DataFrameT, Dict]'` {#toa_event_odds_history}

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

**Example**

```python
from sportsdataverse.odds import toa_event_odds_history
toa_event_odds_history(sport="americanfootball_nfl", event_id="...",
    date="2023-11-29T22:45:00Z").head()
```

### `toa_sports(all_sports: 'Optional[bool]' = None, api_key: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[DataFrameT, List]'` {#toa_sports}

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
| `key` | character | The Odds API sport key, a lowercase slug such as 'americanfootball_ncaaf' or 'aussierules_afl', with futures keys ending in a suffix like '_championship_winner'; pass it as sport= to the other toa_* functions. |
| `group` | character | Stat group (e.g. "hitting", "pitching", "fielding"). |
| `title` | character | Specific role title for the assignment. |
| `description` | character | Long-form description text. |
| `active` | logical | TRUE if the row represents an active record (player / team / season). |
| `has_outrights` | logical | True for futures keys such as 'americanfootball_ncaaf_championship_winner' or 'americanfootball_nfl_super_bowl_winner', False for game leagues such as 'americanfootball_ncaaf'. |

**Example**

```python
from sportsdataverse.odds import toa_sports
toa_sports(all_sports=True).head()
```

### `toa_sports_events(sport: 'str' = 'americanfootball_nfl', date_format: 'Optional[str]' = 'iso', event_ids: 'Optional[str]' = None, commence_time_from: 'Optional[str]' = None, commence_time_to: 'Optional[str]' = None, include_rotation_numbers: 'Optional[bool]' = None, api_key: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[DataFrameT, List]'` {#toa_sports_events}

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
| `id` | character | ID of the player in the 'name' column. |
| `sport_key` | character | The Odds API sport key the event belongs to, the same slug toa_sports returns as key (sampled 'americanfootball_nfl', the function's default sport). |
| `sport_title` | character | Display title of the sport key, e.g. 'NFL' for americanfootball_nfl. |
| `commence_time` | character | Scheduled start of the event from The Odds API, an ISO-8601 UTC string with a trailing Z, kept as text. |
| `home_team` | character | Home team name. |
| `away_team` | character | Away team name. |

**Example**

```python
from sportsdataverse.odds import toa_sports_events
toa_sports_events(sport="americanfootball_nfl").head()
```

### `toa_sports_events_history(sport: 'str' = 'americanfootball_nfl', date: 'str' = '2023-11-29T22:45:00Z', date_format: 'Optional[str]' = 'iso', event_ids: 'Optional[str]' = None, commence_time_from: 'Optional[str]' = None, commence_time_to: 'Optional[str]' = None, include_rotation_numbers: 'Optional[bool]' = None, api_key: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[DataFrameT, Dict]'` {#toa_sports_events_history}

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
| `snapshot_timestamp` | character | Time of the historical snapshot the API returned, an ISO-8601 UTC string with a trailing Z; the same on every row of a call (sampled '2023-11-29T22:40:39Z' for the default date '2023-11-29T22:45:00Z'). |
| `previous_timestamp` | character | Time of the snapshot immediately before the returned one, an ISO-8601 UTC string with a trailing Z; the same on every row of a call (sampled '2023-11-29T22:35:39Z'). |
| `next_timestamp` | character | Time of the snapshot immediately after the returned one, an ISO-8601 UTC string with a trailing Z; the same on every row of a call (sampled '2023-11-29T22:45:40Z'). |
| `id` | character | ID of the player in the 'name' column. |
| `sport_key` | character | The Odds API sport key the event belongs to, the same slug toa_sports returns as key (sampled 'americanfootball_nfl', the function's default sport). |
| `sport_title` | character | Display title of the sport key, e.g. 'NFL' for americanfootball_nfl. |
| `commence_time` | character | Scheduled start of the event from The Odds API, an ISO-8601 UTC string with a trailing Z, kept as text. |
| `home_team` | character | Home team name. |
| `away_team` | character | Away team name. |

**Example**

```python
from sportsdataverse.odds import toa_sports_events_history
toa_sports_events_history(sport="americanfootball_nfl", date="2023-11-29T22:45:00Z").head()
```

### `toa_sports_odds(sport: 'str' = 'americanfootball_nfl', regions: 'str' = 'us', markets: 'Optional[str]' = 'h2h', odds_format: 'Optional[str]' = 'american', date_format: 'Optional[str]' = 'iso', event_ids: 'Optional[str]' = None, bookmakers: 'Optional[str]' = None, commence_time_from: 'Optional[str]' = None, commence_time_to: 'Optional[str]' = None, include_links: 'Optional[bool]' = None, include_sids: 'Optional[bool]' = None, include_bet_limits: 'Optional[bool]' = None, include_rotation_numbers: 'Optional[bool]' = None, api_key: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[DataFrameT, List]'` {#toa_sports_odds}

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
| `event_id` | character | Unique event / game identifier (ESPN). |
| `sport_key` | character | The Odds API sport key the event belongs to, the same slug toa_sports returns as key (sampled 'americanfootball_nfl', the function's default sport). |
| `sport_title` | character | Display title of the sport key, e.g. 'NFL' for americanfootball_nfl. |
| `commence_time` | character | Scheduled start of the event from The Odds API, an ISO-8601 UTC string with a trailing Z, kept as text. |
| `home_team` | character | Home team name. |
| `away_team` | character | Away team name. |
| `bookmaker_key` | character | The Odds API bookmaker key, a lowercase slug such as 'draftkings', 'fanduel' or 'williamhill_us'. |
| `bookmaker_title` | character | Bookmaker display name, e.g. 'DraftKings', 'BetOnline.ag', or 'Caesars' for williamhill_us. |
| `bookmaker_last_update` | character | When this bookmaker last updated its odds for the event, an ISO-8601 UTC string with a trailing Z. |
| `market_key` | character | Betting market of the outcome row; only 'h2h' (moneyline) appears in the sampled data, which used the default markets='h2h'. |
| `market_last_update` | character | When the bookmaker last updated this market for the event, an ISO-8601 UTC string with a trailing Z. |
| `outcome_name` | character | Outcome the price applies to; on the sampled h2h rows, a team's full name (e.g. 'Buffalo Bills'). |
| `outcome_description` | character | Copied from the outcome's description field; all null in the sampled data, which held only h2h outcomes. |
| `outcome_price` | integer | Price of the outcome as an integer in American odds under the default odds_format='american' (e.g. -238, 195). |
| `outcome_point` | character | Copied from the outcome's point field, stored as text; all null in the sampled data, which held only h2h (moneyline) outcomes. |
| `outcome_link` | character | Outcome deep link, only sent when include_links=True; all null in the sampled data, which did not request links. |
| `outcome_sid` | character | Bookmaker-specific source id for the outcome, only sent when include_sids=True; all null in the sampled data, which did not request sids. |

**Example**

```python
from sportsdataverse.odds import toa_sports_odds
toa_sports_odds(sport="americanfootball_nfl", regions="us", markets="h2h,spreads").head()
```

### `toa_sports_odds_history(sport: 'str' = 'americanfootball_nfl', date: 'str' = '2023-11-29T22:45:00Z', regions: 'str' = 'us', markets: 'Optional[str]' = 'h2h', odds_format: 'Optional[str]' = 'american', date_format: 'Optional[str]' = 'iso', event_ids: 'Optional[str]' = None, bookmakers: 'Optional[str]' = None, api_key: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[DataFrameT, Dict]'` {#toa_sports_odds_history}

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
| `snapshot_timestamp` | character | Time of the historical snapshot the API returned, an ISO-8601 UTC string with a trailing Z; the same on every row of a call (sampled '2023-11-29T22:40:39Z' for the default date '2023-11-29T22:45:00Z'). |
| `previous_timestamp` | character | Time of the snapshot immediately before the returned one, an ISO-8601 UTC string with a trailing Z; the same on every row of a call (sampled '2023-11-29T22:35:39Z'). |
| `next_timestamp` | character | Time of the snapshot immediately after the returned one, an ISO-8601 UTC string with a trailing Z; the same on every row of a call (sampled '2023-11-29T22:45:40Z'). |
| `event_id` | character | Unique event / game identifier (ESPN). |
| `sport_key` | character | The Odds API sport key the event belongs to, the same slug toa_sports returns as key (sampled 'americanfootball_nfl', the function's default sport). |
| `sport_title` | character | Display title of the sport key, e.g. 'NFL' for americanfootball_nfl. |
| `commence_time` | character | Scheduled start of the event from The Odds API, an ISO-8601 UTC string with a trailing Z, kept as text. |
| `home_team` | character | Home team name. |
| `away_team` | character | Away team name. |
| `bookmaker_key` | character | The Odds API bookmaker key, a lowercase slug such as 'draftkings', 'fanduel' or 'williamhill_us'. |
| `bookmaker_title` | character | Bookmaker display name, e.g. 'DraftKings', 'BetOnline.ag', or 'Caesars' for williamhill_us. |
| `bookmaker_last_update` | character | When this bookmaker last updated its odds for the event, an ISO-8601 UTC string with a trailing Z. |
| `market_key` | character | Betting market of the outcome row; only 'h2h' (moneyline) appears in the sampled data, which used the default markets='h2h'. |
| `market_last_update` | character | When the bookmaker last updated this market for the event, an ISO-8601 UTC string with a trailing Z. |
| `outcome_name` | character | Outcome the price applies to; on the sampled h2h rows, a team's full name (e.g. 'Buffalo Bills'). |
| `outcome_description` | character | Copied from the outcome's description field; all null in the sampled data, which held only h2h outcomes. |
| `outcome_price` | integer | Price of the outcome as an integer in American odds under the default odds_format='american' (e.g. -238, 195). |
| `outcome_point` | character | Copied from the outcome's point field, stored as text; all null in the sampled data, which held only h2h (moneyline) outcomes. |
| `outcome_link` | character | Outcome deep link, only sent when include_links=True; all null in the sampled data, which did not request links. |
| `outcome_sid` | character | Bookmaker-specific source id for the outcome, only sent when include_sids=True; all null in the sampled data, which did not request sids. |

**Example**

```python
from sportsdataverse.odds import toa_sports_odds_history
toa_sports_odds_history(sport="americanfootball_nfl", date="2023-11-29T22:45:00Z").head()
```

### `toa_sports_participants(sport: 'str' = 'americanfootball_nfl', api_key: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[DataFrameT, List]'` {#toa_sports_participants}

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
| `full_name` | character | Player's full name. |
| `id` | character | ID of the player in the 'name' column. |

**Example**

```python
from sportsdataverse.odds import toa_sports_participants
toa_sports_participants(sport="americanfootball_nfl").head()
```

### `toa_sports_scores(sport: 'str' = 'americanfootball_nfl', days_from: 'Optional[int]' = None, date_format: 'Optional[str]' = 'iso', event_ids: 'Optional[str]' = None, api_key: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[DataFrameT, List]'` {#toa_sports_scores}

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
| `id` | character | ID of the player in the 'name' column. |
| `sport_key` | character | The Odds API sport key the event belongs to, the same slug toa_sports returns as key (sampled 'americanfootball_nfl', the function's default sport). |
| `sport_title` | character | Display title of the sport key, e.g. 'NFL' for americanfootball_nfl. |
| `commence_time` | character | Scheduled start of the event from The Odds API, an ISO-8601 UTC string with a trailing Z, kept as text. |
| `completed` | logical | `TRUE` if the game is complete. |
| `home_team` | character | Home team name. |
| `away_team` | character | Away team name. |
| `scores` | character | Event scores as the parser's str() rendering of the API's list value (Python repr, not JSON); null on all 32 sampled events, none of which had started. |
| `last_update` | character | Time of the event's last score update; null on all 32 sampled events, none of which had started when the data was pulled. |

**Example**

```python
from sportsdataverse.odds import toa_sports_scores
toa_sports_scores(sport="americanfootball_nfl", days_from=3).head()
```

### `toa_usage(return_as_pandas: 'bool' = False) -> 'DataFrameT'` {#toa_usage}

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
| `requests_remaining` | integer | Requests left on the API key, from the x-requests-remaining header of the most recent call in this Python session; null until a call has been made. |
| `requests_used` | integer | Requests the API key has used, from the x-requests-used header of the most recent call in this Python session; null until a call has been made. |
| `last_cost` | integer | Quota credits the most recent The Odds API call in this Python session used, from its x-requests-last header; 0 in the sample. Null until a call has been made. |

**Example**

```python
from sportsdataverse.odds import toa_sports, toa_usage
_ = toa_sports()
toa_usage()
```
