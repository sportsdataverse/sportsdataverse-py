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

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/scoreboard/main](https://api.foxsports.com/bifrost/v1/nfl/scoreboard/main)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |
| `groupId` | `group_id` |  |  | `Y` | Conference / group id filter. |

### Returns {#fox_api_scoreboard-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_fox_api_events`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_scoreboard-example}

```python
fox_api_scoreboard(sport='nfl')
```

_Last validated n/a._

## fox_api_scorechip

GET /bifrost/v1/{sport}/scorechip/{chip_id} -- one game's score chip (this route 400s if api-version is sent).

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/scorechip/{chip_id}`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/scorechip/nfl11195](https://api.foxsports.com/bifrost/v1/nfl/scorechip/nfl11195)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `chip_id` | `chip_id` |  | `Y` |  | Score-chip id: the league slug plus the numeric game id, e.g. ``nfl11195``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |

### Returns {#fox_api_scorechip-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_fox_api_scorechip`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_scorechip-example}

```python
fox_api_scorechip(sport='nfl', chip_id='nfl11195')
```

_Last validated n/a._

## fox_api_topevents_scoreboard_segment

GET /bifrost/v1/topevents/scoreboard/segment/{segment} -- Fox Sports API topevents scoreboard segment.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/topevents/scoreboard/segment/{segment}`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/topevents/scoreboard/segment/1](https://api.foxsports.com/bifrost/v1/topevents/scoreboard/segment/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `segment` | `segment` |  | `Y` |  | Top-events scoreboard segment id (``0`` / ``1`` ... as listed by ``topevents/scoreboard/main``); NOT a league ``<season>-<week>-<type>`` id. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_topevents_scoreboard_segment-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_fox_api_events`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_topevents_scoreboard_segment-example}

```python
fox_api_topevents_scoreboard_segment(segment='1')
```

_Last validated n/a._

## fox_api_league_conferences

GET /bifrost/v1/{sport}/league/conferences -- Fox Sports API league conferences.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/league/conferences`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/league/conferences](https://api.foxsports.com/bifrost/v1/nfl/league/conferences)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_league_conferences-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_fox_api_nav`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_league_conferences-example}

```python
fox_api_league_conferences(sport='nfl')
```

_Last validated n/a._

## fox_api_league_header

GET /bifrost/v1/{sport}/league/header -- Fox Sports API league header.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/league/header`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/league/header](https://api.foxsports.com/bifrost/v1/nfl/league/header)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_league_header-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_fox_api_header`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_league_header-example}

```python
fox_api_league_header(sport='nfl')
```

_Last validated n/a._

## fox_api_league_odds

GET /bifrost/v1/{sport}/league/odds -- Fox Sports API league odds.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/league/odds`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/league/odds](https://api.foxsports.com/bifrost/v1/nfl/league/odds)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |
| `groupId` | `group_id` |  |  | `Y` | Conference / group id filter. |

### Returns {#fox_api_league_odds-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_fox_api`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_league_odds-example}

```python
fox_api_league_odds(sport='nfl')
```

_Last validated n/a._

## fox_api_league_playernews

GET /bifrost/v1/{sport}/league/playernews -- Fox Sports API league playernews.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/league/playernews`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/league/playernews](https://api.foxsports.com/bifrost/v1/nfl/league/playernews)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_league_playernews-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_fox_api`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_league_playernews-example}

```python
fox_api_league_playernews(sport='nfl')
```

_Last validated n/a._

## fox_api_league_polls

GET /bifrost/v1/{sport}/league/polls -- Fox Sports API league polls.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/league/polls`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/league/polls](https://api.foxsports.com/bifrost/v1/nfl/league/polls)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_league_polls-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_fox_api_polls`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_league_polls-example}

```python
fox_api_league_polls(sport='nfl')
```

_Last validated n/a._

## fox_api_league_schedule

GET /bifrost/v1/{sport}/league/schedule -- Fox Sports API league schedule.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/league/schedule`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/league/schedule](https://api.foxsports.com/bifrost/v1/nfl/league/schedule)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_league_schedule-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_fox_api_events`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_league_schedule-example}

```python
fox_api_league_schedule(sport='nfl')
```

_Last validated n/a._

## fox_api_league_scores

GET /bifrost/v1/{sport}/league/scores -- Fox Sports API league scores.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/league/scores`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/league/scores](https://api.foxsports.com/bifrost/v1/nfl/league/scores)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_league_scores-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_fox_api_events`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_league_scores-example}

```python
fox_api_league_scores(sport='nfl')
```

_Last validated n/a._

## fox_api_league_scores_segment

GET /bifrost/v1/{sport}/league/scores-segment/{segment_id} -- Fox Sports API league scores segment.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/league/scores-segment/{segment_id}`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/league/scores-segment/2026-3-1](https://api.foxsports.com/bifrost/v1/nfl/league/scores-segment/2026-3-1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `segment_id` | `segment_id` |  | `Y` |  | Scores segment id, ``<season>-<week>-<seasonTypeCode>``, e.g. ``2026-3-1``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |
| `groupId` | `group_id` |  |  | `Y` | Conference / group id filter. |

### Returns {#fox_api_league_scores_segment-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_fox_api_events`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_league_scores_segment-example}

```python
fox_api_league_scores_segment(sport='nfl', segment_id='2026-3-1')
```

_Last validated n/a._

## fox_api_league_standings

GET /bifrost/v1/{sport}/league/standings -- Fox Sports API league standings.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/league/standings`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/league/standings](https://api.foxsports.com/bifrost/v1/nfl/league/standings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_league_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_fox_api_standings`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_league_standings-example}

```python
fox_api_league_standings(sport='nfl')
```

_Last validated n/a._

## fox_api_league_stats

GET /bifrost/v1/{sport}/league/stats -- Fox Sports API league stats.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/league/stats`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/league/stats](https://api.foxsports.com/bifrost/v1/nfl/league/stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_league_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_fox_api`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_league_stats-example}

```python
fox_api_league_stats(sport='nfl')
```

_Last validated n/a._

## fox_api_league_stats_con

GET /bifrost/v1/{sport}/league/stats-con/{who}/{category}/{page} -- Fox Sports API league stats con.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/league/stats-con/{who}/{category}/{page}`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/league/stats-con/player/passing/1](https://api.foxsports.com/bifrost/v1/nfl/league/stats-con/player/passing/1)

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_fox_api`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_league_stats_con-example}

```python
fox_api_league_stats_con(sport='nfl', who='player', category='passing', page='1')
```

_Last validated n/a._

## fox_api_league_teamnav

GET /bifrost/v1/{sport}/league/teamnav -- Fox Sports API league teamnav.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/league/teamnav`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/league/teamnav](https://api.foxsports.com/bifrost/v1/nfl/league/teamnav)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_league_teamnav-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_fox_api_nav`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_league_teamnav-example}

```python
fox_api_league_teamnav(sport='nfl')
```

_Last validated n/a._

## fox_api_event_data

GET /bifrost/v1/{sport}/event/{event_id}/data -- Fox Sports API event data.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/event/{event_id}/data`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/event/11195/data](https://api.foxsports.com/bifrost/v1/nfl/event/11195/data)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `event_id` | `event_id` |  | `Y` |  | Numeric Fox game (event) id, e.g. ``11195``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_event_data-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_fox_api`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_event_data-example}

```python
fox_api_event_data(sport='nfl', event_id='11195')
```

_Last validated n/a._

## fox_api_event_matchup

GET /bifrost/v1/{sport}/event/{event_id}/matchup -- Fox Sports API event matchup.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/event/{event_id}/matchup`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/event/11195/matchup](https://api.foxsports.com/bifrost/v1/nfl/event/11195/matchup)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `event_id` | `event_id` |  | `Y` |  | Numeric Fox game (event) id, e.g. ``11195``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_event_matchup-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_fox_api`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_event_matchup-example}

```python
fox_api_event_matchup(sport='nfl', event_id='11195')
```

_Last validated n/a._

## fox_api_event_odds

GET /bifrost/v1/{sport}/event/{event_id}/odds -- Fox Sports API event odds.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/event/{event_id}/odds`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/event/11195/odds](https://api.foxsports.com/bifrost/v1/nfl/event/11195/odds)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `event_id` | `event_id` |  | `Y` |  | Numeric Fox game (event) id, e.g. ``11195``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_event_odds-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_fox_api`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_event_odds-example}

```python
fox_api_event_odds(sport='nfl', event_id='11195')
```

_Last validated n/a._

## fox_api_event_recap

GET /bifrost/v1/{sport}/event/{event_id}/recap -- Fox Sports API event recap.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/event/{event_id}/recap`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/event/11195/recap](https://api.foxsports.com/bifrost/v1/nfl/event/11195/recap)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `event_id` | `event_id` |  | `Y` |  | Numeric Fox game (event) id, e.g. ``11195``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_event_recap-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_fox_api`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_event_recap-example}

```python
fox_api_event_recap(sport='nfl', event_id='11195')
```

_Last validated n/a._

## fox_api_event_standings

GET /bifrost/v1/{sport}/event/{event_id}/standings -- Fox Sports API event standings.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/event/{event_id}/standings`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/event/11195/standings](https://api.foxsports.com/bifrost/v1/nfl/event/11195/standings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `event_id` | `event_id` |  | `Y` |  | Numeric Fox game (event) id, e.g. ``11195``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_event_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_fox_api_standings`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_event_standings-example}

```python
fox_api_event_standings(sport='nfl', event_id='11195')
```

_Last validated n/a._

## fox_api_team_gamelog

GET /bifrost/v1/{sport}/team/{team_id}/gamelog -- Fox Sports API team gamelog.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/team/{team_id}/gamelog`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/team/25/gamelog](https://api.foxsports.com/bifrost/v1/nfl/team/25/gamelog)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `team_id` | `team_id` |  | `Y` |  | Numeric Fox team id, e.g. ``25``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_team_gamelog-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_fox_api`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_team_gamelog-example}

```python
fox_api_team_gamelog(sport='nfl', team_id='25')
```

_Last validated n/a._

## fox_api_team_header

GET /bifrost/v1/{sport}/team/{team_id}/header -- Fox Sports API team header.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/team/{team_id}/header`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/team/25/header](https://api.foxsports.com/bifrost/v1/nfl/team/25/header)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `team_id` | `team_id` |  | `Y` |  | Numeric Fox team id, e.g. ``25``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_team_header-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_fox_api_header`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_team_header-example}

```python
fox_api_team_header(sport='nfl', team_id='25')
```

_Last validated n/a._

## fox_api_team_roster

GET /bifrost/v1/{sport}/team/{team_id}/roster -- Fox Sports API team roster.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/team/{team_id}/roster`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/team/25/roster](https://api.foxsports.com/bifrost/v1/nfl/team/25/roster)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `team_id` | `team_id` |  | `Y` |  | Numeric Fox team id, e.g. ``25``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_team_roster-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_fox_api_roster`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_team_roster-example}

```python
fox_api_team_roster(sport='nfl', team_id='25')
```

_Last validated n/a._

## fox_api_team_standings

GET /bifrost/v1/{sport}/team/{team_id}/standings -- Fox Sports API team standings.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/team/{team_id}/standings`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/team/25/standings](https://api.foxsports.com/bifrost/v1/nfl/team/25/standings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `team_id` | `team_id` |  | `Y` |  | Numeric Fox team id, e.g. ``25``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_team_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_fox_api_standings`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_team_standings-example}

```python
fox_api_team_standings(sport='nfl', team_id='25')
```

_Last validated n/a._

## fox_api_team_stats

GET /bifrost/v1/{sport}/team/{team_id}/stats -- Fox Sports API team stats.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/team/{team_id}/stats`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/team/25/stats](https://api.foxsports.com/bifrost/v1/nfl/team/25/stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `team_id` | `team_id` |  | `Y` |  | Numeric Fox team id, e.g. ``25``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_team_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_fox_api`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_team_stats-example}

```python
fox_api_team_stats(sport='nfl', team_id='25')
```

_Last validated n/a._

## fox_api_explore_browse

GET /bifrost/v1/explore/browse/{section}/main -- Fox Sports API explore browse.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/explore/browse/{section}/main`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/explore/browse/sports/main](https://api.foxsports.com/bifrost/v1/explore/browse/sports/main)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `section` | `section` |  | `Y` |  | Browse section: ``sports``, ``players``, ``shows``, ``personalities`` or ``topics``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_explore_browse-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_fox_api_nav`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_explore_browse-example}

```python
fox_api_explore_browse(section='sports')
```

_Last validated n/a._

## fox_api_explore_odds

GET /bifrost/v1/explore/odds/main -- Fox Sports API explore odds.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/explore/odds/main`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/explore/odds/main](https://api.foxsports.com/bifrost/v1/explore/odds/main)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_explore_odds-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_fox_api`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_explore_odds-example}

```python
fox_api_explore_odds()
```

_Last validated n/a._

## fox_api_search_content

GET /bifrost/v1/search/content -- Fox Sports API search content.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/search/content`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/search/content?text=mahomes](https://api.foxsports.com/bifrost/v1/search/content?text=mahomes)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |
| `text` | `text` |  |  | `Y` | Search text. |

### Returns {#fox_api_search_content-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_fox_api_search`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_search_content-example}

```python
fox_api_search_content(text='mahomes')
```

_Last validated n/a._

## fox_api_search_entities

GET /bifrost/v1/search/entities -- Fox Sports API search entities.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/search/entities`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/search/entities?text=mahomes](https://api.foxsports.com/bifrost/v1/search/entities?text=mahomes)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |
| `text` | `text` |  |  | `Y` | Search text. |

### Returns {#fox_api_search_entities-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_fox_api_search`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_search_entities-example}

```python
fox_api_search_entities(text='mahomes')
```

_Last validated n/a._

## fox_api_search_popular

GET /bifrost/v1/search/popular -- Fox Sports API search popular.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/search/popular`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/search/popular](https://api.foxsports.com/bifrost/v1/search/popular)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_search_popular-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_fox_api_search`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_search_popular-example}

```python
fox_api_search_popular()
```

_Last validated n/a._

## fox_api_trending_articles

GET /bifrost/v1/general/trending/articles -- Fox Sports API trending articles.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/general/trending/articles`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/general/trending/articles](https://api.foxsports.com/bifrost/v1/general/trending/articles)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports feed-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |
| `duration` | `duration` |  |  | `Y` | Trending look-back window. |
| `tags` | `tags` |  |  | `Y` | Comma-separated tag filter. |

### Returns {#fox_api_trending_articles-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_fox_api_trending`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_trending_articles-example}

```python
fox_api_trending_articles()
```

_Last validated n/a._

## fox_api_trending_videos

GET /bifrost/v1/general/trending/videos -- Fox Sports API trending videos.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/general/trending/videos`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/general/trending/videos](https://api.foxsports.com/bifrost/v1/general/trending/videos)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports feed-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |
| `duration` | `duration` |  |  | `Y` | Trending look-back window. |
| `maxItems` | `max_items` |  |  | `Y` | Maximum items to return. |

### Returns {#fox_api_trending_videos-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_fox_api_trending`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_trending_videos-example}

```python
fox_api_trending_videos()
```

_Last validated n/a._

## fox_api_foxpolls

GET /foxpolls/v1/polls -- Fox Sports API foxpolls.

**Endpoint URL:** `GET https://api.foxsports.com/foxpolls/v1/polls`

**Valid URL:** [https://api.foxsports.com/foxpolls/v1/polls](https://api.foxsports.com/foxpolls/v1/polls)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports feed-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `associatedEntityIds` | `associated_entity_ids` |  |  | `Y` | Comma-separated Fox entity ids the polls are associated with. |
| `includeAnswers` | `include_answers` |  |  | `Y` | Include poll answer options. |

### Returns {#fox_api_foxpolls-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_fox_api`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#fox_api_foxpolls-example}

```python
fox_api_foxpolls()
```

_Last validated n/a._
