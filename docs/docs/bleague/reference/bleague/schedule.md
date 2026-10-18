---
title: "BLEAGUE — B.LEAGUE game contexts (S3) and pages (www.bleague.jp, Japan) — Schedule"
sidebar_label: "Schedule"
sidebar_position: 1
description: "BLEAGUE — B.LEAGUE game contexts (S3) and pages (www.bleague.jp, Japan) — Schedule — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# BLEAGUE — B.LEAGUE game contexts (S3) and pages (www.bleague.jp, Japan) — Schedule

## bleague_schedule

Games of one day of one competition and division: schedule keys, clubs, scores, venue, tip-off.

**Endpoint URL:** `GET https://www.bleague.jp/schedule/`

**Valid URL:** [https://www.bleague.jp/schedule/?season=2026&tab=1&month=10&day=4](https://www.bleague.jp/schedule/?season=2026&tab=1&month=10&day=4)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Required. Season END year (2026 = the 2025-26 season). The site's own year is the START year; ``bleague_runtime`` sends ``season - 1``. |
| `tab` | `tab` |  | `Y` |  | Required. Division tab: 1 = B1, 2 = B2, 3 = B3. |
| `month` | `month` |  | `Y` |  | Required. Calendar month 1-12 of the game day (seasons run September to June). |
| `day` | `day` |  | `Y` |  | Required. Day of month of the game day. |
| `event` | `event` |  |  | `Y` | Competition type: 2 B1 league, 3 B1 Championship, 4 B1 relegation playoffs, 5 all-star, 7 B2 league, 8 B2 playoffs, 11 B1-B2 promotion/relegation, 15 B3 league, 32 B3 playoffs. Omitted = the league competition of the tab (2 / 7 / 15). |

### Returns {#bleague_schedule-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2026 = 2025-26), the feed's Year (the START year) plus one. |
| `tab` | integer | Division tab: 1 = B1, 2 = B2, 3 = B3. |
| `event` | integer | Competition type of the page: 2 B1 league, 3 B1 Championship, 4 B1 relegation playoffs, 5 all-star, 7 B2 league, 8 B2 playoffs, 11 B1-B2 promotion/relegation, 15 B3 league, 32 B3 playoffs. |
| `schedule_key` | integer | B.LEAGUE ScheduleKey (key space: B.LEAGUE games, ONE global integer sequence across B1, B2, B3 and every season). The {schedule_key} every game wrapper takes and the schedule page's list-item id. |
| `date` | character | Local game date, derived from the requested month and day (September to June; months 7-12 fall in the season's START year). |
| `round_label` | character | Round label as printed, 第N節 (round N); playoffs print other labels. |
| `round` | integer | Round number parsed from round_label; null when the label is not 第N節. |
| `home_team_id` | character | Home club TeamID as text, mapped from the page's club filter. B.LEAGUE TeamID as text (key space: B.LEAGUE clubs, stable across seasons AND club renames: 703 = Tochigi Brex in 2016-17 = Utsunomiya Brex in 2025-26). The feed sends a string on v2 and an integer on v1; both are text here. Not a player id. |
| `home_team` | character | Home club short name in Japanese as the page prints it (大阪 = Osaka). |
| `home_score` | integer | Home final score; null for a game without one. |
| `away_team_id` | character | Away club TeamID as text, mapped from the page's club filter. B.LEAGUE TeamID as text (key space: B.LEAGUE clubs, stable across seasons AND club renames: 703 = Tochigi Brex in 2016-17 = Utsunomiya Brex in 2025-26). The feed sends a string on v2 and an integer on v1; both are text here. Not a player id. |
| `away_team` | character | Away club short name in Japanese as the page prints it (三遠 = San-en). |
| `away_score` | integer | Away final score; null for a game without one. |
| `prefecture` | character | Prefecture of the venue in Japanese (大阪府 = Osaka Prefecture); null when the page gives none. |
| `venue` | character | Venue name in Japanese, abbreviated by the page (おおきに = Osaka Edion Arena). |
| `tip_time` | character | Tip-off time 'HH:MM' in JST (no offset on the page); null when absent. |
| `state` | character | Score-state label on the page (FINAL for a finished game); null when absent. |

**`return_parsed=False`** — the raw body (`dict`): the game context (`Game`, `PlayByPlays`, `HomeBoxscores`, `AwayBoxscores`, `Summaries` and on v1 `Leaders`, `Points`) stamped `_request` (`schedule_key`, `feed`), or the schedule / standings page record.

### Raises {#bleague_schedule-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | B.LEAGUE has nothing for the request (a ScheduleKey no source holds, a schedule day without games, HTTP 404). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is not the one asked for (an unfinished game, a context or page for another game, season, tab or day, an error or changed body, a truncated schedule list). |
| `ValueError` | a non-integer or out-of-range argument, or HTTP 400 / 422. |

### Example {#bleague_schedule-example}

```python
bleague_schedule(day=4, month=10, season=2026, tab=1)
```

_Last validated n/a._

## bleague_schedule_clubs

Clubs of the season and division from the schedule page's club filter, with their team ids.

**Endpoint URL:** `GET https://www.bleague.jp/schedule/`

**Valid URL:** [https://www.bleague.jp/schedule/?season=2026&tab=1&month=10&day=4](https://www.bleague.jp/schedule/?season=2026&tab=1&month=10&day=4)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Required. Season END year (2026 = the 2025-26 season). The site's own year is the START year; ``bleague_runtime`` sends ``season - 1``. |
| `tab` | `tab` |  | `Y` |  | Required. Division tab: 1 = B1, 2 = B2, 3 = B3. |
| `month` | `month` |  | `Y` |  | Required. Calendar month 1-12 of the game day (seasons run September to June). |
| `day` | `day` |  | `Y` |  | Required. Day of month of the game day. |
| `event` | `event` |  |  | `Y` | Competition type: 2 B1 league, 3 B1 Championship, 4 B1 relegation playoffs, 5 all-star, 7 B2 league, 8 B2 playoffs, 11 B1-B2 promotion/relegation, 15 B3 league, 32 B3 playoffs. Omitted = the league competition of the tab (2 / 7 / 15). |

### Returns {#bleague_schedule_clubs-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2026 = 2025-26), the feed's Year (the START year) plus one. |
| `tab` | integer | Division tab: 1 = B1, 2 = B2, 3 = B3. |
| `event` | integer | Competition type of the page: 2 B1 league, 3 B1 Championship, 4 B1 relegation playoffs, 5 all-star, 7 B2 league, 8 B2 playoffs, 11 B1-B2 promotion/relegation, 15 B3 league, 32 B3 playoffs. |
| `team_id` | character | B.LEAGUE TeamID as text (key space: B.LEAGUE clubs, stable across seasons AND club renames: 703 = Tochigi Brex in 2016-17 = Utsunomiya Brex in 2025-26). The feed sends a string on v2 and an integer on v1; both are text here. Not a player id. |
| `name` | character | Club short name in Japanese as the schedule page prints it (北海道 = Hokkaido). |

**`return_parsed=False`** — the raw body (`dict`): the game context (`Game`, `PlayByPlays`, `HomeBoxscores`, `AwayBoxscores`, `Summaries` and on v1 `Leaders`, `Points`) stamped `_request` (`schedule_key`, `feed`), or the schedule / standings page record.

### Raises {#bleague_schedule_clubs-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | B.LEAGUE has nothing for the request (a ScheduleKey no source holds, a schedule day without games, HTTP 404). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is not the one asked for (an unfinished game, a context or page for another game, season, tab or day, an error or changed body, a truncated schedule list). |
| `ValueError` | a non-integer or out-of-range argument, or HTTP 400 / 422. |

### Example {#bleague_schedule_clubs-example}

```python
bleague_schedule_clubs(day=4, month=10, season=2026, tab=1)
```

_Last validated n/a._

## bleague_schedule_events

Competitions the schedule page offers for the season and division.

**Endpoint URL:** `GET https://www.bleague.jp/schedule/`

**Valid URL:** [https://www.bleague.jp/schedule/?season=2026&tab=1&month=10&day=4](https://www.bleague.jp/schedule/?season=2026&tab=1&month=10&day=4)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Required. Season END year (2026 = the 2025-26 season). The site's own year is the START year; ``bleague_runtime`` sends ``season - 1``. |
| `tab` | `tab` |  | `Y` |  | Required. Division tab: 1 = B1, 2 = B2, 3 = B3. |
| `month` | `month` |  | `Y` |  | Required. Calendar month 1-12 of the game day (seasons run September to June). |
| `day` | `day` |  | `Y` |  | Required. Day of month of the game day. |
| `event` | `event` |  |  | `Y` | Competition type: 2 B1 league, 3 B1 Championship, 4 B1 relegation playoffs, 5 all-star, 7 B2 league, 8 B2 playoffs, 11 B1-B2 promotion/relegation, 15 B3 league, 32 B3 playoffs. Omitted = the league competition of the tab (2 / 7 / 15). |

### Returns {#bleague_schedule_events-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2026 = 2025-26), the feed's Year (the START year) plus one. |
| `tab` | integer | Division tab: 1 = B1, 2 = B2, 3 = B3. |
| `event_id` | integer | Competition type code (see event); the page's event filter value. |
| `label` | character | Competition label in Japanese (B1リーグ = B1 league, オールスターゲーム = all-star game, B1チャンピオンシップ = B1 Championship). |
| `selected` | logical | True for the competition the page was asked for. |

**`return_parsed=False`** — the raw body (`dict`): the game context (`Game`, `PlayByPlays`, `HomeBoxscores`, `AwayBoxscores`, `Summaries` and on v1 `Leaders`, `Points`) stamped `_request` (`schedule_key`, `feed`), or the schedule / standings page record.

### Raises {#bleague_schedule_events-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | B.LEAGUE has nothing for the request (a ScheduleKey no source holds, a schedule day without games, HTTP 404). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is not the one asked for (an unfinished game, a context or page for another game, season, tab or day, an error or changed body, a truncated schedule list). |
| `ValueError` | a non-integer or out-of-range argument, or HTTP 400 / 422. |

### Example {#bleague_schedule_events-example}

```python
bleague_schedule_events(day=4, month=10, season=2026, tab=1)
```

_Last validated n/a._

## bleague_schedule_days

Game days of the requested month for the competition.

**Endpoint URL:** `GET https://www.bleague.jp/schedule/`

**Valid URL:** [https://www.bleague.jp/schedule/?season=2026&tab=1&month=10&day=4](https://www.bleague.jp/schedule/?season=2026&tab=1&month=10&day=4)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Required. Season END year (2026 = the 2025-26 season). The site's own year is the START year; ``bleague_runtime`` sends ``season - 1``. |
| `tab` | `tab` |  | `Y` |  | Required. Division tab: 1 = B1, 2 = B2, 3 = B3. |
| `month` | `month` |  | `Y` |  | Required. Calendar month 1-12 of the game day (seasons run September to June). |
| `day` | `day` |  | `Y` |  | Required. Day of month of the game day. |
| `event` | `event` |  |  | `Y` | Competition type: 2 B1 league, 3 B1 Championship, 4 B1 relegation playoffs, 5 all-star, 7 B2 league, 8 B2 playoffs, 11 B1-B2 promotion/relegation, 15 B3 league, 32 B3 playoffs. Omitted = the league competition of the tab (2 / 7 / 15). |

### Returns {#bleague_schedule_days-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2026 = 2025-26), the feed's Year (the START year) plus one. |
| `tab` | integer | Division tab: 1 = B1, 2 = B2, 3 = B3. |
| `event` | integer | Competition type of the page: 2 B1 league, 3 B1 Championship, 4 B1 relegation playoffs, 5 all-star, 7 B2 league, 8 B2 playoffs, 11 B1-B2 promotion/relegation, 15 B3 league, 32 B3 playoffs. |
| `date` | character | Local date of a game day in the requested month (a date strip entry). |

**`return_parsed=False`** — the raw body (`dict`): the game context (`Game`, `PlayByPlays`, `HomeBoxscores`, `AwayBoxscores`, `Summaries` and on v1 `Leaders`, `Points`) stamped `_request` (`schedule_key`, `feed`), or the schedule / standings page record.

### Raises {#bleague_schedule_days-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | B.LEAGUE has nothing for the request (a ScheduleKey no source holds, a schedule day without games, HTTP 404). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is not the one asked for (an unfinished game, a context or page for another game, season, tab or day, an error or changed body, a truncated schedule list). |
| `ValueError` | a non-integer or out-of-range argument, or HTTP 400 / 422. |

### Example {#bleague_schedule_days-example}

```python
bleague_schedule_days(day=4, month=10, season=2026, tab=1)
```

_Last validated n/a._
