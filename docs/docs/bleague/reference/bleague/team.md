---
title: "B.LEAGUE — B.LEAGUE game contexts (S3) and pages (www.bleague.jp, Japan) — Team"
sidebar_label: "Team"
sidebar_position: 2
description: "B.LEAGUE — B.LEAGUE game contexts (S3) and pages (www.bleague.jp, Japan) — Team — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# B.LEAGUE — B.LEAGUE game contexts (S3) and pages (www.bleague.jp, Japan) — Team

## bleague_team_box

Team rows of the box score (team-credited stats and team totals) per period category.

**Endpoint URL:** `GET https://b-league.s3.amazonaws.com/web_json/contexts/{schedule_key}`

**Valid URL:** [https://b-league.s3.amazonaws.com/web_json/contexts/504734](https://b-league.s3.amazonaws.com/web_json/contexts/504734)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `schedule_key` | `schedule_key` |  | `Y` |  | Required. B.LEAGUE ScheduleKey (integer, one global sequence across B1, B2, B3 and every season), e.g. 504734 = B1 2025-26 sec 1, Alvark Tokyo v Utsunomiya Brex; list a day's keys with bleague_schedule. |
| `season` | `season` |  |  | `Y` | Optional guard: the season END year the game must belong to. A context whose Year differs raises AssetFetchError. Omitted = no check (the key alone selects the game). |

### Returns {#bleague_team_box-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `schedule_key` | Int64 | B.LEAGUE ScheduleKey (key space: B.LEAGUE games, ONE global integer sequence across B1, B2, B3 and every season). The {schedule_key} every game wrapper takes and the schedule page's list-item id. |
| `season` | Int64 | Season END year (2026 = 2025-26), the feed's Year (the START year) plus one. |
| `feed` | String | Source feed of the row: v2 (v2_genius_contexts, B1/B2 from 2020-21) or v1 (v1_contexts: B3, B1/B2 2018-2020, and the 2016-17 / 2017-18 game-page embed). The feeds differ in id types, dates and some codes. |
| `team_side` | String | Side of the game the row belongs to: home or away. |
| `team_id` | String | B.LEAGUE TeamID as text (key space: B.LEAGUE clubs, stable across seasons AND club renames: 703 = Tochigi Brex in 2016-17 = Utsunomiya Brex in 2025-26). The feed sends a string on v2 and an integer on v1; both are text here. Not a player id. |
| `category` | Int64 | Row category: 1 = player, 2 = team / coaches row (team-credited stats; on v1 it names the head coach), 3 = team totals (合計). One row per (category, player, period category). |
| `period_category` | Int64 | Period category: 1-4 = quarters 1-4, 15 = first half, 16 = second half, 18 = whole game. Overtime codes are unverified. |
| `team_name_j` | String | Team name in Japanese (official name, e.g. アルバルク東京 = Alvark Tokyo). |
| `team_name_e` | String | Team name in English, uppercase ('ALVARK TOKYO'). |
| `player_id` | String | B.LEAGUE PlayerID as text (key space: B.LEAGUE persons, stable across seasons and clubs: Makoto Hiejima = '8589'). A string on v2, an integer on v1; both are text here. Null where the row names no player. |
| `player_no` | String | Shirt number as text, so '00' and '0' stay distinct; null on team and game-level rows. |
| `player_name_j` | String | Player name in Japanese (katakana for foreign players); null on team and game-level rows. |
| `player_name_e` | String | Player name in English ('Makoto Hiejima'); null on team and totals rows. |
| `play_time` | String | Minutes played as the feed's 'mm:ss'; 'DNP' = did not play. Totals rows hold 5 x the period length ('200:00' for the game). |
| `on_court` | Boolean | True for the five players on court when the context was last written (for a final game, at the final buzzer); the same value on every period row of the player. Null on v2 team and totals rows. |
| `display_order` | Int64 | Display order of the row (v1 only), integer; null on v2. |
| `starter` | Boolean | True when the player started; false for the bench; null on team and totals rows. (The feed sends 1 / null on v2 and a boolean on v1.) |
| `seconds_played` | Int64 | Time on court in seconds, parsed from play_time (50:00 = 3000); null for DNP and blank. |
| `points` | Int64 | Points scored. The v2 feed sends '' (a placeholder, not 0) on the team row of quarters; null here. |
| `fg3_made` | Int64 | Three-point field goals made (v2 placeholder '' on quarter team rows becomes null). |
| `fg3_attempted` | Int64 | Three-point field goals attempted. |
| `fg2_made` | Int64 | Two-point field goals made. |
| `fg2_attempted` | Int64 | Two-point field goals attempted. |
| `ft_made` | Int64 | Free throws made. |
| `ft_attempted` | Int64 | Free throws attempted. |
| `fouls` | Int64 | Personal fouls committed (F). |
| `fouls_drawn` | Int64 | Fouls drawn (被ファウル, FD). |
| `reb_off` | Int64 | Offensive rebounds (OR). |
| `reb_def` | Int64 | Defensive rebounds (DR). |
| `reb` | Int64 | Total rebounds. |
| `turnovers` | Int64 | Turnovers (TO). |
| `assists` | Int64 | Assists credited to the player or team (AS). |
| `steals` | Int64 | Steals credited to the player or team (ST). |
| `blocks` | Int64 | Blocks made (BS). |
| `blocks_received` | Int64 | Shots of this player or team that were blocked (被ブロック, BSON). |
| `ptfb` | Int64 | Fast-break figure (ファストブレイク); equals the summary's fast-break points on the one game checked, but whether it counts points or baskets is unverified. |
| `pt2in` | Int64 | Inside-the-paint figure: on v2 a COUNT of made inside-the-paint 2PT (the summary holds 2x that in points); on v1 points in the paint (0 on B3, whose 2PT shots are never split by paint). |
| `pt2nd` | Int64 | Second-chance figure (セカンドチャンス); smaller than the summary's second-chance points, so likely made baskets; unverified. |
| `ptpft` | Int64 | Points off turnovers (ポインツオフターンオーバー); v1 box rows only, null on v2. |
| `dunks` | Int64 | Dunks made (PT2DUNK); v1 box rows only, null on v2. |
| `efficiency` | Int64 | Efficiency rating (貢献度), era-dependent: v2 = points + rebounds + assists + steals + blocks + fouls drawn - missed FG - missed FT - turnovers - fouls - shots blocked (PIR); v1 omits fouls drawn, fouls and shots blocked. |
| `plus_minus` | Int64 | Plus/minus: team point difference while on court. v2 from 2022-23 only; null on totals and half rows. |
| `ast_to` | Float64 | Assist-to-turnover ratio (0 when no turnovers). v2 from 2022-23 only; null on half rows. |
| `efg` | Float64 | Effective field-goal percentage as a FRACTION 0-1 (3PT weighted 1.5x); the site multiplies by 100. v2 from 2022-23 only. |
| `ts` | Float64 | True-shooting percentage as a FRACTION 0-1; the site multiplies by 100. v2 from 2022-23 only. |
| `usg` | Float64 | Usage rate in PERCENT 0-100. v2 from 2022-23 only; absent on totals rows. |
| `off_rtg` | Float64 | Offensive rating. v2 from 2022-23 only; on team and totals rows 0 in every sample, never filled. |
| `def_rtg` | Float64 | Defensive rating. v2 from 2022-23 only; on team and totals rows 0 in every sample, never filled. |
| `net_rtg` | Float64 | Net rating. v2 from 2022-23 only; on team and totals rows 0 in every sample, never filled. |
| `poss` | Float64 | Possessions. v2 from 2022-23 only; on team and totals rows 0 in every sample, never filled. |

**`return_parsed=False`** — the raw body (`dict`): the game context (`Game`, `PlayByPlays`, `HomeBoxscores`, `AwayBoxscores`, `Summaries` and on v1 `Leaders`, `Points`) stamped `_request` (`schedule_key`, `feed`), or the schedule / standings page record.

### Raises {#bleague_team_box-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. B.LEAGUE has nothing for the request (a ScheduleKey no source holds, a schedule day without games, HTTP 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a context or page for another game, season, tab or day, an error or changed body, a truncated schedule list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A non-integer or out-of-range argument, or HTTP 400 / 422. |

### Example {#bleague_team_box-example}

```python
bleague_team_box(schedule_key=504734)
```

## bleague_team_summary

Team summary per period category in long form: shooting, rebounds, paint, fast-break and second-chance points.

**Endpoint URL:** `GET https://b-league.s3.amazonaws.com/web_json/contexts/{schedule_key}`

**Valid URL:** [https://b-league.s3.amazonaws.com/web_json/contexts/504734](https://b-league.s3.amazonaws.com/web_json/contexts/504734)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `schedule_key` | `schedule_key` |  | `Y` |  | Required. B.LEAGUE ScheduleKey (integer, one global sequence across B1, B2, B3 and every season), e.g. 504734 = B1 2025-26 sec 1, Alvark Tokyo v Utsunomiya Brex; list a day's keys with bleague_schedule. |
| `season` | `season` |  |  | `Y` | Optional guard: the season END year the game must belong to. A context whose Year differs raises AssetFetchError. Omitted = no check (the key alone selects the game). |

### Returns {#bleague_team_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `schedule_key` | Int64 | B.LEAGUE ScheduleKey (key space: B.LEAGUE games, ONE global integer sequence across B1, B2, B3 and every season). The {schedule_key} every game wrapper takes and the schedule page's list-item id. |
| `season` | Int64 | Season END year (2026 = 2025-26), the feed's Year (the START year) plus one. |
| `feed` | String | Source feed of the row: v2 (v2_genius_contexts, B1/B2 from 2020-21) or v1 (v1_contexts: B3, B1/B2 2018-2020, and the 2016-17 / 2017-18 game-page embed). The feeds differ in id types, dates and some codes. |
| `period_category` | Int64 | Period category: 1-4 = quarters 1-4, 15 = first half, 16 = second half, 18 = whole game. Overtime codes are unverified. |
| `team_side` | String | Side of the game the row belongs to: home or away. |
| `team_id` | String | B.LEAGUE TeamID as text (key space: B.LEAGUE clubs, stable across seasons AND club renames: 703 = Tochigi Brex in 2016-17 = Utsunomiya Brex in 2025-26). The feed sends a string on v2 and an integer on v1; both are text here. Not a player id. |
| `team_name_j` | String | Team name in Japanese. |
| `team_name_e` | String | Team name in English, uppercase. |
| `fg_made` | Int64 | Field goals made (PTM). |
| `fg_attempted` | Int64 | Field goals attempted (PTA). |
| `fg_pct` | Float64 | Field-goal percentage 0-100, one decimal. |
| `fg2_made` | Int64 | Two-point field goals made. |
| `fg2_attempted` | Int64 | Two-point field goals attempted. |
| `fg2_pct` | Float64 | Two-point percentage 0-100. |
| `fg3_made` | Int64 | Three-point field goals made. |
| `fg3_attempted` | Int64 | Three-point field goals attempted. |
| `fg3_pct` | Float64 | Three-point percentage 0-100. |
| `ft_made` | Int64 | Free throws made. |
| `ft_attempted` | Int64 | Free throws attempted. |
| `ft_pct` | Float64 | Free-throw percentage 0-100. |
| `reb_off` | Int64 | Offensive rebounds. |
| `reb_def` | Int64 | Defensive rebounds. |
| `reb` | Int64 | Total rebounds. |
| `assists` | Int64 | Assists credited to the team in the period category. |
| `turnovers` | Int64 | Turnovers committed by the team in the period category. |
| `steals` | Int64 | Steals made by the team in the period category. |
| `blocks` | Int64 | Blocks made by the team in the period category. |
| `blocks_received` | Int64 | Shots blocked by the opponent (被ブロック). |
| `fouls` | Int64 | Fouls committed. |
| `fouls_drawn` | Int64 | Fouls drawn (被ファウル). |
| `paint_points` | Int64 | Points in the paint (inside-the-paint 2PT points; 2x the players' PT2IN count on v2). |
| `points_off_turnover` | Int64 | Points off turnovers (ポインツオフターンオーバー). |
| `second_chance_points` | Int64 | Second-chance points (セカンドチャンス). |
| `fast_break_points` | Int64 | Fast-break points (ファストブレイク). |
| `max_lead` | Int64 | Largest lead of the team in the period category (0 if it never led). |
| `lead_last` | Int64 | Lead statistic (LeadLast), integer; meaning unverified (7 for the home side on all checked games). |
| `lead_last_home` | Int64 | Home score-like value tied to lead_last; meaning unverified. A string on v2 quarter / game rows. |
| `lead_last_away` | Int64 | Away score-like value tied to lead_last; meaning unverified. |
| `lead_changes` | Int64 | Lead changes in the period category (LeadCount); the same on both side rows. |
| `ties` | Int64 | Times the score was tied in the period category (TieCount); the same on both side rows. |

**`return_parsed=False`** — the raw body (`dict`): the game context (`Game`, `PlayByPlays`, `HomeBoxscores`, `AwayBoxscores`, `Summaries` and on v1 `Leaders`, `Points`) stamped `_request` (`schedule_key`, `feed`), or the schedule / standings page record.

### Raises {#bleague_team_summary-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. B.LEAGUE has nothing for the request (a ScheduleKey no source holds, a schedule day without games, HTTP 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a context or page for another game, season, tab or day, an error or changed body, a truncated schedule list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A non-integer or out-of-range argument, or HTTP 400 / 422. |

### Example {#bleague_team_summary-example}

```python
bleague_team_summary(schedule_key=504734)
```
