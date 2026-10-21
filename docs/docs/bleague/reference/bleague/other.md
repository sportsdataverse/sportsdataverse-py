---
title: "BLEAGUE — B.LEAGUE game contexts (S3) and pages (www.bleague.jp, Japan) — Other"
sidebar_label: "Other"
sidebar_position: 3
description: "BLEAGUE — B.LEAGUE game contexts (S3) and pages (www.bleague.jp, Japan) — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# BLEAGUE — B.LEAGUE game contexts (S3) and pages (www.bleague.jp, Japan) — Other

## bleague_game_info

Game header: competition, round, venue, officials, tip-off, both teams and the scores.

**Endpoint URL:** `GET https://b-league.s3.amazonaws.com/web_json/contexts/{schedule_key}`

**Valid URL:** [https://b-league.s3.amazonaws.com/web_json/contexts/504734](https://b-league.s3.amazonaws.com/web_json/contexts/504734)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `schedule_key` | `schedule_key` |  | `Y` |  | Required. B.LEAGUE ScheduleKey (integer, one global sequence across B1, B2, B3 and every season), e.g. 504734 = B1 2025-26 sec 1, Alvark Tokyo v Utsunomiya Brex; list a day's keys with bleague_schedule. |
| `season` | `season` |  |  | `Y` | Optional guard: the season END year the game must belong to. A context whose Year differs raises AssetFetchError. Omitted = no check (the key alone selects the game). |

### Returns {#bleague_game_info-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `schedule_key` | integer | B.LEAGUE ScheduleKey (key space: B.LEAGUE games, ONE global integer sequence across B1, B2, B3 and every season). The {schedule_key} every game wrapper takes and the schedule page's list-item id. |
| `season` | integer | Season END year (2026 = 2025-26), the feed's Year (the START year) plus one. |
| `feed` | character | Source feed of the row: v2 (v2_genius_contexts, B1/B2 from 2020-21) or v1 (v1_contexts: B3, B1/B2 2018-2020, and the 2016-17 / 2017-18 game-page embed). The feeds differ in id types, dates and some codes. |
| `year` | integer | Season START year as the site stores it (2025 = 2025-26); season is the END year. |
| `code` | integer | Feed/organisation code repeated on every row: 546 on v2, 6 on v1; meaning unverified. |
| `event` | integer | Competition type (v1 header only): 2 B1 league, 3 B1 Championship, 4 B1 relegation playoffs, 5 all-star, 7 B2 league, 8 B2 playoffs, 11 B1-B2 promotion/relegation, 15 B3 league, 32 B3 playoffs; null on v2. |
| `convention_key` | character | ConventionKey as text (key space: competition-seasons, 大会): v2 '514' = B1 2025-26, v1 102 = B3 2025-26. Key on (feed, convention_key); the feeds may number independently. |
| `convention_class` | integer | Competition class code (1 on every sample); meaning unverified. |
| `round` | integer | Round (節, Setu) within the competition-season; playoffs use 100+ (101). Integer. |
| `game_no` | integer | Game number within the competition (v1 header only); null on v2. |
| `schedule_state` | integer | Schedule state code (v1 header only; 2 on the final-game sample); meaning unverified. |
| `schedule_conference` | integer | Conference code (v1 header only); meaning unverified. |
| `interleague` | logical | True for an inter-league game (v1 header only). |
| `stadium_cd` | character | StadiumCD as text (key space: B.LEAGUE venues; stability unverified). |
| `attendance` | integer | Announced attendance. |
| `game_datetime` | character | Scheduled tip-off as a UTC instant (the feed's JST wall clock minus nine hours). |
| `game_start_time` | character | Actual start as a UTC instant (v1 header only; epoch seconds on the 2016-17 embed). |
| `game_end_time` | character | Actual end as a UTC instant (v1 header only). |
| `max_period` | integer | Number of periods the game had (4 plus overtimes). |
| `current_period` | integer | Period the context was last written in (the last period for a final game). |
| `rest_time` | integer | Clock remaining when last written (v1 header only; 0 for a final game). |
| `game_ended` | logical | True when the game has ended; the runtime serves only games where this and record_fixed are true. |
| `record_fixed` | logical | True when the record is finalised (no more corrections expected). |
| `box_exists` | logical | True when the box score exists. |
| `pbp_exists` | logical | True when play-by-play exists. |
| `picture_exists` | logical | True when photos exist (v1 header only). |
| `report_exists` | logical | True when a game report text exists (v1 header only). |
| `home_win` | logical | True when the home team won (v1 header only); null on v2. |
| `commissioner_id` | character | Commissioner (コミッショナー) id as text; null when none is assigned. |
| `referee_id` | character | Crew-chief referee RefereeID as text (key space: officials; stability unverified). |
| `sub_referee_id1` | character | Second referee RefereeID as text. |
| `sub_referee_id2` | character | Third referee RefereeID as text. |
| `temperature` | numeric | Temperature; null in every observed body, so its dtype is an assumption. |
| `humidity` | numeric | Humidity; null in every observed body, so its dtype is an assumption. |
| `convention_name_j` | character | Competition name in Japanese (e.g. りそなグループ B.LEAGUE 2025-26 B1リーグ戦 = B1 regular season). |
| `convention_name_e` | character | Competition name in English; empty (null) on early v1 bodies. |
| `convention_title_j` | character | Competition title in Japanese (v1 header only). |
| `convention_title_e` | character | Competition title in English (v1 header only). |
| `stadium_name_j` | character | Venue name in Japanese (e.g. TOYOTA ARENA TOKYO, AGF鈴鹿体育館 = AGF Suzuka Gymnasium). |
| `stadium_name_e` | character | Venue name in English; often empty (null). |
| `court` | character | Court number at the venue (v1 header only). |
| `report` | character | Game report text (v1 header only); usually empty (null). |
| `weather` | character | Weather text (v1 header only); null in every observed body. |
| `schedule_class_name_j` | character | Schedule class name in Japanese (v1 header only); usually empty. |
| `schedule_class_name_e` | character | Schedule class name in English (v1 header only); usually empty. |
| `schedule_class_short_name_j` | character | Schedule class short name in Japanese (v1 header only); usually empty. |
| `schedule_class_short_name_e` | character | Schedule class short name in English (v1 header only); usually empty. |
| `commissioner_name_j` | character | Commissioner name in Japanese (names only; no contact data is published). |
| `commissioner_name_e` | character | Commissioner name in English (v1 header only). |
| `referee_name_j` | character | Crew-chief referee name in Japanese (名 only; no contact data is published). |
| `referee_name_e` | character | Crew-chief referee name in English (v1 header only). |
| `sub_referee_name_j1` | character | Second referee name in Japanese. |
| `sub_referee_name_e1` | character | Second referee name in English (v1 header only). |
| `sub_referee_name_j2` | character | Third referee name in Japanese. |
| `sub_referee_name_e2` | character | Third referee name in English (v1 header only). |
| `home_team_id` | character | Home team id. B.LEAGUE TeamID as text (key space: B.LEAGUE clubs, stable across seasons AND club renames: 703 = Tochigi Brex in 2016-17 = Utsunomiya Brex in 2025-26). The feed sends a string on v2 and an integer on v1; both are text here. Not a player id. |
| `home_team_name_j` | character | Home team name in Japanese (official name). |
| `home_team_name_e` | character | Home team name in English, uppercase. |
| `home_team_short_name_j` | character | Home team short name in Japanese (e.g. A東京). |
| `home_team_short_name_e` | character | Home team short name in English (e.g. A TOKYO). |
| `home_team_sub_name_j` | character | Home team sub name in Japanese (v1 header only); usually empty. |
| `home_team_sub_name_e` | character | Home team sub name in English (v1 header only); usually empty. |
| `home_score` | integer | Home team final score. |
| `home_winning_count` | integer | Home team winning streak (v1 header only); null in every observed body. |
| `home_losing_count` | integer | Home team losing streak (v1 header only); null in every observed body. |
| `away_team_id` | character | Away team id. B.LEAGUE TeamID as text (key space: B.LEAGUE clubs, stable across seasons AND club renames: 703 = Tochigi Brex in 2016-17 = Utsunomiya Brex in 2025-26). The feed sends a string on v2 and an integer on v1; both are text here. Not a player id. |
| `away_team_name_j` | character | Away team name in Japanese (official name). |
| `away_team_name_e` | character | Away team name in English, uppercase. |
| `away_team_short_name_j` | character | Away team short name in Japanese (e.g. A東京). |
| `away_team_short_name_e` | character | Away team short name in English (e.g. A TOKYO). |
| `away_team_sub_name_j` | character | Away team sub name in Japanese (v1 header only); usually empty. |
| `away_team_sub_name_e` | character | Away team sub name in English (v1 header only); usually empty. |
| `away_score` | integer | Away team final score. |
| `away_winning_count` | integer | Away team winning streak (v1 header only); null in every observed body. |
| `away_losing_count` | integer | Away team losing streak (v1 header only); null in every observed body. |
| `home_score_p1` | integer | Home team points scored IN period 1 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `home_score_p2` | integer | Home team points scored IN period 2 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `home_score_p3` | integer | Home team points scored IN period 3 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `home_score_p4` | integer | Home team points scored IN period 4 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `home_score_p5` | integer | Home team points scored IN period 5 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `home_score_p6` | integer | Home team points scored IN period 6 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `home_score_p7` | integer | Home team points scored IN period 7 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `home_score_p8` | integer | Home team points scored IN period 8 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `home_score_p9` | integer | Home team points scored IN period 9 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `home_score_p10` | integer | Home team points scored IN period 10 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `home_score_p11` | integer | Home team points scored IN period 11 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `home_score_p12` | integer | Home team points scored IN period 12 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `home_score_p13` | integer | Home team points scored IN period 13 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `home_score_p14` | integer | Home team points scored IN period 14 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `away_score_p1` | integer | Away team points scored IN period 1 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `away_score_p2` | integer | Away team points scored IN period 2 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `away_score_p3` | integer | Away team points scored IN period 3 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `away_score_p4` | integer | Away team points scored IN period 4 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `away_score_p5` | integer | Away team points scored IN period 5 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `away_score_p6` | integer | Away team points scored IN period 6 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `away_score_p7` | integer | Away team points scored IN period 7 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `away_score_p8` | integer | Away team points scored IN period 8 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `away_score_p9` | integer | Away team points scored IN period 9 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `away_score_p10` | integer | Away team points scored IN period 10 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `away_score_p11` | integer | Away team points scored IN period 11 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `away_score_p12` | integer | Away team points scored IN period 12 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `away_score_p13` | integer | Away team points scored IN period 13 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `away_score_p14` | integer | Away team points scored IN period 14 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |

**`return_parsed=False`** — the raw body (`dict`): the game context (`Game`, `PlayByPlays`, `HomeBoxscores`, `AwayBoxscores`, `Summaries` and on v1 `Leaders`, `Points`) stamped `_request` (`schedule_key`, `feed`), or the schedule / standings page record.

### Raises {#bleague_game_info-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. B.LEAGUE has nothing for the request (a ScheduleKey no source holds, a schedule day without games, HTTP 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a context or page for another game, season, tab or day, an error or changed body, a truncated schedule list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A non-integer or out-of-range argument, or HTTP 400 / 422. |

### Example {#bleague_game_info-example}

```python
bleague_game_info(schedule_key=504734)
```

_Last validated n/a._

## bleague_period_scores

Points each team scored in each period of a game, overtime included.

**Endpoint URL:** `GET https://b-league.s3.amazonaws.com/web_json/contexts/{schedule_key}`

**Valid URL:** [https://b-league.s3.amazonaws.com/web_json/contexts/504734](https://b-league.s3.amazonaws.com/web_json/contexts/504734)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `schedule_key` | `schedule_key` |  | `Y` |  | Required. B.LEAGUE ScheduleKey (integer, one global sequence across B1, B2, B3 and every season), e.g. 504734 = B1 2025-26 sec 1, Alvark Tokyo v Utsunomiya Brex; list a day's keys with bleague_schedule. |
| `season` | `season` |  |  | `Y` | Optional guard: the season END year the game must belong to. A context whose Year differs raises AssetFetchError. Omitted = no check (the key alone selects the game). |

### Returns {#bleague_period_scores-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `schedule_key` | integer | B.LEAGUE ScheduleKey (key space: B.LEAGUE games, ONE global integer sequence across B1, B2, B3 and every season). The {schedule_key} every game wrapper takes and the schedule page's list-item id. |
| `season` | integer | Season END year (2026 = 2025-26), the feed's Year (the START year) plus one. |
| `feed` | character | Source feed of the row: v2 (v2_genius_contexts, B1/B2 from 2020-21) or v1 (v1_contexts: B3, B1/B2 2018-2020, and the 2016-17 / 2017-18 game-page embed). The feeds differ in id types, dates and some codes. |
| `period` | integer | Period number: 1-4 regulation; overtime numbering is unverified (no overtime sample). |
| `team_side` | character | Side of the game the row belongs to: home or away. |
| `team_id` | character | B.LEAGUE TeamID as text (key space: B.LEAGUE clubs, stable across seasons AND club renames: 703 = Tochigi Brex in 2016-17 = Utsunomiya Brex in 2025-26). The feed sends a string on v2 and an integer on v1; both are text here. Not a player id. |
| `points` | integer | Points the team scored IN this period (not cumulative); from the header's score slots. |

**`return_parsed=False`** — the raw body (`dict`): the game context (`Game`, `PlayByPlays`, `HomeBoxscores`, `AwayBoxscores`, `Summaries` and on v1 `Leaders`, `Points`) stamped `_request` (`schedule_key`, `feed`), or the schedule / standings page record.

### Raises {#bleague_period_scores-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. B.LEAGUE has nothing for the request (a ScheduleKey no source holds, a schedule day without games, HTTP 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a context or page for another game, season, tab or day, an error or changed body, a truncated schedule list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A non-integer or out-of-range argument, or HTTP 400 / 422. |

### Example {#bleague_period_scores-example}

```python
bleague_period_scores(schedule_key=504734)
```

_Last validated n/a._

## bleague_player_box

Player box score per period category: minutes, points, shooting, rebounds, assists, efficiency.

**Endpoint URL:** `GET https://b-league.s3.amazonaws.com/web_json/contexts/{schedule_key}`

**Valid URL:** [https://b-league.s3.amazonaws.com/web_json/contexts/504734](https://b-league.s3.amazonaws.com/web_json/contexts/504734)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `schedule_key` | `schedule_key` |  | `Y` |  | Required. B.LEAGUE ScheduleKey (integer, one global sequence across B1, B2, B3 and every season), e.g. 504734 = B1 2025-26 sec 1, Alvark Tokyo v Utsunomiya Brex; list a day's keys with bleague_schedule. |
| `season` | `season` |  |  | `Y` | Optional guard: the season END year the game must belong to. A context whose Year differs raises AssetFetchError. Omitted = no check (the key alone selects the game). |

### Returns {#bleague_player_box-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `schedule_key` | integer | B.LEAGUE ScheduleKey (key space: B.LEAGUE games, ONE global integer sequence across B1, B2, B3 and every season). The {schedule_key} every game wrapper takes and the schedule page's list-item id. |
| `season` | integer | Season END year (2026 = 2025-26), the feed's Year (the START year) plus one. |
| `feed` | character | Source feed of the row: v2 (v2_genius_contexts, B1/B2 from 2020-21) or v1 (v1_contexts: B3, B1/B2 2018-2020, and the 2016-17 / 2017-18 game-page embed). The feeds differ in id types, dates and some codes. |
| `team_side` | character | Side of the game the row belongs to: home or away. |
| `team_id` | character | B.LEAGUE TeamID as text (key space: B.LEAGUE clubs, stable across seasons AND club renames: 703 = Tochigi Brex in 2016-17 = Utsunomiya Brex in 2025-26). The feed sends a string on v2 and an integer on v1; both are text here. Not a player id. |
| `category` | integer | Row category: 1 = player, 2 = team / coaches row (team-credited stats; on v1 it names the head coach), 3 = team totals (合計). One row per (category, player, period category). |
| `period_category` | integer | Period category: 1-4 = quarters 1-4, 15 = first half, 16 = second half, 18 = whole game. Overtime codes are unverified. |
| `team_name_j` | character | Team name in Japanese (official name, e.g. アルバルク東京 = Alvark Tokyo). |
| `team_name_e` | character | Team name in English, uppercase ('ALVARK TOKYO'). |
| `player_id` | character | B.LEAGUE PlayerID as text (key space: B.LEAGUE persons, stable across seasons and clubs: Makoto Hiejima = '8589'). A string on v2, an integer on v1; both are text here. Null where the row names no player. |
| `player_no` | character | Shirt number as text, so '00' and '0' stay distinct; null on team and game-level rows. |
| `player_name_j` | character | Player name in Japanese (katakana for foreign players); null on team and game-level rows. |
| `player_name_e` | character | Player name in English ('Makoto Hiejima'); null on team and totals rows. |
| `play_time` | character | Minutes played as the feed's 'mm:ss'; 'DNP' = did not play. Totals rows hold 5 x the period length ('200:00' for the game). |
| `on_court` | logical | True for the five players on court when the context was last written (for a final game, at the final buzzer); the same value on every period row of the player. Null on v2 team and totals rows. |
| `display_order` | integer | Display order of the row (v1 only), integer; null on v2. |
| `starter` | logical | True when the player started; false for the bench; null on team and totals rows. (The feed sends 1 / null on v2 and a boolean on v1.) |
| `seconds_played` | integer | Time on court in seconds, parsed from play_time (50:00 = 3000); null for DNP and blank. |
| `points` | integer | Points scored. The v2 feed sends '' (a placeholder, not 0) on the team row of quarters; null here. |
| `fg3_made` | integer | Three-point field goals made (v2 placeholder '' on quarter team rows becomes null). |
| `fg3_attempted` | integer | Three-point field goals attempted. |
| `fg2_made` | integer | Two-point field goals made. |
| `fg2_attempted` | integer | Two-point field goals attempted. |
| `ft_made` | integer | Free throws made. |
| `ft_attempted` | integer | Free throws attempted. |
| `fouls` | integer | Personal fouls committed (F). |
| `fouls_drawn` | integer | Fouls drawn (被ファウル, FD). |
| `reb_off` | integer | Offensive rebounds (OR). |
| `reb_def` | integer | Defensive rebounds (DR). |
| `reb` | integer | Total rebounds. |
| `turnovers` | integer | Turnovers (TO). |
| `assists` | integer | Assists credited to the player or team (AS). |
| `steals` | integer | Steals credited to the player or team (ST). |
| `blocks` | integer | Blocks made (BS). |
| `blocks_received` | integer | Shots of this player or team that were blocked (被ブロック, BSON). |
| `ptfb` | integer | Fast-break figure (ファストブレイク); equals the summary's fast-break points on the one game checked, but whether it counts points or baskets is unverified. |
| `pt2in` | integer | Inside-the-paint figure: on v2 a COUNT of made inside-the-paint 2PT (the summary holds 2x that in points); on v1 points in the paint (0 on B3, whose 2PT shots are never split by paint). |
| `pt2nd` | integer | Second-chance figure (セカンドチャンス); smaller than the summary's second-chance points, so likely made baskets; unverified. |
| `ptpft` | integer | Points off turnovers (ポインツオフターンオーバー); v1 box rows only, null on v2. |
| `dunks` | integer | Dunks made (PT2DUNK); v1 box rows only, null on v2. |
| `efficiency` | integer | Efficiency rating (貢献度), era-dependent: v2 = points + rebounds + assists + steals + blocks + fouls drawn - missed FG - missed FT - turnovers - fouls - shots blocked (PIR); v1 omits fouls drawn, fouls and shots blocked. |
| `plus_minus` | integer | Plus/minus: team point difference while on court. v2 from 2022-23 only; null on totals and half rows. |
| `ast_to` | numeric | Assist-to-turnover ratio (0 when no turnovers). v2 from 2022-23 only; null on half rows. |
| `efg` | numeric | Effective field-goal percentage as a FRACTION 0-1 (3PT weighted 1.5x); the site multiplies by 100. v2 from 2022-23 only. |
| `ts` | numeric | True-shooting percentage as a FRACTION 0-1; the site multiplies by 100. v2 from 2022-23 only. |
| `usg` | numeric | Usage rate in PERCENT 0-100. v2 from 2022-23 only; absent on totals rows. |
| `off_rtg` | numeric | Offensive rating. v2 from 2022-23 only; on team and totals rows 0 in every sample, never filled. |
| `def_rtg` | numeric | Defensive rating. v2 from 2022-23 only; on team and totals rows 0 in every sample, never filled. |
| `net_rtg` | numeric | Net rating. v2 from 2022-23 only; on team and totals rows 0 in every sample, never filled. |
| `poss` | numeric | Possessions. v2 from 2022-23 only; on team and totals rows 0 in every sample, never filled. |

**`return_parsed=False`** — the raw body (`dict`): the game context (`Game`, `PlayByPlays`, `HomeBoxscores`, `AwayBoxscores`, `Summaries` and on v1 `Leaders`, `Points`) stamped `_request` (`schedule_key`, `feed`), or the schedule / standings page record.

### Raises {#bleague_player_box-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. B.LEAGUE has nothing for the request (a ScheduleKey no source holds, a schedule day without games, HTTP 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a context or page for another game, season, tab or day, an error or changed body, a truncated schedule list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A non-integer or out-of-range argument, or HTTP 400 / 422. |

### Example {#bleague_player_box-example}

```python
bleague_player_box(schedule_key=504734)
```

_Last validated n/a._

## bleague_pbp

Play-by-play of a finished game in game order, with decoded action codes and x / y from 2022-23.

**Endpoint URL:** `GET https://b-league.s3.amazonaws.com/web_json/contexts/{schedule_key}`

**Valid URL:** [https://b-league.s3.amazonaws.com/web_json/contexts/504734](https://b-league.s3.amazonaws.com/web_json/contexts/504734)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `schedule_key` | `schedule_key` |  | `Y` |  | Required. B.LEAGUE ScheduleKey (integer, one global sequence across B1, B2, B3 and every season), e.g. 504734 = B1 2025-26 sec 1, Alvark Tokyo v Utsunomiya Brex; list a day's keys with bleague_schedule. |
| `season` | `season` |  |  | `Y` | Optional guard: the season END year the game must belong to. A context whose Year differs raises AssetFetchError. Omitted = no check (the key alone selects the game). |

### Returns {#bleague_pbp-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `schedule_key` | integer | B.LEAGUE ScheduleKey (key space: B.LEAGUE games, ONE global integer sequence across B1, B2, B3 and every season). The {schedule_key} every game wrapper takes and the schedule page's list-item id. |
| `season` | integer | Season END year (2026 = 2025-26), the feed's Year (the START year) plus one. |
| `feed` | character | Source feed of the row: v2 (v2_genius_contexts, B1/B2 from 2020-21) or v1 (v1_contexts: B3, B1/B2 2018-2020, and the 2016-17 / 2017-18 game-page embed). The feeds differ in id types, dates and some codes. |
| `period` | integer | Period number: 1-4 regulation; overtime numbering is unverified (no overtime sample). |
| `event_no` | integer | Event sequence number, ascending in game order. v2 counts in steps of 1000 (inserted corrections may fall in between); v1 counts 1, 2, 3... |
| `action_code` | integer | ActionCD1, the primary action code: 1 / 2 3PT made / missed, 3 / 5 2PT outside the paint made / missed, 4 / 6 inside the paint, 7 / 8 free throw, 9 / 10 defensive / offensive rebound, 11 block, 12 assist, 13 turnover, 14 steal, 15 foul drawn, 16 and-one, 17 team turnover, 18 / 19 team rebounds (SWAPPED between feeds), 22-25 fouls, 80-85 game and clock events, 86 / 87 player in / out, 88 timeout, 89 substitution (v1). |
| `action` | character | English slug for action_code (fg3_made, ft_missed, rebound_def, foul_personal, player_in, ...). The team-rebound codes 18 / 19 follow the feed column; null for a code no sample contains. |
| `qualifier_code2` | integer | ActionCD2, a secondary qualifier (shot type, free throws awarded on a fouls, scoring context, 24 s violation); one code space shared with qualifier_code3. Null when the row has none. |
| `qualifier2` | character | English slug for qualifier_code2 (jump_shot, layup, dunk, free_throws_awarded_N, fast_break, second_chance, shooting_foul, ...); null when unknown. |
| `qualifier_code3` | integer | ActionCD3, a tertiary qualifier (scoring context on made shots, shooting foul / foul-out on fouls); same code space as qualifier_code2. |
| `qualifier3` | character | English slug for qualifier_code3; null when unknown. |
| `team_side` | character | Side of the acting team from HomeAway (1 home, 2 away); null on game-level rows and official timeouts. |
| `team_id` | character | B.LEAGUE TeamID as text (key space: B.LEAGUE clubs, stable across seasons AND club renames: 703 = Tochigi Brex in 2016-17 = Utsunomiya Brex in 2025-26). The feed sends a string on v2 and an integer on v1; both are text here. Not a player id. |
| `team_name_j` | character | Acting team name in Japanese; null on game-level rows and official timeouts. |
| `player_id` | character | Primary actor PlayerID as text (key space: B.LEAGUE persons). On a v1 substitution it is the player going out. Null on team rows and game-level rows. |
| `player_no` | character | Shirt number as text, so '00' and '0' stay distinct; null on team and game-level rows. |
| `player_name_j` | character | Player name in Japanese (katakana for foreign players); null on team and game-level rows. |
| `player2_id` | character | Second actor PlayerID as text; on a v1 substitution the player coming in. v1 / embed only. |
| `player2_no` | character | Shirt number of the second actor (v1 / embed only). |
| `player2_name_j` | character | Second actor's name in Japanese (v1 / embed only). |
| `rest_time` | character | Game clock REMAINING in the period as the feed's 'mm:ss' text (counts down from 10:00). |
| `clock_seconds` | integer | Game clock remaining in the period in whole seconds (8:50 = 530), derived from rest_time. |
| `score` | character | Running score 'home-away' after the event (e.g. '2-4'); null on player-in rows and v1 substitutions. |
| `home_score` | integer | Home running score after the event, split from score. |
| `away_score` | integer | Away running score after the event, split from score. |
| `period_end` | logical | True on the closing row of a period (ActionCD1 83). |
| `game_end` | logical | True on the game-end row on v1 and the embed; false on EVERY v2 row, the game-end row (81) included, so use action instead. |
| `x` | numeric | Event x: percent (0-100) of the court LENGTH from the 'left' baseline (baskets near 5 and 95). Metres (x / 100 x 28) are an inference. 2022-23 onward on v2 only. |
| `y` | numeric | Event y: percent (0-100) of the court WIDTH (baskets at 50). Which sideline is y = 0 is unverified; metres (y / 100 x 15) are an inference. 2022-23 onward on v2 only. |
| `area_code` | integer | Shot-zone code 1-13 read from the site's shot-chart SVG: 1 restricted area, 2 rest of the paint, 3 / 7 baseline mid-range, 4 / 6 wing mid-range, 5 top-of-the-key mid-range, 8 / 12 corner 3, 9 / 11 wing 3, 10 top 3, 13 deep band. Of each pair the first is the side far along the baseline. Also set on turnover and foul rows; 2022-23 onward. |
| `area` | character | English zone slug for area_code (restricted_area, paint, baseline_mid_range, wing_mid_range, top_of_key_mid_range, corner_3, wing_3, top_3, deep_3); mirrored codes share a slug. |
| `success` | logical | Outcome flag: true = made / successful, false = missed; true on non-shot rows. v2 from 2022-23 only. |
| `side` | character | Basket the play is charted at: 'left' (x near 0) or 'right' (x near 100); flips at half-time. Present from 2025-26 only; earlier seasons leave it null and the end must be inferred from x. |
| `play_text` | character | Play description in Japanese: '#{shirt} {surname} {action} ({running count})'. ○ = made, × = missed, フリースロー = free throw, ブロック = block, ディフェンス / オフェンスリバウンド = defensive / offensive rebound, → = substitution out to in (v1). |
| `recorded_at` | character | When the event was logged, as a UTC instant (v2 stores UTC text, v1 a JST-offset .NET date, the embed epoch seconds). |
| `edited_at` | character | When the event was last edited, as a UTC instant; null when never edited. |
| `code` | integer | Feed/organisation code repeated on every row: 546 on v2, 6 on v1; meaning unverified. |

**`return_parsed=False`** — the raw body (`dict`): the game context (`Game`, `PlayByPlays`, `HomeBoxscores`, `AwayBoxscores`, `Summaries` and on v1 `Leaders`, `Points`) stamped `_request` (`schedule_key`, `feed`), or the schedule / standings page record.

### Raises {#bleague_pbp-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. B.LEAGUE has nothing for the request (a ScheduleKey no source holds, a schedule day without games, HTTP 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a context or page for another game, season, tab or day, an error or changed body, a truncated schedule list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A non-integer or out-of-range argument, or HTTP 400 / 422. |

### Example {#bleague_pbp-example}

```python
bleague_pbp(schedule_key=504734)
```

_Last validated n/a._

## bleague_shots

Field-goal attempts with court position (2022-23 onward, v2 feed).

**Endpoint URL:** `GET https://b-league.s3.amazonaws.com/web_json/contexts/{schedule_key}`

**Valid URL:** [https://b-league.s3.amazonaws.com/web_json/contexts/504734](https://b-league.s3.amazonaws.com/web_json/contexts/504734)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `schedule_key` | `schedule_key` |  | `Y` |  | Required. B.LEAGUE ScheduleKey (integer, one global sequence across B1, B2, B3 and every season), e.g. 504734 = B1 2025-26 sec 1, Alvark Tokyo v Utsunomiya Brex; list a day's keys with bleague_schedule. |
| `season` | `season` |  |  | `Y` | Optional guard: the season END year the game must belong to. A context whose Year differs raises AssetFetchError. Omitted = no check (the key alone selects the game). |

### Returns {#bleague_shots-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `schedule_key` | integer | B.LEAGUE ScheduleKey (key space: B.LEAGUE games, ONE global integer sequence across B1, B2, B3 and every season). The {schedule_key} every game wrapper takes and the schedule page's list-item id. |
| `season` | integer | Season END year (2026 = 2025-26), the feed's Year (the START year) plus one. |
| `feed` | character | Source feed of the row: v2 (v2_genius_contexts, B1/B2 from 2020-21) or v1 (v1_contexts: B3, B1/B2 2018-2020, and the 2016-17 / 2017-18 game-page embed). The feeds differ in id types, dates and some codes. |
| `period` | integer | Period number: 1-4 regulation; overtime numbering is unverified (no overtime sample). |
| `event_no` | integer | Event sequence number, ascending in game order. v2 counts in steps of 1000 (inserted corrections may fall in between); v1 counts 1, 2, 3... |
| `team_side` | character | Side of the acting team from HomeAway (1 home, 2 away); null on game-level rows and official timeouts. |
| `team_id` | character | B.LEAGUE TeamID as text (key space: B.LEAGUE clubs, stable across seasons AND club renames: 703 = Tochigi Brex in 2016-17 = Utsunomiya Brex in 2025-26). The feed sends a string on v2 and an integer on v1; both are text here. Not a player id. |
| `player_id` | character | Primary actor PlayerID as text (key space: B.LEAGUE persons). On a v1 substitution it is the player going out. Null on team rows and game-level rows. |
| `player_no` | character | Shirt number as text, so '00' and '0' stay distinct; null on team and game-level rows. |
| `player_name_j` | character | Player name in Japanese (katakana for foreign players); null on team and game-level rows. |
| `shot_type` | character | 3pt or 2pt, from ActionCD1 (1-2 three-point, 3-6 two-point). |
| `in_paint` | logical | True for a two-point shot inside the paint, false outside; null for threes and on the B3 feed, whose 2PT codes do not split by paint. |
| `made` | logical | True when the shot was made (ActionCD1 odd for threes and 3 / 4), false when missed. |
| `qualifier2` | character | English slug for the shot's ActionCD2 (jump_shot, layup, dunk); null when unknown. |
| `qualifier3` | character | English slug for the shot's ActionCD3 (fast_break, points_off_turnover, second_chance, ...); null when none. |
| `x` | numeric | Event x: percent (0-100) of the court LENGTH from the 'left' baseline (baskets near 5 and 95). Metres (x / 100 x 28) are an inference. 2022-23 onward on v2 only. |
| `y` | numeric | Event y: percent (0-100) of the court WIDTH (baskets at 50). Which sideline is y = 0 is unverified; metres (y / 100 x 15) are an inference. 2022-23 onward on v2 only. |
| `area_code` | integer | Shot-zone code 1-13 read from the site's shot-chart SVG: 1 restricted area, 2 rest of the paint, 3 / 7 baseline mid-range, 4 / 6 wing mid-range, 5 top-of-the-key mid-range, 8 / 12 corner 3, 9 / 11 wing 3, 10 top 3, 13 deep band. Of each pair the first is the side far along the baseline. Also set on turnover and foul rows; 2022-23 onward. |
| `area` | character | English zone slug for area_code (restricted_area, paint, baseline_mid_range, wing_mid_range, top_of_key_mid_range, corner_3, wing_3, top_3, deep_3); mirrored codes share a slug. |
| `side` | character | Basket the play is charted at: 'left' (x near 0) or 'right' (x near 100); flips at half-time. Present from 2025-26 only; earlier seasons leave it null and the end must be inferred from x. |
| `rest_time` | character | Game clock REMAINING in the period as the feed's 'mm:ss' text (counts down from 10:00). |
| `clock_seconds` | integer | Game clock remaining in the period in whole seconds (8:50 = 530), derived from rest_time. |
| `score` | character | Running score 'home-away' after the event (e.g. '2-4'); null on player-in rows and v1 substitutions. |

**`return_parsed=False`** — the raw body (`dict`): the game context (`Game`, `PlayByPlays`, `HomeBoxscores`, `AwayBoxscores`, `Summaries` and on v1 `Leaders`, `Points`) stamped `_request` (`schedule_key`, `feed`), or the schedule / standings page record.

### Raises {#bleague_shots-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. B.LEAGUE has nothing for the request (a ScheduleKey no source holds, a schedule day without games, HTTP 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a context or page for another game, season, tab or day, an error or changed body, a truncated schedule list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A non-integer or out-of-range argument, or HTTP 400 / 422. |

### Example {#bleague_shots-example}

```python
bleague_shots(schedule_key=504734)
```

_Last validated n/a._

## bleague_leaders

Team leaders per statistic and period category (v1 feed only).

**Endpoint URL:** `GET https://b-league.s3.amazonaws.com/web_json/contexts/{schedule_key}`

**Valid URL:** [https://b-league.s3.amazonaws.com/web_json/contexts/504734](https://b-league.s3.amazonaws.com/web_json/contexts/504734)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `schedule_key` | `schedule_key` |  | `Y` |  | Required. B.LEAGUE ScheduleKey (integer, one global sequence across B1, B2, B3 and every season), e.g. 504734 = B1 2025-26 sec 1, Alvark Tokyo v Utsunomiya Brex; list a day's keys with bleague_schedule. |
| `season` | `season` |  |  | `Y` | Optional guard: the season END year the game must belong to. A context whose Year differs raises AssetFetchError. Omitted = no check (the key alone selects the game). |

### Returns {#bleague_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `schedule_key` | integer | B.LEAGUE ScheduleKey (key space: B.LEAGUE games, ONE global integer sequence across B1, B2, B3 and every season). The {schedule_key} every game wrapper takes and the schedule page's list-item id. |
| `season` | integer | Season END year (2026 = 2025-26), the feed's Year (the START year) plus one. |
| `feed` | character | Source feed of the row: v2 (v2_genius_contexts, B1/B2 from 2020-21) or v1 (v1_contexts: B3, B1/B2 2018-2020, and the 2016-17 / 2017-18 game-page embed). The feeds differ in id types, dates and some codes. |
| `period_category` | integer | Period category: 1-4 = quarters 1-4, 15 = first half, 16 = second half, 18 = whole game. Overtime codes are unverified. |
| `category` | integer | Leader category code, decoded by matching count to the box score: 1 points, 2 assists, 3 steals, 4 blocks, 5 rebounds, 8 shots blocked. |
| `category_name` | character | English name of category (points, assists, steals, blocks, rebounds, blocks_received); null for an unknown code. |
| `rank` | integer | Rank within the team for the category (1 = best; ties share a rank). |
| `tie_order` | integer | Order within a tied rank (1 = first listed). |
| `team_side` | character | Side of the game the row belongs to: home or away. |
| `team_id` | character | B.LEAGUE TeamID as text (key space: B.LEAGUE clubs, stable across seasons AND club renames: 703 = Tochigi Brex in 2016-17 = Utsunomiya Brex in 2025-26). The feed sends a string on v2 and an integer on v1; both are text here. Not a player id. |
| `team_name_j` | character | Team name in Japanese (official name, e.g. アルバルク東京 = Alvark Tokyo). |
| `player_id` | character | B.LEAGUE PlayerID as text (key space: B.LEAGUE persons, stable across seasons and clubs: Makoto Hiejima = '8589'). A string on v2, an integer on v1; both are text here. Null where the row names no player. |
| `player_no` | character | Shirt number as text, so '00' and '0' stay distinct; null on team and game-level rows. |
| `player_name_j` | character | Player name in Japanese (katakana for foreign players); null on team and game-level rows. |
| `count` | integer | The player's value in the category for the period category. |
| `reb_off_count` | integer | Offensive-rebound count carried on the row (filled for the rebounds category). |
| `reb_def_count` | integer | Defensive-rebound count carried on the row (filled for the rebounds category). |

**`return_parsed=False`** — the raw body (`dict`): the game context (`Game`, `PlayByPlays`, `HomeBoxscores`, `AwayBoxscores`, `Summaries` and on v1 `Leaders`, `Points`) stamped `_request` (`schedule_key`, `feed`), or the schedule / standings page record.

### Raises {#bleague_leaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. B.LEAGUE has nothing for the request (a ScheduleKey no source holds, a schedule day without games, HTTP 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a context or page for another game, season, tab or day, an error or changed body, a truncated schedule list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A non-integer or out-of-range argument, or HTTP 400 / 422. |

### Example {#bleague_leaders-example}

```python
bleague_leaders(schedule_key=504734)
```

_Last validated n/a._

## bleague_score_track

Running score after each scoring event (v1 feed only).

**Endpoint URL:** `GET https://b-league.s3.amazonaws.com/web_json/contexts/{schedule_key}`

**Valid URL:** [https://b-league.s3.amazonaws.com/web_json/contexts/504734](https://b-league.s3.amazonaws.com/web_json/contexts/504734)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `schedule_key` | `schedule_key` |  | `Y` |  | Required. B.LEAGUE ScheduleKey (integer, one global sequence across B1, B2, B3 and every season), e.g. 504734 = B1 2025-26 sec 1, Alvark Tokyo v Utsunomiya Brex; list a day's keys with bleague_schedule. |
| `season` | `season` |  |  | `Y` | Optional guard: the season END year the game must belong to. A context whose Year differs raises AssetFetchError. Omitted = no check (the key alone selects the game). |

### Returns {#bleague_score_track-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `schedule_key` | integer | B.LEAGUE ScheduleKey (key space: B.LEAGUE games, ONE global integer sequence across B1, B2, B3 and every season). The {schedule_key} every game wrapper takes and the schedule page's list-item id. |
| `season` | integer | Season END year (2026 = 2025-26), the feed's Year (the START year) plus one. |
| `feed` | character | Source feed of the row: v2 (v2_genius_contexts, B1/B2 from 2020-21) or v1 (v1_contexts: B3, B1/B2 2018-2020, and the 2016-17 / 2017-18 game-page embed). The feeds differ in id types, dates and some codes. |
| `period` | integer | Period number: 1-4 regulation; overtime numbering is unverified (no overtime sample). |
| `event_index` | integer | Index of the scoring event, 0 = the 0-0 start row, then 1, 2... (the score track's x axis; NOT a clock). |
| `home_points` | integer | Home running total after the event. |
| `away_points` | integer | Away running total after the event. |

**`return_parsed=False`** — the raw body (`dict`): the game context (`Game`, `PlayByPlays`, `HomeBoxscores`, `AwayBoxscores`, `Summaries` and on v1 `Leaders`, `Points`) stamped `_request` (`schedule_key`, `feed`), or the schedule / standings page record.

### Raises {#bleague_score_track-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. B.LEAGUE has nothing for the request (a ScheduleKey no source holds, a schedule day without games, HTTP 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a context or page for another game, season, tab or day, an error or changed body, a truncated schedule list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A non-integer or out-of-range argument, or HTTP 400 / 422. |

### Example {#bleague_score_track-example}

```python
bleague_score_track(schedule_key=504734)
```

_Last validated n/a._

## bleague_standings

Standings of one season and division, per conference / division table and wild card.

**Endpoint URL:** `GET https://www.bleague.jp/standings/`

**Valid URL:** [https://www.bleague.jp/standings/?season=2026&tab=1](https://www.bleague.jp/standings/?season=2026&tab=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Required. Season END year (2026 = the 2025-26 season). The site's own year is the START year; ``bleague_runtime`` sends ``season - 1``. |
| `tab` | `tab` |  | `Y` |  | Required. Division tab: 1 = B1, 2 = B2, 3 = B3. |

### Returns {#bleague_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2026 = 2025-26), the feed's Year (the START year) plus one. |
| `tab` | integer | Division tab: 1 = B1, 2 = B2, 3 = B3. |
| `season_label` | character | Season label of the selected option (2025-26). |
| `section` | character | Table heading in Japanese: 東地区 East, 中地区 Central, 西地区 West, ワイルドカード wild card. |
| `section_en` | character | English name of section for the four known headings; null for any other heading. |
| `rank` | integer | Rank in that table (順位); a club in a division table also appears in the wild-card table. |
| `team_id` | character | B.LEAGUE TeamID as text (key space: B.LEAGUE clubs, stable across seasons AND club renames: 703 = Tochigi Brex in 2016-17 = Utsunomiya Brex in 2025-26). The feed sends a string on v2 and an integer on v1; both are text here. Not a player id. |
| `team` | character | Club name in Japanese as the standings page prints it (千葉ジェッツ = Chiba Jets). |
| `team_short` | character | Club short name in Japanese (千葉J). |
| `wins` | integer | Games won (勝) so far this season in the table. |
| `losses` | integer | Games lost (負) so far this season in the table. |
| `win_pct` | numeric | Win percentage (勝率) as a 0-1 fraction. |
| `games_behind` | numeric | Games behind the table leader (差). |
| `points_for` | integer | Points scored (得点) over the season to date. |
| `points_against` | integer | Points allowed (失点) over the season to date. |
| `point_diff` | integer | Point differential (得失点差). |
| `home_record` | character | Home record W-L (ホーム). |
| `away_record` | character | Away record W-L (アウェー). |
| `last_n` | integer | Length of the recent-form window the page shows: 5 or 10 games (過去5試合 / 過去10試合). |
| `last_n_record` | character | Record W-L over the last_n games. |
| `streak` | character | Current streak (連勝/連敗) as printed: W5 = five straight wins, L1 = one loss. |
| `division_title` | character | Division-title mark (地区優勝) as printed; null when none ('--'). |

**`return_parsed=False`** — the raw body (`dict`): the game context (`Game`, `PlayByPlays`, `HomeBoxscores`, `AwayBoxscores`, `Summaries` and on v1 `Leaders`, `Points`) stamped `_request` (`schedule_key`, `feed`), or the schedule / standings page record.

### Raises {#bleague_standings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. B.LEAGUE has nothing for the request (a ScheduleKey no source holds, a schedule day without games, HTTP 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (an unfinished game, a context or page for another game, season, tab or day, an error or changed body, a truncated schedule list). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A non-integer or out-of-range argument, or HTTP 400 / 422. |

### Example {#bleague_standings-example}

```python
bleague_standings(season=2026, tab=1)
```

_Last validated n/a._
