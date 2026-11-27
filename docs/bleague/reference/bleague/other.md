# B.LEAGUE — B.LEAGUE game contexts (S3) and pages (www.bleague.jp, Japan) — Other

> B.LEAGUE — B.LEAGUE game contexts (S3) and pages (www.bleague.jp, Japan) — Other — function reference in sdv-py, the SportsDataverse Python package.

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
| `schedule_key` | Int64 | B.LEAGUE ScheduleKey (key space: B.LEAGUE games, ONE global integer sequence across B1, B2, B3 and every season). The {schedule_key} every game wrapper takes and the schedule page's list-item id. |
| `season` | Int64 | Season END year (2026 = 2025-26), the feed's Year (the START year) plus one. |
| `feed` | String | Source feed of the row: v2 (v2_genius_contexts, B1/B2 from 2020-21) or v1 (v1_contexts: B3, B1/B2 2018-2020, and the 2016-17 / 2017-18 game-page embed). The feeds differ in id types, dates and some codes. |
| `year` | Int64 | Season START year as the site stores it (2025 = 2025-26); season is the END year. |
| `code` | Int64 | Feed/organisation code repeated on every row: 546 on v2, 6 on v1; meaning unverified. |
| `event` | Int64 | Competition type (v1 header only): 2 B1 league, 3 B1 Championship, 4 B1 relegation playoffs, 5 all-star, 7 B2 league, 8 B2 playoffs, 11 B1-B2 promotion/relegation, 15 B3 league, 32 B3 playoffs; null on v2. |
| `convention_key` | String | ConventionKey as text (key space: competition-seasons, 大会): v2 '514' = B1 2025-26, v1 102 = B3 2025-26. Key on (feed, convention_key); the feeds may number independently. |
| `convention_class` | Int64 | Competition class code (1 on every sample); meaning unverified. |
| `round` | Int64 | Round (節, Setu) within the competition-season; playoffs use 100+ (101). Integer. |
| `game_no` | Int64 | Game number within the competition (v1 header only); null on v2. |
| `schedule_state` | Int64 | Schedule state code (v1 header only; 2 on the final-game sample); meaning unverified. |
| `schedule_conference` | Int64 | Conference code (v1 header only); meaning unverified. |
| `interleague` | Boolean | True for an inter-league game (v1 header only). |
| `stadium_cd` | String | StadiumCD as text (key space: B.LEAGUE venues; stability unverified). |
| `attendance` | Int64 | Announced attendance. |
| `game_datetime` | String | Scheduled tip-off as a UTC instant (the feed's JST wall clock minus nine hours). |
| `game_start_time` | String | Actual start as a UTC instant (v1 header only; epoch seconds on the 2016-17 embed). |
| `game_end_time` | String | Actual end as a UTC instant (v1 header only). |
| `max_period` | Int64 | Number of periods the game had (4 plus overtimes). |
| `current_period` | Int64 | Period the context was last written in (the last period for a final game). |
| `rest_time` | Int64 | Clock remaining when last written (v1 header only; 0 for a final game). |
| `game_ended` | Boolean | True when the game has ended; the runtime serves only games where this and record_fixed are true. |
| `record_fixed` | Boolean | True when the record is finalised (no more corrections expected). |
| `box_exists` | Boolean | True when the box score exists. |
| `pbp_exists` | Boolean | True when play-by-play exists. |
| `picture_exists` | Boolean | True when photos exist (v1 header only). |
| `report_exists` | Boolean | True when a game report text exists (v1 header only). |
| `home_win` | Boolean | True when the home team won (v1 header only); null on v2. |
| `commissioner_id` | String | Commissioner (コミッショナー) id as text; null when none is assigned. |
| `referee_id` | String | Crew-chief referee RefereeID as text (key space: officials; stability unverified). |
| `sub_referee_id1` | String | Second referee RefereeID as text. |
| `sub_referee_id2` | String | Third referee RefereeID as text. |
| `temperature` | Float64 | Temperature; null in every observed body, so its dtype is an assumption. |
| `humidity` | Float64 | Humidity; null in every observed body, so its dtype is an assumption. |
| `convention_name_j` | String | Competition name in Japanese (e.g. りそなグループ B.LEAGUE 2025-26 B1リーグ戦 = B1 regular season). |
| `convention_name_e` | String | Competition name in English; empty (null) on early v1 bodies. |
| `convention_title_j` | String | Competition title in Japanese (v1 header only). |
| `convention_title_e` | String | Competition title in English (v1 header only). |
| `stadium_name_j` | String | Venue name in Japanese (e.g. TOYOTA ARENA TOKYO, AGF鈴鹿体育館 = AGF Suzuka Gymnasium). |
| `stadium_name_e` | String | Venue name in English; often empty (null). |
| `court` | String | Court number at the venue (v1 header only). |
| `report` | String | Game report text (v1 header only); usually empty (null). |
| `weather` | String | Weather text (v1 header only); null in every observed body. |
| `schedule_class_name_j` | String | Schedule class name in Japanese (v1 header only); usually empty. |
| `schedule_class_name_e` | String | Schedule class name in English (v1 header only); usually empty. |
| `schedule_class_short_name_j` | String | Schedule class short name in Japanese (v1 header only); usually empty. |
| `schedule_class_short_name_e` | String | Schedule class short name in English (v1 header only); usually empty. |
| `commissioner_name_j` | String | Commissioner name in Japanese (names only; no contact data is published). |
| `commissioner_name_e` | String | Commissioner name in English (v1 header only). |
| `referee_name_j` | String | Crew-chief referee name in Japanese (名 only; no contact data is published). |
| `referee_name_e` | String | Crew-chief referee name in English (v1 header only). |
| `sub_referee_name_j1` | String | Second referee name in Japanese. |
| `sub_referee_name_e1` | String | Second referee name in English (v1 header only). |
| `sub_referee_name_j2` | String | Third referee name in Japanese. |
| `sub_referee_name_e2` | String | Third referee name in English (v1 header only). |
| `home_team_id` | String | Home team id. B.LEAGUE TeamID as text (key space: B.LEAGUE clubs, stable across seasons AND club renames: 703 = Tochigi Brex in 2016-17 = Utsunomiya Brex in 2025-26). The feed sends a string on v2 and an integer on v1; both are text here. Not a player id. |
| `home_team_name_j` | String | Home team name in Japanese (official name). |
| `home_team_name_e` | String | Home team name in English, uppercase. |
| `home_team_short_name_j` | String | Home team short name in Japanese (e.g. A東京). |
| `home_team_short_name_e` | String | Home team short name in English (e.g. A TOKYO). |
| `home_team_sub_name_j` | String | Home team sub name in Japanese (v1 header only); usually empty. |
| `home_team_sub_name_e` | String | Home team sub name in English (v1 header only); usually empty. |
| `home_score` | Int64 | Home team final score. |
| `home_winning_count` | Int64 | Home team winning streak (v1 header only); null in every observed body. |
| `home_losing_count` | Int64 | Home team losing streak (v1 header only); null in every observed body. |
| `away_team_id` | String | Away team id. B.LEAGUE TeamID as text (key space: B.LEAGUE clubs, stable across seasons AND club renames: 703 = Tochigi Brex in 2016-17 = Utsunomiya Brex in 2025-26). The feed sends a string on v2 and an integer on v1; both are text here. Not a player id. |
| `away_team_name_j` | String | Away team name in Japanese (official name). |
| `away_team_name_e` | String | Away team name in English, uppercase. |
| `away_team_short_name_j` | String | Away team short name in Japanese (e.g. A東京). |
| `away_team_short_name_e` | String | Away team short name in English (e.g. A TOKYO). |
| `away_team_sub_name_j` | String | Away team sub name in Japanese (v1 header only); usually empty. |
| `away_team_sub_name_e` | String | Away team sub name in English (v1 header only); usually empty. |
| `away_score` | Int64 | Away team final score. |
| `away_winning_count` | Int64 | Away team winning streak (v1 header only); null in every observed body. |
| `away_losing_count` | Int64 | Away team losing streak (v1 header only); null in every observed body. |
| `home_score_p1` | Int64 | Home team points scored IN period 1 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `home_score_p2` | Int64 | Home team points scored IN period 2 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `home_score_p3` | Int64 | Home team points scored IN period 3 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `home_score_p4` | Int64 | Home team points scored IN period 4 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `home_score_p5` | Int64 | Home team points scored IN period 5 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `home_score_p6` | Int64 | Home team points scored IN period 6 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `home_score_p7` | Int64 | Home team points scored IN period 7 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `home_score_p8` | Int64 | Home team points scored IN period 8 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `home_score_p9` | Int64 | Home team points scored IN period 9 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `home_score_p10` | Int64 | Home team points scored IN period 10 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `home_score_p11` | Int64 | Home team points scored IN period 11 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `home_score_p12` | Int64 | Home team points scored IN period 12 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `home_score_p13` | Int64 | Home team points scored IN period 13 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `home_score_p14` | Int64 | Home team points scored IN period 14 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `away_score_p1` | Int64 | Away team points scored IN period 1 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `away_score_p2` | Int64 | Away team points scored IN period 2 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `away_score_p3` | Int64 | Away team points scored IN period 3 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `away_score_p4` | Int64 | Away team points scored IN period 4 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `away_score_p5` | Int64 | Away team points scored IN period 5 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `away_score_p6` | Int64 | Away team points scored IN period 6 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `away_score_p7` | Int64 | Away team points scored IN period 7 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `away_score_p8` | Int64 | Away team points scored IN period 8 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `away_score_p9` | Int64 | Away team points scored IN period 9 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `away_score_p10` | Int64 | Away team points scored IN period 10 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `away_score_p11` | Int64 | Away team points scored IN period 11 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `away_score_p12` | Int64 | Away team points scored IN period 12 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `away_score_p13` | Int64 | Away team points scored IN period 13 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |
| `away_score_p14` | Int64 | Away team points scored IN period 14 (not cumulative). v2 headers carry four slots, v1 fourteen; an unplayed period is 0 on v1 and null on v2. |

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
| `schedule_key` | Int64 | B.LEAGUE ScheduleKey (key space: B.LEAGUE games, ONE global integer sequence across B1, B2, B3 and every season). The {schedule_key} every game wrapper takes and the schedule page's list-item id. |
| `season` | Int64 | Season END year (2026 = 2025-26), the feed's Year (the START year) plus one. |
| `feed` | String | Source feed of the row: v2 (v2_genius_contexts, B1/B2 from 2020-21) or v1 (v1_contexts: B3, B1/B2 2018-2020, and the 2016-17 / 2017-18 game-page embed). The feeds differ in id types, dates and some codes. |
| `period` | Int64 | Period number: 1-4 regulation; overtime numbering is unverified (no overtime sample). |
| `team_side` | String | Side of the game the row belongs to: home or away. |
| `team_id` | String | B.LEAGUE TeamID as text (key space: B.LEAGUE clubs, stable across seasons AND club renames: 703 = Tochigi Brex in 2016-17 = Utsunomiya Brex in 2025-26). The feed sends a string on v2 and an integer on v1; both are text here. Not a player id. |
| `points` | Int64 | Points the team scored IN this period (not cumulative); from the header's score slots. |

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
| `schedule_key` | Int64 | B.LEAGUE ScheduleKey (key space: B.LEAGUE games, ONE global integer sequence across B1, B2, B3 and every season). The {schedule_key} every game wrapper takes and the schedule page's list-item id. |
| `season` | Int64 | Season END year (2026 = 2025-26), the feed's Year (the START year) plus one. |
| `feed` | String | Source feed of the row: v2 (v2_genius_contexts, B1/B2 from 2020-21) or v1 (v1_contexts: B3, B1/B2 2018-2020, and the 2016-17 / 2017-18 game-page embed). The feeds differ in id types, dates and some codes. |
| `period` | Int64 | Period number: 1-4 regulation; overtime numbering is unverified (no overtime sample). |
| `event_no` | Int64 | Event sequence number, ascending in game order. v2 counts in steps of 1000 (inserted corrections may fall in between); v1 counts 1, 2, 3... |
| `action_code` | Int64 | ActionCD1, the primary action code: 1 / 2 3PT made / missed, 3 / 5 2PT outside the paint made / missed, 4 / 6 inside the paint, 7 / 8 free throw, 9 / 10 defensive / offensive rebound, 11 block, 12 assist, 13 turnover, 14 steal, 15 foul drawn, 16 and-one, 17 team turnover, 18 / 19 team rebounds (SWAPPED between feeds), 22-25 fouls, 80-85 game and clock events, 86 / 87 player in / out, 88 timeout, 89 substitution (v1). |
| `action` | String | English slug for action_code (fg3_made, ft_missed, rebound_def, foul_personal, player_in, ...). The team-rebound codes 18 / 19 follow the feed column; null for a code no sample contains. |
| `qualifier_code2` | Int64 | ActionCD2, a secondary qualifier (shot type, free throws awarded on a fouls, scoring context, 24 s violation); one code space shared with qualifier_code3. Null when the row has none. |
| `qualifier2` | String | English slug for qualifier_code2 (jump_shot, layup, dunk, free_throws_awarded_N, fast_break, second_chance, shooting_foul, ...); null when unknown. |
| `qualifier_code3` | Int64 | ActionCD3, a tertiary qualifier (scoring context on made shots, shooting foul / foul-out on fouls); same code space as qualifier_code2. |
| `qualifier3` | String | English slug for qualifier_code3; null when unknown. |
| `team_side` | String | Side of the acting team from HomeAway (1 home, 2 away); null on game-level rows and official timeouts. |
| `team_id` | String | B.LEAGUE TeamID as text (key space: B.LEAGUE clubs, stable across seasons AND club renames: 703 = Tochigi Brex in 2016-17 = Utsunomiya Brex in 2025-26). The feed sends a string on v2 and an integer on v1; both are text here. Not a player id. |
| `team_name_j` | String | Acting team name in Japanese; null on game-level rows and official timeouts. |
| `player_id` | String | Primary actor PlayerID as text (key space: B.LEAGUE persons). On a v1 substitution it is the player going out. Null on team rows and game-level rows. |
| `player_no` | String | Shirt number as text, so '00' and '0' stay distinct; null on team and game-level rows. |
| `player_name_j` | String | Player name in Japanese (katakana for foreign players); null on team and game-level rows. |
| `player2_id` | String | Second actor PlayerID as text; on a v1 substitution the player coming in. v1 / embed only. |
| `player2_no` | String | Shirt number of the second actor (v1 / embed only). |
| `player2_name_j` | String | Second actor's name in Japanese (v1 / embed only). |
| `rest_time` | String | Game clock REMAINING in the period as the feed's 'mm:ss' text (counts down from 10:00). |
| `clock_seconds` | Int64 | Game clock remaining in the period in whole seconds (8:50 = 530), derived from rest_time. |
| `score` | String | Running score 'home-away' after the event (e.g. '2-4'); null on player-in rows and v1 substitutions. |
| `home_score` | Int64 | Home running score after the event, split from score. |
| `away_score` | Int64 | Away running score after the event, split from score. |
| `period_end` | Boolean | True on the closing row of a period (ActionCD1 83). |
| `game_end` | Boolean | True on the game-end row on v1 and the embed; false on EVERY v2 row, the game-end row (81) included, so use action instead. |
| `x` | Float64 | Event x: percent (0-100) of the court LENGTH from the 'left' baseline (baskets near 5 and 95). Metres (x / 100 x 28) are an inference. 2022-23 onward on v2 only. |
| `y` | Float64 | Event y: percent (0-100) of the court WIDTH (baskets at 50). Which sideline is y = 0 is unverified; metres (y / 100 x 15) are an inference. 2022-23 onward on v2 only. |
| `area_code` | Int64 | Shot-zone code 1-13 read from the site's shot-chart SVG: 1 restricted area, 2 rest of the paint, 3 / 7 baseline mid-range, 4 / 6 wing mid-range, 5 top-of-the-key mid-range, 8 / 12 corner 3, 9 / 11 wing 3, 10 top 3, 13 deep band. Of each pair the first is the side far along the baseline. Also set on turnover and foul rows; 2022-23 onward. |
| `area` | String | English zone slug for area_code (restricted_area, paint, baseline_mid_range, wing_mid_range, top_of_key_mid_range, corner_3, wing_3, top_3, deep_3); mirrored codes share a slug. |
| `success` | Boolean | Outcome flag: true = made / successful, false = missed; true on non-shot rows. v2 from 2022-23 only. |
| `side` | String | Basket the play is charted at: 'left' (x near 0) or 'right' (x near 100); flips at half-time. Present from 2025-26 only; earlier seasons leave it null and the end must be inferred from x. |
| `play_text` | String | Play description in Japanese: '#{shirt} {surname} {action} ({running count})'. ○ = made, × = missed, フリースロー = free throw, ブロック = block, ディフェンス / オフェンスリバウンド = defensive / offensive rebound, → = substitution out to in (v1). |
| `recorded_at` | String | When the event was logged, as a UTC instant (v2 stores UTC text, v1 a JST-offset .NET date, the embed epoch seconds). |
| `edited_at` | String | When the event was last edited, as a UTC instant; null when never edited. |
| `code` | Int64 | Feed/organisation code repeated on every row: 546 on v2, 6 on v1; meaning unverified. |

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
| `schedule_key` | Int64 | B.LEAGUE ScheduleKey (key space: B.LEAGUE games, ONE global integer sequence across B1, B2, B3 and every season). The {schedule_key} every game wrapper takes and the schedule page's list-item id. |
| `season` | Int64 | Season END year (2026 = 2025-26), the feed's Year (the START year) plus one. |
| `feed` | String | Source feed of the row: v2 (v2_genius_contexts, B1/B2 from 2020-21) or v1 (v1_contexts: B3, B1/B2 2018-2020, and the 2016-17 / 2017-18 game-page embed). The feeds differ in id types, dates and some codes. |
| `period` | Int64 | Period number: 1-4 regulation; overtime numbering is unverified (no overtime sample). |
| `event_no` | Int64 | Event sequence number, ascending in game order. v2 counts in steps of 1000 (inserted corrections may fall in between); v1 counts 1, 2, 3... |
| `team_side` | String | Side of the acting team from HomeAway (1 home, 2 away); null on game-level rows and official timeouts. |
| `team_id` | String | B.LEAGUE TeamID as text (key space: B.LEAGUE clubs, stable across seasons AND club renames: 703 = Tochigi Brex in 2016-17 = Utsunomiya Brex in 2025-26). The feed sends a string on v2 and an integer on v1; both are text here. Not a player id. |
| `player_id` | String | Primary actor PlayerID as text (key space: B.LEAGUE persons). On a v1 substitution it is the player going out. Null on team rows and game-level rows. |
| `player_no` | String | Shirt number as text, so '00' and '0' stay distinct; null on team and game-level rows. |
| `player_name_j` | String | Player name in Japanese (katakana for foreign players); null on team and game-level rows. |
| `shot_type` | String | 3pt or 2pt, from ActionCD1 (1-2 three-point, 3-6 two-point). |
| `in_paint` | Boolean | True for a two-point shot inside the paint, false outside; null for threes and on the B3 feed, whose 2PT codes do not split by paint. |
| `made` | Boolean | True when the shot was made (ActionCD1 odd for threes and 3 / 4), false when missed. |
| `qualifier2` | String | English slug for the shot's ActionCD2 (jump_shot, layup, dunk); null when unknown. |
| `qualifier3` | String | English slug for the shot's ActionCD3 (fast_break, points_off_turnover, second_chance, ...); null when none. |
| `x` | Float64 | Event x: percent (0-100) of the court LENGTH from the 'left' baseline (baskets near 5 and 95). Metres (x / 100 x 28) are an inference. 2022-23 onward on v2 only. |
| `y` | Float64 | Event y: percent (0-100) of the court WIDTH (baskets at 50). Which sideline is y = 0 is unverified; metres (y / 100 x 15) are an inference. 2022-23 onward on v2 only. |
| `area_code` | Int64 | Shot-zone code 1-13 read from the site's shot-chart SVG: 1 restricted area, 2 rest of the paint, 3 / 7 baseline mid-range, 4 / 6 wing mid-range, 5 top-of-the-key mid-range, 8 / 12 corner 3, 9 / 11 wing 3, 10 top 3, 13 deep band. Of each pair the first is the side far along the baseline. Also set on turnover and foul rows; 2022-23 onward. |
| `area` | String | English zone slug for area_code (restricted_area, paint, baseline_mid_range, wing_mid_range, top_of_key_mid_range, corner_3, wing_3, top_3, deep_3); mirrored codes share a slug. |
| `side` | String | Basket the play is charted at: 'left' (x near 0) or 'right' (x near 100); flips at half-time. Present from 2025-26 only; earlier seasons leave it null and the end must be inferred from x. |
| `rest_time` | String | Game clock REMAINING in the period as the feed's 'mm:ss' text (counts down from 10:00). |
| `clock_seconds` | Int64 | Game clock remaining in the period in whole seconds (8:50 = 530), derived from rest_time. |
| `score` | String | Running score 'home-away' after the event (e.g. '2-4'); null on player-in rows and v1 substitutions. |

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
| `schedule_key` | Int64 | B.LEAGUE ScheduleKey (key space: B.LEAGUE games, ONE global integer sequence across B1, B2, B3 and every season). The {schedule_key} every game wrapper takes and the schedule page's list-item id. |
| `season` | Int64 | Season END year (2026 = 2025-26), the feed's Year (the START year) plus one. |
| `feed` | String | Source feed of the row: v2 (v2_genius_contexts, B1/B2 from 2020-21) or v1 (v1_contexts: B3, B1/B2 2018-2020, and the 2016-17 / 2017-18 game-page embed). The feeds differ in id types, dates and some codes. |
| `period_category` | Int64 | Period category: 1-4 = quarters 1-4, 15 = first half, 16 = second half, 18 = whole game. Overtime codes are unverified. |
| `category` | Int64 | Leader category code, decoded by matching count to the box score: 1 points, 2 assists, 3 steals, 4 blocks, 5 rebounds, 8 shots blocked. |
| `category_name` | String | English name of category (points, assists, steals, blocks, rebounds, blocks_received); null for an unknown code. |
| `rank` | Int64 | Rank within the team for the category (1 = best; ties share a rank). |
| `tie_order` | Int64 | Order within a tied rank (1 = first listed). |
| `team_side` | String | Side of the game the row belongs to: home or away. |
| `team_id` | String | B.LEAGUE TeamID as text (key space: B.LEAGUE clubs, stable across seasons AND club renames: 703 = Tochigi Brex in 2016-17 = Utsunomiya Brex in 2025-26). The feed sends a string on v2 and an integer on v1; both are text here. Not a player id. |
| `team_name_j` | String | Team name in Japanese (official name, e.g. アルバルク東京 = Alvark Tokyo). |
| `player_id` | String | B.LEAGUE PlayerID as text (key space: B.LEAGUE persons, stable across seasons and clubs: Makoto Hiejima = '8589'). A string on v2, an integer on v1; both are text here. Null where the row names no player. |
| `player_no` | String | Shirt number as text, so '00' and '0' stay distinct; null on team and game-level rows. |
| `player_name_j` | String | Player name in Japanese (katakana for foreign players); null on team and game-level rows. |
| `count` | Int64 | The player's value in the category for the period category. |
| `reb_off_count` | Int64 | Offensive-rebound count carried on the row (filled for the rebounds category). |
| `reb_def_count` | Int64 | Defensive-rebound count carried on the row (filled for the rebounds category). |

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
| `schedule_key` | Int64 | B.LEAGUE ScheduleKey (key space: B.LEAGUE games, ONE global integer sequence across B1, B2, B3 and every season). The {schedule_key} every game wrapper takes and the schedule page's list-item id. |
| `season` | Int64 | Season END year (2026 = 2025-26), the feed's Year (the START year) plus one. |
| `feed` | String | Source feed of the row: v2 (v2_genius_contexts, B1/B2 from 2020-21) or v1 (v1_contexts: B3, B1/B2 2018-2020, and the 2016-17 / 2017-18 game-page embed). The feeds differ in id types, dates and some codes. |
| `period` | Int64 | Period number: 1-4 regulation; overtime numbering is unverified (no overtime sample). |
| `event_index` | Int64 | Index of the scoring event, 0 = the 0-0 start row, then 1, 2... (the score track's x axis; NOT a clock). |
| `home_points` | Int64 | Home running total after the event. |
| `away_points` | Int64 | Away running total after the event. |

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
| `season` | Int64 | Season END year (2026 = 2025-26), the feed's Year (the START year) plus one. |
| `tab` | Int64 | Division tab: 1 = B1, 2 = B2, 3 = B3. |
| `season_label` | String | Season label of the selected option (2025-26). |
| `section` | String | Table heading in Japanese: 東地区 East, 中地区 Central, 西地区 West, ワイルドカード wild card. |
| `section_en` | String | English name of section for the four known headings; null for any other heading. |
| `rank` | Int64 | Rank in that table (順位); a club in a division table also appears in the wild-card table. |
| `team_id` | String | B.LEAGUE TeamID as text (key space: B.LEAGUE clubs, stable across seasons AND club renames: 703 = Tochigi Brex in 2016-17 = Utsunomiya Brex in 2025-26). The feed sends a string on v2 and an integer on v1; both are text here. Not a player id. |
| `team` | String | Club name in Japanese as the standings page prints it (千葉ジェッツ = Chiba Jets). |
| `team_short` | String | Club short name in Japanese (千葉J). |
| `wins` | Int64 | Games won (勝) so far this season in the table. |
| `losses` | Int64 | Games lost (負) so far this season in the table. |
| `win_pct` | Float64 | Win percentage (勝率) as a 0-1 fraction. |
| `games_behind` | Float64 | Games behind the table leader (差). |
| `points_for` | Int64 | Points scored (得点) over the season to date. |
| `points_against` | Int64 | Points allowed (失点) over the season to date. |
| `point_diff` | Int64 | Point differential (得失点差). |
| `home_record` | String | Home record W-L (ホーム). |
| `away_record` | String | Away record W-L (アウェー). |
| `last_n` | Int64 | Length of the recent-form window the page shows: 5 or 10 games (過去5試合 / 過去10試合). |
| `last_n_record` | String | Record W-L over the last_n games. |
| `streak` | String | Current streak (連勝/連敗) as printed: W5 = five straight wins, L1 = one loss. |
| `division_title` | String | Division-title mark (地区優勝) as printed; null when none ('--'). |

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
