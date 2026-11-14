# MLB — ESPN web API (v3)

> MLB — ESPN web API (v3) — endpoint reference in sdv-py, the SportsDataverse Python package.

`sportsdataverse.mlb` — 5 endpoints.

## espn_mlb_player_overview

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/baseball/mlb/athletes/{athlete_id}/overview`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/baseball/mlb/athletes/4239/overview](https://site.web.api.espn.com/apis/common/v3/sports/baseball/mlb/athletes/4239/overview)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_mlb_player_overview-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `athlete_id` | Int64 | Unique ESPN athlete identifier. |
| `athlete_display_name` | String | Athlete display name (full). |
| `athlete_short_name` | String | Athlete short display name. |
| `athlete_position` | String |  |
| `athlete_jersey` | String | Athlete jersey. |
| `athlete_team_id` | Int64 |  |
| `athlete_team_abbreviation` | String |  |
| `split_name` | String | Split name (typically "All Splits"). |
| `split_category` | String |  |
| `games_played` | String | Games played. |
| `avg_minutes` | String |  |
| `field_goal_pct` | String |  |
| `three_point_pct` | String |  |
| `free_throw_pct` | String |  |
| `avg_rebounds` | String |  |
| `avg_assists` | String |  |
| `avg_blocks` | String |  |
| `avg_steals` | String |  |
| `avg_fouls` | String |  |
| `avg_turnovers` | String |  |
| `avg_points` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_player_overview-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_player_overview-example}

```python
espn_mlb_player_overview(athlete_id='4239')
```

## espn_mlb_player_stats_v3

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/baseball/mlb/athletes/{athlete_id}/stats`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/baseball/mlb/athletes/39832/stats](https://site.web.api.espn.com/apis/common/v3/sports/baseball/mlb/athletes/39832/stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#espn_mlb_player_stats_v3-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `category` | String | ESPN stat category the row belongs to (e.g., basketball "averages" / "totals" / "miscellaneous", NFL "passing", MLB "career-batting"). The stat columns after `position` are named from each category's own `names` list, so they differ by sport, and the other categories' columns are null on a row. |
| `split_name` | String | Season label of the row (e.g., "2010-11") on the Web v3 payload, or "Career" on the row built from the category's career totals; the split's name on payloads that ship named splits. |
| `split_category` | String | Category of a named split; null on the Web v3 per-season rows. |
| `split_value` | String | Value of a named split; null on the Web v3 per-season rows. |
| `season` | Int64 | Season end year of the row (e.g., 2011 for 2010-11); null on the "Career" row. |
| `team_id` | Int64 | ESPN team id (Int64) the athlete played that season for; a traded player has one row per team. Null on the "Career" row. |
| `position` | String | Position abbreviation the athlete was listed at for that season and team (e.g., "G"); null on the "Career" row. |
| `games_played` | String | Games played (GP), as ESPN ships it (a string). |
| `at_bats` | String | At bats (AB), plate appearances minus walks, hit-by-pitches and sacrifices. |
| `runs` | String | Runs scored (R). |
| `hits` | String | Hits (H), singles through home runs. |
| `doubles` | String | Doubles (2B), two-base hits. |
| `triples` | String | Triples (3B), three-base hits. |
| `home_runs` | String | Home runs (HR). |
| `rb_is` | String | Runs batted in (RBI); ESPN's stat key `RBIs`, snake-cased. |
| `walks` | String | Walks (BB), bases on balls. |
| `hit_by_pitch` | String | Times hit by a pitch (HBP). |
| `strikeouts` | String | Strikeouts (SO). |
| `stolen_bases` | String | Stolen bases (SB). |
| `caught_stealing` | String | Times caught stealing (CS). |
| `avg` | String | Batting average (AVG), hits / at bats. |
| `on_base_pct` | String | On-base percentage (OBP), (hits + walks + hit-by-pitches) / plate appearances. |
| `slug_avg` | String | Slugging percentage (SLG), total bases / at bats. |
| `ops` | String | On-base plus slugging (OPS). |
| `warbr` | String | Wins above replacement (WAR); ESPN's stat key `WARBR`, snake-cased. |
| `plate_appearances` | String | Plate appearances (PA). |
| `pitches` | String | Pitches seen (P). |
| `pitches_per_plate_appearance` | String | Pitches seen per plate appearance (P/PA). |
| `extra_base_hits` | String | Extra-base hits (XBH), doubles plus triples plus home runs. |
| `total_bases` | String | Total bases (TB), 1 per single, 2 per double, 3 per triple, 4 per home run. |
| `intentional_walks` | String | Intentional walks (IBB). |
| `gid_ps` | String | Grounded into double play (GIDP); ESPN's stat key `GIDPs`, snake-cased. |
| `sac_hits` | String | Sacrifice bunts (SH). |
| `sac_flies` | String | Sacrifice flies (SF). |
| `stolen_base_pct` | String | Stolen-base success percentage (SB%). |
| `off_warbr` | String | Offensive wins above replacement (OWAR); ESPN's stat key `offWARBR`, snake-cased. |
| `runs_created` | String | Runs created (RC), an estimate of the runs a batter produced. |
| `runs_created_per27_outs` | String | Runs created per 27 outs (RC/27). |
| `isolated_power` | String | Isolated power (ISOP), slugging percentage minus batting average. |
| `secondary_avg` | String | Secondary average (SECA), extra bases from hits plus walks and stolen bases, per at bat. |
| `at_bats_per_home_run` | String | At bats per home run (AB/HR). |
| `walks_per_plate_appearance` | String | Walks per plate appearance (BB/PA). |
| `walk_to_strikeout_ratio` | String | Walks per strikeout (BB/K). |
| `ground_balls` | String | Ground-ball outs (GO). |
| `fly_balls` | String | Fly-ball outs (FO). |
| `ground_to_fly_ratio` | String | Ground outs per fly out (GO/FO). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_player_stats_v3-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_player_stats_v3-example}

```python
espn_mlb_player_stats_v3(athlete_id='39832')
```

## espn_mlb_player_gamelog

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/baseball/mlb/athletes/{athlete_id}/gamelog`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/baseball/mlb/athletes/4239/gamelog](https://site.web.api.espn.com/apis/common/v3/sports/baseball/mlb/athletes/4239/gamelog)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#espn_mlb_player_gamelog-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season_type_id` | Int64 |  |
| `season_type_name` | String |  |
| `category` | String | Category label. |
| `event_id` | Int64 | Unique ESPN event/game identifier. |
| `event_date` | String | Event date-time in ISO 8601 (e.g. '2017-07-11T00:00:00Z'). |
| `home_away` | String | Venue label for the team ('home' or 'away'). |
| `score` | String | Running score (away-home) after the play. |
| `opponent_id` | Int64 | Unique identifier for opponent. |
| `opponent_abbreviation` | String | Opponent abbreviation. |
| `opponent_display_name` | String | Display name. |
| `game_result` | String | Game result (W/L). |
| `game_processed` | String |  |
| `stat_0` | String |  |
| `stat_1` | String |  |
| `stat_2` | String |  |
| `stat_3` | String |  |
| `stat_4` | String |  |
| `stat_5` | String |  |
| `stat_6` | String |  |
| `stat_7` | String |  |
| `stat_8` | String |  |
| `stat_9` | String |  |
| `stat_10` | String |  |
| `stat_11` | String |  |
| `stat_12` | String |  |
| `stat_13` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_player_gamelog-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_player_gamelog-example}

```python
espn_mlb_player_gamelog(athlete_id='4239')
```

## espn_mlb_player_splits

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/baseball/mlb/athletes/{athlete_id}/splits`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/baseball/mlb/athletes/4239/splits](https://site.web.api.espn.com/apis/common/v3/sports/baseball/mlb/athletes/4239/splits)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#espn_mlb_player_splits-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `descriptions` | String |  |
| `display_name` | String | Display name. |
| `display_names` | String |  |
| `filters` | String |  |
| `labels` | String |  |
| `names` | String | Associated detail names for the type. |
| `split_categories` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_player_splits-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_player_splits-example}

```python
espn_mlb_player_splits(athlete_id='4239')
```

## espn_mlb_leaders

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/baseball/mlb/statistics/byathlete`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/baseball/mlb/statistics/byathlete?limit=50&page=1](https://site.web.api.espn.com/apis/common/v3/sports/baseball/mlb/statistics/byathlete?limit=50&page=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `category` | `category` |  |  | `Y` | Leaderboard population: the athletes ranked in that category, whose values it fills (other categories come back null). Football takes `<group>:<category>` (`offense:passing`, `offense:rushing`, `offense:receiving`, `defense:defensive`, `scoring:scoring`); a bare `passing` is rejected with HTTP 400 (ValueError). Basketball takes the bare name (`offensive`). |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `seasontype` | `season_type` |  |  | `Y` | Season phase: 1=preseason, 2=regular season, 3=postseason. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `sort` | `sort` |  |  | `Y` | Sort key `<category>.<statName>:<asc\|desc>`, the names in the payload's categories (`passing.passingYards:desc`, `offensive.avgPoints:desc`). A stat without its category is rejected with HTTP 400 (ValueError). |

### Returns {#espn_mlb_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `sort_order` | Int64 | Position of the athlete in ESPN's leaderboard order (the requested category or sort), counted across pages: (page - 1) * limit + position on the page. A position, not a rank: tied athletes still count up; ESPN's per-stat ranks stay in the raw payload (return_parsed=False). |
| `athlete_id` | Int64 | ESPN id of the athlete, the key the athlete endpoints (athlete_overview, athlete_stats) take. |
| `athlete_uid` | String | ESPN composite athlete uid (`s:<sport>~l:<league>~a:<athlete id>`), kept as text. |
| `athlete_display_name` | String | Athlete's full display name. |
| `athlete_short_name` | String | Athlete's abbreviated name (e.g. 'B. Nix'). |
| `athlete_first_name` | String | Athlete's first name. |
| `athlete_last_name` | String | Athlete's last name. |
| `athlete_position` | String | Athlete's position abbreviation (e.g. QB, G, C). |
| `athlete_status` | String | Athlete's ESPN status name (e.g. Active, Inactive) as ESPN reports it at request time, not for the requested season. |
| `team_id` | Int64 | ESPN id of the team the athlete played for in the requested season. |
| `team_abbreviation` | String | Abbreviation of the athlete's team in the requested season (ESPN teamShortName, e.g. ORE). |
| `team_name` | String | Nickname of the athlete's team in the requested season (ESPN teamName, e.g. Ducks). |
| `batting_games_played` | Float64 | Games Played (ESPN 'batting' category). |
| `batting_at_bats` | Float64 | At Bats (ESPN 'batting' category): The number of times a batter comes up to bat not counting walks and sacrifices. |
| `batting_runs` | Float64 | Runs (ESPN 'batting' category): The number of runs scored. |
| `batting_hits` | Float64 | Hits (ESPN 'batting' category): The number of hits. |
| `batting_avg` | Float64 | Batting Average (ESPN 'batting' category): The average number of times the batter reached successfully on base due to a hit: hits / atBats. |
| `batting_doubles` | Float64 | Doubles (ESPN 'batting' category): The number of times that a batter reaches second base as the result of a hit without an error being committed. |
| `batting_triples` | Float64 | Triples (ESPN 'batting' category): The number of hits that result in the batter reaching 3rd base without the aid of an error. |
| `batting_home_runs` | Float64 | Home Runs (ESPN 'batting' category): The number of hits that allowed the batter to round the bases and score without the aid of an error of the defense. |
| `batting_rb_is` | Float64 | Runs Batted In (ESPN 'batting' category): The number of runs scored by a batter's play. |
| `batting_total_bases` | Float64 | Total Bases (ESPN 'batting' category): The number of bases calculated as 1 for a 1B, 2 for a 2B, 3 for a 3B and 4 for a HR. |
| `batting_walks` | Float64 | Walks (ESPN 'batting' category): The number of times a batter reaches base on four balls. |
| `batting_strikeouts` | Float64 | Strikeouts (ESPN 'batting' category): The number of times a player strikes out. |
| `batting_stolen_bases` | Float64 | Stolen Bases (ESPN 'batting' category): The number of bases stolen. |
| `batting_on_base_pct` | Float64 | On Base Percentage (ESPN 'batting' category): The ratio of the number of times the batter reached base per plate appearnce: (hits + walks + hbp / plateAppearances). |
| `batting_slug_avg` | Float64 | Slugging Percentage (ESPN 'batting' category): Slugging percentage is a metric that is meant to measuring how many bases the hitter safely advances to after making contact with the ball: (totalBases / atBats). |
| `batting_ops` | Float64 | OBP Pct + SLG Pct (ESPN 'batting' category): The average calculated by adding up on-base percentage and slugging percentage. |
| `batting_warbr` | Float64 | Wins Above Replacement (ESPN 'batting' category). |
| `pitching_games_played` | Float64 | Games Played (ESPN 'pitching' category). |
| `pitching_games_started` | Float64 | Games Started (ESPN 'pitching' category): The number of games started by a pitcher. |
| `pitching_quality_starts` | Float64 | Quality Starts (ESPN 'pitching' category): The number of quality starts for a pitcher. |
| `pitching_era` | Float64 | Earned Run Average (ESPN 'pitching' category): The average number of runs a pitcher yields per 9 innings thrown. |
| `pitching_wins` | Float64 | Wins (ESPN 'pitching' category): The number of times the pitcher was attributed with a win. |
| `pitching_losses` | Float64 | Losses (ESPN 'pitching' category): The number of losses for a pitcher. |
| `pitching_saves` | Float64 | Saves (ESPN 'pitching' category): The number of times a pitcher enters and finishes the game without surrendering the lead in a save opportunity. |
| `pitching_holds` | Float64 | Holds (ESPN 'pitching' category): The number of times a pitcher enters the game with the lead, gets at least an out and leaves the game wihtout having given up the lead. |
| `pitching_innings` | Float64 | Innings pitched (ESPN 'pitching' category): The number of innings pitched. |
| `pitching_hits` | Float64 | Hits (ESPN 'pitching' category): The number of hits surrendered by a pitcher. |
| `pitching_earned_runs` | Float64 | Earned runs (ESPN 'pitching' category): The number of earned runs surrendered by a pitcher. |
| `pitching_home_runs` | Float64 | Home Runs (ESPN 'pitching' category): The number of times a pitcher has allowed a home run. |
| `pitching_walks` | Float64 | Walks (ESPN 'pitching' category): The number of times a pitcher surrenders a base on balls. |
| `pitching_strikeouts` | Float64 | Strikeouts (ESPN 'pitching' category): The number of times the pitcher struck out a batter. |
| `pitching_strikeouts_per_nine_innings` | Float64 | Strikeouts Per 9 Innings (ESPN 'pitching' category): The average number of strikeouts for a pitcher per 9 innings. |
| `pitching_whip` | Float64 | Walks Plus Hits Per Inning Pitched (ESPN 'pitching' category): The number representing walks plus hits divided by innings pitched. |
| `pitching_warbr` | Float64 | Wins Above Replacement (ESPN 'pitching' category). |
| `fielding_games_played` | Float64 | Games Played (ESPN 'fielding' category). |
| `fielding_games_started` | Float64 | Games Started (ESPN 'fielding' category): The number of games started by a fielder. |
| `fielding_full_innings_played` | Float64 | Full Innings (ESPN 'fielding' category): The number of fully completed innings by a fielder. |
| `fielding_total_chances` | Float64 | Total Chances (ESPN 'fielding' category): The number of chances a fielder has to make an out calculated by assists plus putouts plus errors. |
| `fielding_putouts` | Float64 | Putouts (ESPN 'fielding' category): The number of times the fielder tags or forces the out of a baserunner. |
| `fielding_assists` | Float64 | Assists (ESPN 'fielding' category): The number of times a fielder is involved in an out when the fielder is not the putouter. |
| `fielding_fielding_pct` | Float64 | Fielding Percentage (ESPN 'fielding' category): The percentage of plays made by a fielder given the total number of chances. |
| `fielding_errors` | Float64 | Errors (ESPN 'fielding' category): The number of times a fielder fails to make the play they are expected to make. |
| `fielding_double_plays` | Float64 | Double Plays (ESPN 'fielding' category): The number of double plays a fielder has been a part of as either the assister or putouter. |
| `fielding_range_factor` | Float64 | Range Factor (ESPN 'fielding' category): The estimation of how much field a fielder can cover. |
| `fielding_def_warbr` | Float64 | Defensive Wins Above Replacement (ESPN 'fielding' category). |
| `fielding_passed_balls` | Float64 | Passed Balls (ESPN 'fielding' category): The number of times the ball is dropped or passes by the catcher. |
| `fielding_catcher_stolen_bases_allowed` | Float64 | Stolen Bases Allowed (ESPN 'fielding' category): The number of stolen bases that a catcher allows. |
| `fielding_catcher_caught_stealing` | Float64 | Caught Stealing (ESPN 'fielding' category): The number of times a catcher throws a baserunner trying to steal a base. |
| `fielding_catcher_caught_stealing_pct` | Float64 | Caught Stealing Percentage (ESPN 'fielding' category): The percentage of baserunners caught by the catcher given the total number of steal attempts. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_leaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_leaders-example}

```python
espn_mlb_leaders()
```
