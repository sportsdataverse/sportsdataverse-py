---
title: "NBA — additional Python functions — Analytics: nba_raw–nba_tracking"
sidebar_label: "Analytics: nba_raw–nba_tracking"
sidebar_position: 14
description: "NBA — additional Python functions — Analytics: nba_raw–nba_tracking — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — Analytics: nba_raw–nba_tracking

### nba_raw_store_season_frame {#nba_raw_store_season_frame}

`nba_raw_store_season_frame(endpoint: 'str', season: 'int', variant: 'Optional[str]' = None, *, result_set: 'Optional[str]' = None, raw_store_dir: 'RawStoreDir' = None) -> "Optional['pl.DataFrame']"`

Read a committed SEASON-LEVEL capture from the raw store, parsed to a frame.

The per-game half of the store is served by the read-through per-game path;
this is the season-keyed half (`leaguegamelog`, `playerindex`,
`leaguedashplayerbiostats`, ...) that the `-raw` scraper writes as

* `{endpoint}/{season}/{variant}.json` -- parameterized captures, where
  *variant* is the slugified parameter sweep (e.g. `"regular-season"`,
  `"regular-season_totals"`), and
* `{endpoint}/{season}.json` -- unparameterized captures (e.g.
  `playerindex`), i.e. `variant=None`.

Roots may be a local checkout or an `http(s)://` base (the raw repo served
over raw.githubusercontent / a CDN), so a consumer -- notably the
`hoopR-nba-stats-data` model producer -- runs clone-free in CI against the
same committed tree the per-game compile reads.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `endpoint` | `str` |  | stats.nba.com endpoint slug (the store subdirectory). |
| `season` | `int` |  | Season **END** year (2024 = 2023-24) -- the key both halves of the store are filed under. |
| `variant` | `Optional[str]` | `None` | Capture variant slug, or `None` for an unparameterized capture. |
| `result_set` | `Optional[str]` | `None` | Named result set to return when the payload carries several; defaults to the first frame that parses. |
| `raw_store_dir` | `RawStoreDir` | `None` | Store root spec (dir or URL base) or per-endpoint mapping; `None` falls back to the env vars, `""` disables. |

**Returns**

The parsed `polars.DataFrame`, or `None` when the store is unset, the capture is absent, or the payload carries no usable frame -- so a caller can cleanly fall back to a live fetch.

| col_name | type | description |
|---|---|---|
| `season_id` | String | Unique season identifier. |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `game_id` | String | Unique game identifier. |
| `game_date` | String | Game date (YYYY-MM-DD). |
| `matchup` | String | Matchup. |
| `wl` | String | Wl. |
| `min` | Int64 | Minutes played. |
| `fgm` | Int64 | Field goals made. |
| `fga` | Int64 | Field goal attempts. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `fg3_m` | Int64 | Three-point field goals made. |
| `fg3_a` | Int64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 | Three-point field goal percentage (0-1). |
| `ftm` | Int64 | Free throws made. |
| `fta` | Int64 | Free throw attempts. |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `oreb` | Int64 | Offensive rebounds. |
| `dreb` | Int64 | Defensive rebounds. |
| `reb` | Int64 | Rebounds per game. |
| `ast` | Int64 | Assists. |
| `stl` | Int64 | Steals. |
| `blk` | Int64 | Blocks. |
| `tov` | Int64 | Turnovers. |
| `pf` | Int64 | Personal fouls. |
| `pts` | Int64 | Points scored. |
| `plus_minus` | Int64 | Plus/minus point differential while on court. |
| `video_available` | Int64 | Video available. |

**Example**

```python
from sportsdataverse.nba import nba_raw_store_season_frame
base = "https://raw.githubusercontent.com/sportsdataverse/hoopR-nba-stats-raw/main/nba_stats/json"
logs = nba_raw_store_season_frame("leaguegamelog", 2024, "regular-season", raw_store_dir=base)

# Fall back to a live fetch when the capture is absent

frame = nba_raw_store_season_frame("playerindex", 2024, raw_store_dir=base)
positions = frame if frame is not None else nba_stats_playerindex(season="2023-24")
```

### nba_referee_assignments {#nba_referee_assignments}

`nba_referee_assignments(date: 'str | _dt.date', *, league: 'str' = 'nba', raw: 'bool' = False, return_as_pandas: 'bool' = False, proxy: 'dict | None' = None) -> 'dict[str, Any]'`

Fetch and parse NBA referee assignments for a given date from official.nba.com.

Retrieves one league's referee crew assignments and replay-center officials for a
date: NBA, G-League or WNBA, picked by `league` (`raw=True` returns all three
leagues' blocks). The `crew_position` column (1–4) represents the feed's slot
order; slot 1 is inferred to be the crew chief. The `season` column converts
from the feed's format to an END year: START+1 for NBA/G-League
(two-calendar-year seasons) and START unchanged for WNBA.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `date` | `str \| date` |  | The date to fetch assignments for (str in "YYYY-MM-DD" format or datetime.date). |
| `league` | `str` | `'nba'` | The league to extract ("nba", "gl", or "wnba"). Defaults to "nba". |
| `raw` | `bool` | `False` | If True, return the raw JSON payload (dict) with all three leagues instead of parsed DataFrames. |
| `return_as_pandas` | `bool` | `False` | If True, return pandas DataFrames instead of polars. |
| `proxy` | `dict \| None` | `None` | Optional proxy dict passed through to the HTTP layer. |

**Returns**

If `raw=True`, the raw JSON dict with keys "nba", "gl", "wnba". Otherwise, a dict with keys `"officials"` and `"replay_center"` mapping to DataFrames as documented in `parse_nba_referee_assignments`.

| col_name | type | description |
|---|---|---|
| `officials.league` | String | League the assignment belongs to: nba, gl (G League), or wnba. |
| `officials.game_id` | String | 10-digit game id (zero-padded) for the assigned game. |
| `officials.game_date` | Date | Game date parsed from the feed's MM/DD/YYYY format. |
| `officials.season` | Int64 | Season end year, converted from the feed's <season-type digit><start year> code: start year + 1 for NBA/G League's two-calendar-year seasons, start year unchanged for WNBA's single-year seasons. |
| `officials.season_type` | String | Season type decoded from the feed's season code first digit: preseason, regular, all-star, playoffs, play-in, or nba-cup-final. |
| `officials.game_code` | String | League game code in YYYYMMDD/AWYHOM format, matching the away and home team abbreviations. |
| `officials.home_team_id` | Int64 | 10-digit team id of the home team. |
| `officials.home_team_abbr` | String | Three-letter abbreviation of the home team. |
| `officials.away_team_id` | Int64 | 10-digit team id of the away team. |
| `officials.away_team_abbr` | String | Three-letter abbreviation of the away team. |
| `officials.crew_position` | Int64 | Feed's official slot order (1-4); slot 1 is inferred to be the crew chief since the API does not label roles. |
| `officials.official_id` | Int64 | Numeric official id from the feed (source field official{n}_code); expected to match stats.nba.com's OFFICIAL_ID. |
| `officials.official_name` | String | Official's display name for this crew slot. |
| `officials.jersey_num` | String | Official's jersey number as a string, from the feed's official{n}_JNum field. |
| `replay_center.league` | String | League the replay-center staffing belongs to: nba, gl, or wnba. |
| `replay_center.game_date` | Date | Date the replay-center official worked; a date-level staffing record, not tied to one game. |
| `replay_center.official_id` | Int64 | Numeric replay-center official id from the feed. |
| `replay_center.official_name` | String | Replay-center official's display name for that date. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If league is not "nba", "gl", or "wnba", or date is not a valid "YYYY-MM-DD" date (checked before any request). |
| `NoDataError` | Official.nba.com answered 404, or 403 with S3's `AccessDenied` body. A date without games is not this: it comes back as a 200 with empty `rows` (see the Note). |
| `AssetFetchError` | The fetch failed (network error, rate limit, or Akamai WAF block), the 200 body is not a JSON object, or the league's `Table`/`Table1` rows are missing or malformed (a `Table` row without a 10-digit `game_id`, or a `Table1` row without a `replaycenter_official` name, included). With `raw=True` all three leagues are checked, since the whole payload is returned. |

**Example**

```python
from sportsdataverse.nba.nba_officiating import nba_referee_assignments
result = nba_referee_assignments("2026-06-13")
officials = result["officials"]
print(f"Found {officials.height} official slots")

# Fetch WNBA assignments for the same date

result = nba_referee_assignments("2026-06-13", league="wnba")
wnba_officials = result["officials"]
```

### nba_season_sim {#nba_season_sim}

`nba_season_sim(season: 'int', *, as_of_date: 'Union[dt.date, None]' = None, league_id: 'str' = '00', n_sims: 'int' = 10000, seed: 'int' = 20261103, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Season sim as of a date: projected W-L, division, seed, play-in, playoff and lottery odds.

Reads the `{nba,wnba}_stats_schedules` and `_possessions` releases, fits the game model on
the regular-season games played before `as_of_date` (`as_of_game_model`), and
simulates the rest (`simulate_pro_season`). The rules applied to each simulated season
are in `sportsdataverse.nba.nba_season_rules`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | The season, named by the year it ends (`2027` is 2026-27); the WNBA season is its calendar year. |
| `as_of_date` | `Union[date, None]` | `None` | Games on or after this date are simulated even when the release has their score (a backtest). None: every game with a score counts as played. |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA. |
| `n_sims` | `int` | `10000` | Simulated seasons (Monte Carlo error: see `SIM_SCHEMA`'s `_se` columns). |
| `seed` | `int` | `20261103` | Seeds the generator; the same release data and seed give the same output. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per team (stats.nba.com `team_id`, Int64), sorted by conference and mean seed: * `wins` / `losses` so far, `games_remaining`, and the as-of `adj_net_rtg` with its posterior sd `adj_net_rtg_sd`. * `proj_wins` / `proj_losses` (means), `proj_wins_sd`, the 10th and 90th percentiles, and `proj_wins_se` (the Monte Carlo error of the mean). * `p_division` (NBA), `p_seed_1` .. `p_seed_15` (the regular-season finish in the conference, NBA, or the league, WNBA) and `mean_seed`. * `p_top4` (first-round home court), `p_top6` (NBA, straight to the playoffs), `p_play_in` (NBA, seeds 7-10), `p_playoffs` (NBA: top 6 or through the play-in; WNBA: top 8) and its Monte Carlo `p_playoffs_se`. * `p_lottery`, `p_pick_1`, `p_drawn_pick` (a pick from the drawing: the NBA's top four, the WNBA's top two) and `mean_pick` (given a lottery finish). The pick columns are null when the league has no published chances for that many lottery teams. * `n_sims`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2026 is 2025-26); the WNBA season is its calendar year. |
| `as_of_date` | String | The as-of date: games on or after it are simulated, the ones before it are played. Null when every game with a score counts as played. |
| `team_id` | Int64 | stats.nba.com team id (Int64). |
| `conference` | String | NBA conference (East / West) from the 2004-05 alignment; "WNBA" for every WNBA team (one table). |
| `division` | String | NBA division (Atlantic, Central, Southeast, Northwest, Pacific, Southwest); "WNBA" for every WNBA team. |
| `wins` | Int64 | Regular-season wins in the games already played. |
| `losses` | Int64 | Regular-season losses in the games already played. |
| `games_remaining` | Int64 | Regular-season games on the schedule still to play (simulated). |
| `adj_net_rtg` | Float64 | As-of adjusted net rating from the possession ridge at the sim's penalty: points per 100 over an average opponent on a neutral court. |
| `adj_net_rtg_sd` | Float64 | Posterior sd of adj_net_rtg (points per 100) from the ridge: the per-season rating shift each simulated season draws. |
| `proj_wins` | Float64 | Mean final regular-season wins over the simulated seasons (played wins plus simulated ones). |
| `proj_losses` | Float64 | Mean final regular-season losses over the simulated seasons. |
| `proj_wins_sd` | Float64 | Standard deviation of the final wins over the simulated seasons. |
| `proj_wins_p10` | Float64 | 10th percentile of the final wins over the simulated seasons. |
| `proj_wins_p90` | Float64 | 90th percentile of the final wins over the simulated seasons. |
| `proj_wins_se` | Float64 | Monte Carlo standard error of proj_wins: proj_wins_sd / sqrt(n_sims). |
| `p_division` | Float64 | Share of simulated seasons in which the team wins its division under the NBA tiebreakers; null for the WNBA. |
| `p_seed_1` | Float64 | Share of simulated seasons in which the team finishes the regular season seeded 1: in its conference for the NBA, in the league for the WNBA (before the NBA play-in). 0 for a seed past the number of teams. |
| `p_seed_2` | Float64 | Share of simulated seasons in which the team finishes the regular season seeded 2: in its conference for the NBA, in the league for the WNBA (before the NBA play-in). 0 for a seed past the number of teams. |
| `p_seed_3` | Float64 | Share of simulated seasons in which the team finishes the regular season seeded 3: in its conference for the NBA, in the league for the WNBA (before the NBA play-in). 0 for a seed past the number of teams. |
| `p_seed_4` | Float64 | Share of simulated seasons in which the team finishes the regular season seeded 4: in its conference for the NBA, in the league for the WNBA (before the NBA play-in). 0 for a seed past the number of teams. |
| `p_seed_5` | Float64 | Share of simulated seasons in which the team finishes the regular season seeded 5: in its conference for the NBA, in the league for the WNBA (before the NBA play-in). 0 for a seed past the number of teams. |
| `p_seed_6` | Float64 | Share of simulated seasons in which the team finishes the regular season seeded 6: in its conference for the NBA, in the league for the WNBA (before the NBA play-in). 0 for a seed past the number of teams. |
| `p_seed_7` | Float64 | Share of simulated seasons in which the team finishes the regular season seeded 7: in its conference for the NBA, in the league for the WNBA (before the NBA play-in). 0 for a seed past the number of teams. |
| `p_seed_8` | Float64 | Share of simulated seasons in which the team finishes the regular season seeded 8: in its conference for the NBA, in the league for the WNBA (before the NBA play-in). 0 for a seed past the number of teams. |
| `p_seed_9` | Float64 | Share of simulated seasons in which the team finishes the regular season seeded 9: in its conference for the NBA, in the league for the WNBA (before the NBA play-in). 0 for a seed past the number of teams. |
| `p_seed_10` | Float64 | Share of simulated seasons in which the team finishes the regular season seeded 10: in its conference for the NBA, in the league for the WNBA (before the NBA play-in). 0 for a seed past the number of teams. |
| `p_seed_11` | Float64 | Share of simulated seasons in which the team finishes the regular season seeded 11: in its conference for the NBA, in the league for the WNBA (before the NBA play-in). 0 for a seed past the number of teams. |
| `p_seed_12` | Float64 | Share of simulated seasons in which the team finishes the regular season seeded 12: in its conference for the NBA, in the league for the WNBA (before the NBA play-in). 0 for a seed past the number of teams. |
| `p_seed_13` | Float64 | Share of simulated seasons in which the team finishes the regular season seeded 13: in its conference for the NBA, in the league for the WNBA (before the NBA play-in). 0 for a seed past the number of teams. |
| `p_seed_14` | Float64 | Share of simulated seasons in which the team finishes the regular season seeded 14: in its conference for the NBA, in the league for the WNBA (before the NBA play-in). 0 for a seed past the number of teams. |
| `p_seed_15` | Float64 | Share of simulated seasons in which the team finishes the regular season seeded 15: in its conference for the NBA, in the league for the WNBA (before the NBA play-in). 0 for a seed past the number of teams. |
| `mean_seed` | Float64 | Mean regular-season seed over the simulated seasons (1 = best). |
| `p_top4` | Float64 | Share of simulated seasons with a top-4 seed: first-round home court. |
| `p_top6` | Float64 | NBA: share of simulated seasons with a top-6 conference seed, straight to the playoffs; null for the WNBA. |
| `p_play_in` | Float64 | NBA: share of simulated seasons with a conference seed of 7 to 10, the play-in; null for the WNBA. |
| `p_playoffs` | Float64 | Share of simulated seasons in which the team makes the playoffs: NBA top 6 or through the simulated play-in; WNBA top 8. |
| `p_playoffs_se` | Float64 | Monte Carlo standard error of p_playoffs: sqrt(p (1 - p) / n_sims). |
| `p_lottery` | Float64 | Share of simulated seasons in which the team misses the playoffs and enters the draft lottery. |
| `p_pick_1` | Float64 | Share of simulated seasons in which the team's own lottery slot draws the first pick; null when the league has no published chances for that many lottery teams. |
| `p_drawn_pick` | Float64 | Share of simulated seasons in which the team's slot gets a drawn pick (NBA top 4, WNBA top 2); null as p_pick_1. |
| `mean_pick` | Float64 | Mean draft pick over the simulated seasons in which the team is in the lottery; null when it never is, or as p_pick_1. |
| `n_sims` | Int64 | Number of simulated seasons behind every share on the row. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not `"00"` or `"10"`. |

**Example**

```python
import datetime as dt
from sportsdataverse.nba.nba_season_sim import nba_season_sim
sim = nba_season_sim(2026, as_of_date=dt.date(2026, 2, 13))
sim.select("team_id", "wins", "proj_wins", "p_playoffs", "p_pick_1").head()

# WNBA via ``league_id``

nba_season_sim(2025, as_of_date=dt.date(2025, 7, 1), league_id="10")
```

### nba_season_standings {#nba_season_standings}

`nba_season_standings(games: 'pl.DataFrame', *, league_id: 'str' = '00', seed: 'int' = 0, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Standings with the league's tiebreakers from completed regular-season games.

NBA: conference seeds and division ranks under the NBA tie-break procedure (division leaders
first, postseason-eligible = a top-10 conference record). WNBA: one league table under the
WNBA procedure. The rules and their sources are in `sportsdataverse.nba.nba_season_rules`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games` | `DataFrame` |  | One row per game with `season`, `home_team_id`, `away_team_id` (Int64 stats.nba.com ids), `home_pts` and `away_pts`; rows without both scores are not played yet and are skipped. A `load_nba_stats_schedules` frame can be passed as is: with a `season_type` column only the `"regular-season"` rows count (the NBA Cup final, preseason, play-in and playoffs do not). |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA. |
| `seed` | `int` | `0` | Seeds the drawing that ends a tie no criterion breaks. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, team_id)` that played: `conference` / `division` (NBA; the WNBA rows say `"WNBA"`), `games`, `wins`, `losses`, `win_pct`, `point_diff`, `division_rank` (1 = the division winner), `division_leader`, `seed` (rank in the conference for the NBA, in the league for the WNBA, 1 = best) and `games_back` (behind the seed-1 team). Sorted by season, conference and seed.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2026 is 2025-26); the WNBA season is its calendar year. |
| `team_id` | Int64 | stats.nba.com team id (Int64). |
| `conference` | String | NBA conference (East / West); "WNBA" for every WNBA team. |
| `division` | String | NBA division; "WNBA" for every WNBA team. |
| `games` | Int64 | Regular-season games played. |
| `wins` | Int64 | Regular-season wins. |
| `losses` | Int64 | Regular-season losses. |
| `win_pct` | Float64 | Regular-season winning percentage, wins divided by games played; the first key of the seeding. |
| `point_diff` | Int64 | Points scored minus points allowed over the regular-season games. |
| `division_rank` | Int64 | Rank within the division under the NBA tiebreakers, 1 = the division winner (the WNBA rows rank the league). |
| `division_leader` | Boolean | True for the NBA division winner after the division tiebreak; False for every WNBA row. |
| `seed` | Int64 | Seed under the league's tiebreakers: rank in the conference (NBA) or the league (WNBA), 1 = best. |
| `games_back` | Float64 | Games behind the seed-1 team of the conference (NBA) or league (WNBA): ((leader wins - wins) + (losses - leader losses)) / 2. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not `"00"` or `"10"`, or an NBA team id is unknown. |
| `TypeError` | If the team-id columns are not Int64. |

**Example**

```python
from sportsdataverse.nba.nba_loaders import load_nba_stats_schedules
from sportsdataverse.nba.nba_season_rules import nba_season_standings
nba_season_standings(load_nba_stats_schedules(2025)).filter(pl.col("seed") <= 10)
```

### nba_shot_value {#nba_shot_value}

`nba_shot_value(player_ids: "'list[int]'", season: 'str', *, league_id: 'str' = '00', include_context: 'bool' = False, return_as_pandas: 'bool' = False) -> "'dict[str, Union[pl.DataFrame, pd.DataFrame]]'"`

One-call shot-value spine: fetch, score, and run all five models.

Fetches each player's `shotchartdetail`, scores per-shot expected points
from the free `LeagueAverages` zone table, and returns the scored shots
plus shooter talent, selection quality, and zone-value maps (and the
defender/shot-clock context tables when `include_context=True`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_ids` | `list[int]` |  | Player ids to fetch. |
| `season` | `str` |  | Season string, e.g. `"2022-23"`. |
| `league_id` | `str` | `'00'` | `"00"` NBA, `"10"` WNBA, `"20"` G-League. |
| `include_context` | `bool` | `False` | Also fetch + return the `playerdashptshots` defender/shot-clock context tables, once per player and team his fetched shots came from. |
| `return_as_pandas` | `bool` | `False` | Return pandas frames instead of polars. |

**Returns**

`{"shots", "talent", "selection", "zones"}` (plus `"context"` when requested). An empty fetch returns a dict of zero-row frames.

**Example**

```python
from sportsdataverse.nba import nba_shot_value
out = nba_shot_value([201939], "2022-23")
out["talent"].head()
```

### nba_shot_value_lineups {#nba_shot_value_lineups}

`nba_shot_value_lineups(group_id: 'str', season: 'str', *, team_id: 'int', league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Scored per-shot frame for one 5-man lineup (`shotchartlineupdetail`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `group_id` | `str` |  | The 5-man lineup group id (dash-joined player ids); kept `Utf8`. |
| `season` | `str` |  | Season string, e.g. `"2022-23"`. |
| `team_id` | `int` |  | The lineup's team id. |
| `league_id` | `str` | `'00'` | `"00"` NBA, `"10"` WNBA, `"20"` G-League. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

The lineup's shots scored by `score_shot_xpoints` (with `xpoints`). Empty fetch returns the augmented zero-row schema.

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.nba import nba_shot_value_lineups
df = nba_shot_value_lineups("201939-202691-...", "2022-23", team_id=1610612744)
```

### nba_team_clutch {#nba_team_clutch}

`nba_team_clutch(season: 'int', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Opponent-agnostic clutch skill (shrunk clutch net-rating delta) per team.

Loads the season's clutch net rating (`nba_stats_leaguedashteamclutch`)
and full-game net baseline (`nba_stats_leaguedashteamstats`), computes
`clutch_delta`, and applies `shrink_clutch`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | End year of the season (e.g. `2024` for 2023-24). |
| `league_id` | `str` | `'00'` | `"00"` NBA / `"10"` WNBA / `"20"` G-League. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per team: `season, team_id, clutch_net_rating, adj_net_rtg, clutch_delta, clutch_skill_shrunk, clutch_poss`. Empty input returns that schema with zero rows.

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.nba.nba_clutch import nba_team_clutch
skill = nba_team_clutch(2024)
skill.sort("clutch_skill_shrunk", descending=True).head()
```

### nba_team_season_rates {#nba_team_season_rates}

`nba_team_season_rates(seasons: 'Union[int, list[int]]', *, league_id: 'str' = '00', season_type: 'str' = 'regular-season', return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Season team rates from possessions: ORtg/DRtg/net, pace and the four factors, with splits.

One row per team, season and split (`all`, `home`, `away`, `close`, `clutch`).
Clutch is a possession that starts with 5:00 or less left in the 4th quarter or an overtime,
the score within 5 (stats.nba.com's clutch window); a close game is one with a clutch
possession. The full definitions are in `sportsdataverse._common.team_season_rates`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, list[int]]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26). The WNBA season is its calendar year. |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA. |
| `season_type` | `str` | `'regular-season'` | `"regular-season"` (default), `"playoffs"` or `"play-in"`, matched to the schedule's `season_type`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, team_id, split)`. `team_id` is the stats.nba.com team id (Int64). `games`, `poss` / `opp_poss` (offensive / defensive possessions) and `pts` / `opp_pts` are the split's totals. The twelve rates are `off_rtg`, `def_rtg`, `net_rtg` (per 100 possessions), `pace` (possessions per 48 minutes, 40 in the WNBA), the four factors `efg_pct`, `tov_pct`, `orb_pct`, `ft_rate` and the same four against (`opp_*`), all fractions. Each rate has `<m>_rank` (1 = best; `def_rtg`, `tov_pct`, `opp_efg_pct`, `opp_orb_pct` and `opp_ft_rate` rank low first; pace ranks high first), `<m>_pct` (the Weibull percentile) and `<m>_n` (the sample: possessions for the ratings, games for pace, field-goal attempts for eFG% and FT rate, possessions for turnover rate, rebound chances for ORB%). Ranks are within the season and split, among teams with at least half the most games, and null under five teams. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2026 is 2025-26). |
| `team_id` | Int64 | stats.nba.com team id (the 10-digit franchise id). |
| `split` | String | Which possessions the row covers: all; home or away (the team's home or road games; neutral-site games are in neither); close (every possession of a game that had a clutch possession); clutch (a possession that starts with 5:00 or less left in the 4th quarter or an overtime, the score within 5 as it starts). |
| `games` | Int64 | Games with at least one possession in the split. |
| `poss` | Int64 | Offensive possessions in the split, counted from possession rows. |
| `opp_poss` | Int64 | Defensive possessions in the split (the opponents' offensive possessions). |
| `pts` | Int64 | Points scored on the team's offensive possessions in the split. Counted from each possession's made shots and free throws. |
| `opp_pts` | Int64 | Points allowed on the team's defensive possessions in the split. |
| `off_rtg` | Float64 | Offensive rating: points scored per 100 offensive possessions. |
| `def_rtg` | Float64 | Defensive rating: points allowed per 100 defensive possessions. |
| `net_rtg` | Float64 | Net rating: off_rtg minus def_rtg. |
| `pace` | Float64 | Pace: possessions per 48 minutes, the mean of poss and opp_poss over the game clock those possessions used. |
| `efg_pct` | Float64 | Effective field-goal percentage, (FGM + 0.5 * 3PM) / FGA, as a fraction. |
| `tov_pct` | Float64 | Turnovers per offensive possession, as a fraction. |
| `orb_pct` | Float64 | Offensive rebound rate: offensive rebounds over offensive-rebound chances (own OREB plus the defense's DREB), as a fraction. |
| `ft_rate` | Float64 | Free-throw rate, FTA / FGA. |
| `opp_efg_pct` | Float64 | Opponents' effective field-goal percentage, (FGM + 0.5 * 3PM) / FGA, as a fraction. |
| `opp_tov_pct` | Float64 | Opponents' turnovers per possession (turnovers forced), as a fraction. |
| `opp_orb_pct` | Float64 | Opponents' offensive rebound rate, as a fraction (one minus the team's defensive rebound rate). |
| `opp_ft_rate` | Float64 | Opponents' free-throw rate, FTA / FGA. |
| `off_rtg_rank` | Float64 | Rank of off_rtg within the season and split (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the split; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `def_rtg_rank` | Float64 | Rank of def_rtg within the season and split (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the split; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `net_rtg_rank` | Float64 | Rank of net_rtg within the season and split (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the split; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `pace_rank` | Float64 | Rank of pace within the season and split (the highest value ranks 1; pace has no better direction; ties average), among teams that played at least half the most games any team played in the split; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `efg_pct_rank` | Float64 | Rank of efg_pct within the season and split (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the split; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `tov_pct_rank` | Float64 | Rank of tov_pct within the season and split (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the split; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `orb_pct_rank` | Float64 | Rank of orb_pct within the season and split (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the split; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `ft_rate_rank` | Float64 | Rank of ft_rate within the season and split (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the split; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `opp_efg_pct_rank` | Float64 | Rank of opp_efg_pct within the season and split (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the split; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `opp_tov_pct_rank` | Float64 | Rank of opp_tov_pct within the season and split (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the split; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `opp_orb_pct_rank` | Float64 | Rank of opp_orb_pct within the season and split (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the split; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `opp_ft_rate_rank` | Float64 | Rank of opp_ft_rate within the season and split (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the split; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `off_rtg_pct` | Float64 | Percentile of off_rtg within the season and split among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `def_rtg_pct` | Float64 | Percentile of def_rtg within the season and split among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `net_rtg_pct` | Float64 | Percentile of net_rtg within the season and split among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `pace_pct` | Float64 | Percentile of pace within the season and split among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `efg_pct_pct` | Float64 | Percentile of efg_pct within the season and split among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `tov_pct_pct` | Float64 | Percentile of tov_pct within the season and split among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `orb_pct_pct` | Float64 | Percentile of orb_pct within the season and split among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `ft_rate_pct` | Float64 | Percentile of ft_rate within the season and split among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `opp_efg_pct_pct` | Float64 | Percentile of opp_efg_pct within the season and split among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `opp_tov_pct_pct` | Float64 | Percentile of opp_tov_pct within the season and split among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `opp_orb_pct_pct` | Float64 | Percentile of opp_orb_pct within the season and split among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `opp_ft_rate_pct` | Float64 | Percentile of opp_ft_rate within the season and split among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `off_rtg_n` | Int64 | Sample size of off_rtg: the team's offensive possessions in the split. |
| `def_rtg_n` | Int64 | Sample size of def_rtg: the team's defensive possessions in the split. |
| `net_rtg_n` | Int64 | Sample size of net_rtg: the team's offensive plus defensive possessions in the split. |
| `pace_n` | Int64 | Sample size of pace: games in the split. |
| `efg_pct_n` | Int64 | Sample size of efg_pct: the team's field-goal attempts in the split. |
| `tov_pct_n` | Int64 | Sample size of tov_pct: the team's offensive possessions in the split. |
| `orb_pct_n` | Int64 | Sample size of orb_pct: the team's offensive-rebound chances in the split. |
| `ft_rate_n` | Int64 | Sample size of ft_rate: the team's field-goal attempts in the split. |
| `opp_efg_pct_n` | Int64 | Sample size of opp_efg_pct: the opponents' field-goal attempts in the split. |
| `opp_tov_pct_n` | Int64 | Sample size of opp_tov_pct: the team's defensive possessions in the split. |
| `opp_orb_pct_n` | Int64 | Sample size of opp_orb_pct: the opponents' offensive-rebound chances in the split. |
| `opp_ft_rate_n` | Int64 | Sample size of opp_ft_rate: the opponents' field-goal attempts in the split. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not `"00"` or `"10"`. |

**Example**

```python
from sportsdataverse.nba.nba_team_season_rates import nba_team_season_rates
rates = nba_team_season_rates(2026)
rates.filter(split="all").sort("net_rtg_rank").head()

# WNBA via ``league_id``

wnba = nba_team_season_rates(2025, league_id="10")
```

### nba_team_strength {#nba_team_strength}

`nba_team_strength(seasons: 'Union[int, list[int]]', *, league_id: 'str' = '00', season_type: 'str' = 'regular-season', return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Team strength from possessions: adjusted ratings, SOS, SOV, pace and the four factors.

One row per team and season. The adjusted ratings are a ridge regression of points per 100
on each possession on offense, defense and home terms (penalty
`~sportsdataverse._common.team_strength.RIDGE_LAMBDA`, fit by season-disjoint CV):
points scored / allowed per 100 against an average opponent on a neutral court. SOS is the
mean opponent `adj_net_rtg` over the team's games, SOV the same over its wins.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, list[int]]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26). The WNBA season is its calendar year. |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA. |
| `season_type` | `str` | `'regular-season'` | `"regular-season"` (default), `"playoffs"` or `"play-in"`, matched to the schedule's `season_type`. The ridge, the rates and the games all use it. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, team_id)`; `team_id` is the stats.nba.com team id (Int64). `games`, `poss` / `opp_poss`, `pts` / `opp_pts` are the season totals from the possessions; `wins` / `losses` come from the schedule's final scores. The rated metrics are `adj_off_rtg`, `adj_def_rtg`, `adj_net_rtg`, `sos`, `sov` and the BB-D3 rates (`off_rtg`, `def_rtg`, `net_rtg`, `pace`, the four factors and the four against). Each has `<m>_rank` (1 = best; `adj_def_rtg`, `def_rtg`, `tov_pct` and the opponent shooting, rebounding and free-throw rates rank low first; `sos` ranks the hardest schedule first), `<m>_pct` (the Weibull percentile) and `<m>_n` (possessions for the ratings, games for SOS and pace, wins for SOV). Ranks are within the season, among teams with at least half the most games, and null under five teams. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2026 is 2025-26). |
| `team_id` | Int64 | stats.nba.com team id (the 10-digit franchise id). |
| `games` | Int64 | Games with at least one possession in the season (from the possession release). |
| `wins` | Int64 | Wins in the season type's completed games (the schedule's final scores). |
| `losses` | Int64 | Losses in the season type's completed games. |
| `poss` | Int64 | Offensive possessions in the season, counted from possession rows. |
| `opp_poss` | Int64 | Defensive possessions in the season (the opponents' offensive possessions). |
| `pts` | Int64 | Points scored on the team's offensive possessions in the season. Counted from each possession's made shots and free throws. |
| `opp_pts` | Int64 | Points allowed on the team's defensive possessions in the season. |
| `adj_off_rtg` | Float64 | Adjusted offensive rating: points per 100 possessions against an average defense on a neutral court. The ridge intercept plus the team's offense coefficient (points per 100 on each possession on offense, defense and home terms; penalty RIDGE_LAMBDA). |
| `adj_def_rtg` | Float64 | Adjusted defensive rating: points allowed per 100 possessions against an average offense on a neutral court. The ridge intercept plus the team's defense coefficient; lower is better. |
| `adj_net_rtg` | Float64 | Adjusted net rating: adj_off_rtg minus adj_def_rtg. |
| `sos` | Float64 | Strength of schedule: the mean adj_net_rtg of the opponents of the team's completed games, one weight per game (an opponent met four times counts four times). |
| `sov` | Float64 | Strength of victory: the mean adj_net_rtg of the opponents the team beat, one weight per win; null with no win. |
| `off_rtg` | Float64 | Offensive rating: points scored per 100 offensive possessions. |
| `def_rtg` | Float64 | Defensive rating: points allowed per 100 defensive possessions. |
| `net_rtg` | Float64 | Net rating: off_rtg minus def_rtg. |
| `pace` | Float64 | Pace: possessions per 48 minutes, the mean of poss and opp_poss over the game clock those possessions used. |
| `efg_pct` | Float64 | Effective field-goal percentage, (FGM + 0.5 * 3PM) / FGA, as a fraction. |
| `tov_pct` | Float64 | Turnovers per offensive possession, as a fraction. |
| `orb_pct` | Float64 | Offensive rebound rate: offensive rebounds over offensive-rebound chances (own OREB plus the defense's DREB), as a fraction. |
| `ft_rate` | Float64 | Free-throw rate, FTA / FGA. |
| `opp_efg_pct` | Float64 | Opponents' effective field-goal percentage, (FGM + 0.5 * 3PM) / FGA, as a fraction. |
| `opp_tov_pct` | Float64 | Opponents' turnovers per possession (turnovers forced), as a fraction. |
| `opp_orb_pct` | Float64 | Opponents' offensive rebound rate, as a fraction (one minus the team's defensive rebound rate). |
| `opp_ft_rate` | Float64 | Opponents' free-throw rate, FTA / FGA. |
| `adj_off_rtg_rank` | Float64 | Rank of adj_off_rtg within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `adj_def_rtg_rank` | Float64 | Rank of adj_def_rtg within the season (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `adj_net_rtg_rank` | Float64 | Rank of adj_net_rtg within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `sos_rank` | Float64 | Rank of sos within the season (the highest value ranks 1; the hardest schedule ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `sov_rank` | Float64 | Rank of sov within the season (the highest value ranks 1; the strongest wins rank 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `off_rtg_rank` | Float64 | Rank of off_rtg within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `def_rtg_rank` | Float64 | Rank of def_rtg within the season (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `net_rtg_rank` | Float64 | Rank of net_rtg within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `pace_rank` | Float64 | Rank of pace within the season (the highest value ranks 1; pace has no better direction; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `efg_pct_rank` | Float64 | Rank of efg_pct within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `tov_pct_rank` | Float64 | Rank of tov_pct within the season (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `orb_pct_rank` | Float64 | Rank of orb_pct within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `ft_rate_rank` | Float64 | Rank of ft_rate within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `opp_efg_pct_rank` | Float64 | Rank of opp_efg_pct within the season (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `opp_tov_pct_rank` | Float64 | Rank of opp_tov_pct within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `opp_orb_pct_rank` | Float64 | Rank of opp_orb_pct within the season (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `opp_ft_rate_rank` | Float64 | Rank of opp_ft_rate within the season (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `adj_off_rtg_pct` | Float64 | Percentile of adj_off_rtg within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `adj_def_rtg_pct` | Float64 | Percentile of adj_def_rtg within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `adj_net_rtg_pct` | Float64 | Percentile of adj_net_rtg within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `sos_pct` | Float64 | Percentile of sos within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `sov_pct` | Float64 | Percentile of sov within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `off_rtg_pct` | Float64 | Percentile of off_rtg within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `def_rtg_pct` | Float64 | Percentile of def_rtg within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `net_rtg_pct` | Float64 | Percentile of net_rtg within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `pace_pct` | Float64 | Percentile of pace within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `efg_pct_pct` | Float64 | Percentile of efg_pct within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `tov_pct_pct` | Float64 | Percentile of tov_pct within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `orb_pct_pct` | Float64 | Percentile of orb_pct within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `ft_rate_pct` | Float64 | Percentile of ft_rate within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `opp_efg_pct_pct` | Float64 | Percentile of opp_efg_pct within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `opp_tov_pct_pct` | Float64 | Percentile of opp_tov_pct within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `opp_orb_pct_pct` | Float64 | Percentile of opp_orb_pct within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `opp_ft_rate_pct` | Float64 | Percentile of opp_ft_rate within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `adj_off_rtg_n` | Int64 | Sample size of adj_off_rtg: the team's offensive possessions in the season. |
| `adj_def_rtg_n` | Int64 | Sample size of adj_def_rtg: the team's defensive possessions in the season. |
| `adj_net_rtg_n` | Int64 | Sample size of adj_net_rtg: the team's offensive plus defensive possessions in the season. |
| `sos_n` | Int64 | Sample size of sos: the completed games against a rated opponent. |
| `sov_n` | Int64 | Sample size of sov: the wins against a rated opponent. |
| `off_rtg_n` | Int64 | Sample size of off_rtg: the team's offensive possessions in the season. |
| `def_rtg_n` | Int64 | Sample size of def_rtg: the team's defensive possessions in the season. |
| `net_rtg_n` | Int64 | Sample size of net_rtg: the team's offensive plus defensive possessions in the season. |
| `pace_n` | Int64 | Sample size of pace: games in the season. |
| `efg_pct_n` | Int64 | Sample size of efg_pct: the team's field-goal attempts in the season. |
| `tov_pct_n` | Int64 | Sample size of tov_pct: the team's offensive possessions in the season. |
| `orb_pct_n` | Int64 | Sample size of orb_pct: the team's offensive-rebound chances in the season. |
| `ft_rate_n` | Int64 | Sample size of ft_rate: the team's field-goal attempts in the season. |
| `opp_efg_pct_n` | Int64 | Sample size of opp_efg_pct: the opponents' field-goal attempts in the season. |
| `opp_tov_pct_n` | Int64 | Sample size of opp_tov_pct: the team's defensive possessions in the season. |
| `opp_orb_pct_n` | Int64 | Sample size of opp_orb_pct: the opponents' offensive-rebound chances in the season. |
| `opp_ft_rate_n` | Int64 | Sample size of opp_ft_rate: the opponents' field-goal attempts in the season. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not `"00"` or `"10"`. |

**Example**

```python
from sportsdataverse.nba.nba_team_strength import nba_team_strength
strength = nba_team_strength(2026)
strength.sort("adj_net_rtg_rank").select("team_id", "adj_net_rtg", "sos", "sov").head()

# WNBA via ``league_id``

wnba = nba_team_strength(2025, league_id="10")
```

### nba_tracking_drive_value {#nba_tracking_drive_value}

`nba_tracking_drive_value(seasons: "'int | str | list'", *, league_id: 'str' = '00', per_mode: 'str' = 'Totals', by_position: 'bool' = True, positions: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False, _get_fn: 'Optional[Callable[..., dict]]' = None) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Drive value over expected + rim-pressure, per player-season.

Fetches the `Drives` `leaguedashptstats` measure and computes
`drive_pts_oe = drive_pts - drives * bucket_pts_per_drive`. `rim_pressure`
is the z-score of `drive_fta / drives` within the player's role bucket
(a proxy for foul-drawing pressure independent of scoring efficiency).
`drive_ast`/`drive_tov` are passed through unchanged.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| str \| list` |  | A single season or list of seasons. |
| `league_id` | `str` | `'00'` | `"00"` NBA (default), `"10"` WNBA, `"20"` G-League. |
| `per_mode` | `str` | `'Totals'` | `per_mode_simple` passed to the fetch (default `"Totals"`). |
| `by_position` | `bool` | `True` | Compute the baseline within role buckets (default); `False` forces one league-wide bucket. |
| `positions` | `Optional[DataFrame]` | `None` | Optional pre-fetched positions frame. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |
| `_get_fn` | `Optional[Callable[..., dict]]` | `None` | Injectable replacement for `nba_stats_leaguedashptstats`. |

**Returns**

One row per player-season: `season:Int64, player_id:Utf8, player_name:Utf8, team_id:Utf8, position_bucket:Utf8, gp:Int64, min:Float64, drives:Float64, drive_pts:Float64, drive_baseline_rate:Float64, drive_expected:Float64, drive_pts_oe:Float64, drive_pts_oe_per_36:Float64, drive_fta:Float64, rim_pressure:Float64, drive_ast:Float64, drive_tov:Float64, league_id:Utf8`. Empty/malformed input returns a zero-row frame with this schema.

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.nba import nba_tracking_drive_value
df = nba_tracking_drive_value(2024)
print(df.sort("drive_pts_oe", descending=True).head())
```

### nba_tracking_pass_value {#nba_tracking_pass_value}

`nba_tracking_pass_value(seasons: "'int | str | list'", *, league_id: 'str' = '00', per_mode: 'str' = 'Totals', by_position: 'bool' = True, positions: 'Optional[pl.DataFrame]' = None, fetch_potential_assists: 'bool' = False, max_players: 'int' = 0, return_as_pandas: 'bool' = False, _get_fn: 'Optional[Callable[..., dict]]' = None, _pass_get_fn: 'Optional[Callable[..., dict]]' = None) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Expected-assists / passer value: `ast_oe` per player-season.

Fetches the `Passing` `leaguedashptstats` measure (one call) and computes
`ast_oe = ast - passes * bucket_assist_rate`. When
`fetch_potential_assists=True`, also fetches `nba_stats_playerdashptpass`
for the top-`max_players` passers (capped, optional -- never a hard
dependency) and recomputes the residual against the richer
`potential_assists` denominator for that subset; `max_players=0`
(default) makes exactly one request total. `ast_pts_created` is passed
through directly from the Passing measure (it is already computed there;
not re-derived).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| str \| list` |  | A single season or list of seasons. |
| `league_id` | `str` | `'00'` | `"00"` NBA (default), `"10"` WNBA, `"20"` G-League. |
| `per_mode` | `str` | `'Totals'` | `per_mode_simple` passed to the fetch (default `"Totals"`). |
| `by_position` | `bool` | `True` | Compute the baseline within role buckets (default); `False` forces one league-wide bucket. |
| `positions` | `Optional[DataFrame]` | `None` | Optional pre-fetched positions frame. |
| `fetch_potential_assists` | `bool` | `False` | Enrich the top passers with `playerdashptpass` potential-assist counts. |
| `max_players` | `int` | `0` | Cap on per-player enrichment fetches; `0` disables enrichment regardless of `fetch_potential_assists`. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |
| `_get_fn` | `Optional[Callable[..., dict]]` | `None` | Injectable replacement for `nba_stats_leaguedashptstats`. |
| `_pass_get_fn` | `Optional[Callable[..., dict]]` | `None` | Injectable replacement for `nba_stats_playerdashptpass`. |

**Returns**

One row per player-season: `season:Int64, player_id:Utf8, player_name:Utf8, team_id:Utf8, position_bucket:Utf8, gp:Int64, min:Float64, ast:Float64, passes:Float64, ast_baseline_rate:Float64, ast_expected:Float64, ast_oe:Float64, ast_oe_per_36:Float64, ast_pts_created:Float64, league_id:Utf8`. Empty/malformed input returns a zero-row frame with this schema.

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.nba import nba_tracking_pass_value
df = nba_tracking_pass_value(2024)
print(df.sort("ast_oe", descending=True).head())

# With potential-assist enrichment for the top 50 passers

df = nba_tracking_pass_value(2024, fetch_potential_assists=True, max_players=50)
```

### nba_tracking_reb_oe {#nba_tracking_reb_oe}

`nba_tracking_reb_oe(seasons: "'int | str | list'", *, league_id: 'str' = '00', per_mode: 'str' = 'Totals', by_position: 'bool' = True, positions: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False, _get_fn: 'Optional[Callable[..., dict]]' = None) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Rebounding-over-expected: `reb_oe` plus OREB/DREB splits, per player-season.

Fetches the `Rebounding` `leaguedashptstats` measure, attaches a
`guard`/`wing`/`big` role bucket, and computes
`reb_oe = reb - reb_chances * bucket_rate` (contest-difficulty-adjusted
when the endpoint carries separate contested/uncontested CHANCE columns;
the live `stats.nba.com` payload currently does not, so this degrades
gracefully to the plain rate -- see the fixtures README for the finding).
OREB/DREB residuals are computed identically against their own chance
columns. Baselines are recomputed from the same season slice on every
call -- there is no fitted constant or bundled artifact.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| str \| list` |  | A single season (`int` ending-year or `"YYYY-YY"` string) or a list of seasons to concatenate. |
| `league_id` | `str` | `'00'` | `"00"` NBA (default), `"10"` WNBA, `"20"` G-League. |
| `per_mode` | `str` | `'Totals'` | `per_mode_simple` passed to the fetch (default `"Totals"`). |
| `by_position` | `bool` | `True` | Compute the baseline within `guard`/`wing`/`big` buckets (default). `False` forces one league-wide bucket. |
| `positions` | `Optional[DataFrame]` | `None` | Optional pre-fetched positions frame (see `_attach_role_bucket`); mostly for injecting a fixture in tests. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |
| `_get_fn` | `Optional[Callable[..., dict]]` | `None` | Injectable replacement for `nba_stats_leaguedashptstats` returning the raw payload dict directly -- offline testing hook. |

**Returns**

One row per player-season: `season:Int64, player_id:Utf8, player_name:Utf8, team_id:Utf8, position_bucket:Utf8, gp:Int64, min:Float64, reb:Float64, reb_chances:Float64, reb_baseline_rate:Float64, reb_expected:Float64, reb_oe:Float64, reb_oe_per_36:Float64, oreb_oe:Float64, dreb_oe:Float64, league_id:Utf8`. Empty/malformed input returns a zero-row frame with this schema.

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.nba import nba_tracking_reb_oe
df = nba_tracking_reb_oe(2024)
print(df.sort("reb_oe", descending=True).head())

# League-wide baseline (no position split)

df_all = nba_tracking_reb_oe(2024, by_position=False)

# Pandas output

df_pd = nba_tracking_reb_oe(2024, return_as_pandas=True)
```

### nba_tracking_rim_protect_value {#nba_tracking_rim_protect_value}

`nba_tracking_rim_protect_value(seasons: "'int | str | list'", *, league_id: 'str' = '00', per_mode: 'str' = 'Totals', by_position: 'bool' = True, positions: 'Optional[pl.DataFrame]' = None, source: 'str' = 'leaguedash', max_players: 'int' = 0, return_as_pandas: 'bool' = False, _get_fn: 'Optional[Callable[..., dict]]' = None, _defend_get_fn: 'Optional[Callable[..., dict]]' = None) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Rim-protection / shot-defend points-saved over expected, per player-season.

Fetches the `Defense` `leaguedashptstats` measure -- which on the live
`stats.nba.com` payload exposes only rim-band defended shooting
(`def_rim_fgm`/`def_rim_fga`/`def_rim_fg_pct`, no separate overall
figure -- see the fixtures README) -- and computes
`rim_protect_pts_saved = (normal_fg_pct - d_fg_pct) * d_fga * 2` where
`normal_fg_pct` is the bucket-mean defended rate (there is no
shooters'-own-average column on this endpoint, so the bucket mean is the
baseline; this is the same attempts-weighted construction as every other
model, just sign-flipped so a defender who holds shooters BELOW the
bucket mean gets a positive points-saved value).

`source="shotdefend"` swaps in the `Less-Than-6-Ft` band from
`nba_stats_playerdashptshotdefend` for the top-`max_players` defenders
by attempt volume (capped, optional -- never a hard dependency);
`max_players=0` (default) uses the leaguedash figures for everyone.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| str \| list` |  | A single season or list of seasons. |
| `league_id` | `str` | `'00'` | `"00"` NBA (default), `"10"` WNBA, `"20"` G-League. |
| `per_mode` | `str` | `'Totals'` | `per_mode_simple` passed to the fetch (default `"Totals"`). |
| `by_position` | `bool` | `True` | Compute the baseline within role buckets (default); `False` forces one league-wide bucket. |
| `positions` | `Optional[DataFrame]` | `None` | Optional pre-fetched positions frame. |
| `source` | `str` | `'leaguedash'` | `"leaguedash"` (default) or `"shotdefend"`. |
| `max_players` | `int` | `0` | Cap on per-player `shotdefend` enrichment fetches; ignored unless `source="shotdefend"`. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |
| `_get_fn` | `Optional[Callable[..., dict]]` | `None` | Injectable replacement for `nba_stats_leaguedashptstats`. |
| `_defend_get_fn` | `Optional[Callable[..., dict]]` | `None` | Injectable replacement for `nba_stats_playerdashptshotdefend`. |

**Returns**

One row per player-season: `season:Int64, player_id:Utf8, player_name:Utf8, team_id:Utf8, position_bucket:Utf8, gp:Int64, min:Float64, d_fga:Float64, d_fgm:Float64, d_fg_pct:Float64, normal_fg_pct:Float64, rim_protect_pts_saved:Float64, rim_protect_pts_saved_per_36:Float64, source:Utf8, league_id:Utf8`. Empty/malformed input returns a zero-row frame with this schema.

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.nba import nba_tracking_rim_protect_value
df = nba_tracking_rim_protect_value(2024)
print(df.sort("rim_protect_pts_saved", descending=True).head())
```

### nba_tracking_shot_diet_value {#nba_tracking_shot_diet_value}

`nba_tracking_shot_diet_value(seasons: "'int | str | list'", *, league_id: 'str' = '00', per_mode: 'str' = 'Totals', by_position: 'bool' = True, positions: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False, _get_fn: 'Optional[Callable[..., dict]]' = None) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Catch-&-shoot vs pull-up points-over-expected, per player-season.

Fetches `CatchShoot` and `PullUpShot` (two calls), scores each with the
shared engine, joins on `player_id` (dtype-asserted `Utf8` both sides
first), and computes `shot_diet_delta = (cs_pts_oe / cs_fga) -
(pu_pts_oe / pu_fga)` (null-safe on zero attempts) -- positive means the
player's efficiency edge comes from catch-&-shoot, negative from
off-the-dribble.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| str \| list` |  | A single season or list of seasons. |
| `league_id` | `str` | `'00'` | `"00"` NBA (default), `"10"` WNBA, `"20"` G-League. |
| `per_mode` | `str` | `'Totals'` | `per_mode_simple` passed to each fetch (default `"Totals"`). |
| `by_position` | `bool` | `True` | Compute each measure's baseline within role buckets (default); `False` forces one league-wide bucket. |
| `positions` | `Optional[DataFrame]` | `None` | Optional pre-fetched positions frame. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |
| `_get_fn` | `Optional[Callable[..., dict]]` | `None` | Injectable replacement for `nba_stats_leaguedashptstats`, dispatched by the `pt_measure_type` kwarg for each of the two calls. |

**Returns**

One row per player-season: `season:Int64, player_id:Utf8, player_name:Utf8, team_id:Utf8, position_bucket:Utf8, cs_fga:Float64, cs_pts:Float64, cs_pts_oe:Float64, pu_fga:Float64, pu_pts:Float64, pu_pts_oe:Float64, shot_diet_delta:Float64, league_id:Utf8`. Empty/malformed input returns a zero-row frame with this schema.

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.nba import nba_tracking_shot_diet_value
df = nba_tracking_shot_diet_value(2024)
print(df.sort("cs_pts_oe", descending=True).head())
```

### nba_tracking_touch_value {#nba_tracking_touch_value}

`nba_tracking_touch_value(seasons: "'int | str | list'", *, league_id: 'str' = '00', per_mode: 'str' = 'Totals', by_position: 'bool' = True, positions: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False, _get_fn: 'Optional[Callable[..., dict]]' = None) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Touch / possession-time value over expected, per player-season.

Fetches the `Possessions` `leaguedashptstats` measure and computes
`pts_per_touch_oe = pts - touches * bucket_pts_per_touch`.
`time_of_poss_eff` is the z-score of `pts / time_of_poss` within the
player's role bucket -- scoring economy per second of possession,
independent of touch volume.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| str \| list` |  | A single season or list of seasons. |
| `league_id` | `str` | `'00'` | `"00"` NBA (default), `"10"` WNBA, `"20"` G-League. |
| `per_mode` | `str` | `'Totals'` | `per_mode_simple` passed to the fetch (default `"Totals"`). |
| `by_position` | `bool` | `True` | Compute the baseline within role buckets (default); `False` forces one league-wide bucket. |
| `positions` | `Optional[DataFrame]` | `None` | Optional pre-fetched positions frame. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |
| `_get_fn` | `Optional[Callable[..., dict]]` | `None` | Injectable replacement for `nba_stats_leaguedashptstats`. |

**Returns**

One row per player-season: `season:Int64, player_id:Utf8, player_name:Utf8, team_id:Utf8, position_bucket:Utf8, gp:Int64, min:Float64, touches:Float64, pts:Float64, touch_baseline_rate:Float64, touch_expected:Float64, pts_per_touch_oe:Float64, time_of_poss:Float64, time_of_poss_eff:Float64, league_id:Utf8`. Empty/malformed input returns a zero-row frame with this schema.

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.nba import nba_tracking_touch_value
df = nba_tracking_touch_value(2024)
print(df.sort("pts_per_touch_oe", descending=True).head())
```
