# NBA G League — G League Stats API (stats.nba.com, LeagueID=20) — Player: playercareerstats–playerindex

> NBA G League — G League Stats API (stats.nba.com, LeagueID=20) — Player: playercareerstats–playerindex — function reference in sdv-py, the SportsDataverse Python package.

## nbagl_stats_playercareerstats

Player career stats.

**Endpoint URL:** `GET https://stats.nba.com/stats/playercareerstats`

**Valid URL:** [https://stats.nba.com/stats/playercareerstats?LeagueID=20&PerMode=Totals&PlayerID=1642443](https://stats.nba.com/stats/playercareerstats?LeagueID=20&PerMode=Totals&PlayerID=1642443)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PerMode` | `per_mode36` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |

### Returns {#nbagl_stats_playercareerstats-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`SeasonTotalsRegularSeason`, `CareerTotalsRegularSeason`, `SeasonTotalsPostSeason`, `CareerTotalsPostSeason`, `SeasonTotalsAllStarSeason`, `CareerTotalsAllStarSeason`, `SeasonTotalsCollegeSeason`, `CareerTotalsCollegeSeason`, `SeasonTotalsShowcaseSeason`, `CareerTotalsShowcaseSeason`, `SeasonRankingsRegularSeason`, `SeasonRankingsPostSeason`, `SeasonHighs`, `CareerHighs`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**SeasonTotalsRegularSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 |  |
| `season_id` | String |  |
| `league_id` | String |  |
| `team_id` | Int64 |  |
| `team_abbreviation` | String |  |
| `player_age` | Float64 | NBA or WNBA Stats value for player age in the playercareerstats result set. |
| `gp` | Int64 |  |
| `gs` | Int64 |  |
| `min` | Float64 |  |
| `fgm` | Float64 |  |
| `fga` | Float64 |  |
| `fg_pct` | Float64 |  |
| `fg3_m` | Float64 | Three-point field goals made. |
| `fg3_a` | Float64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 |  |
| `ftm` | Float64 |  |
| `fta` | Float64 |  |
| `ft_pct` | Float64 |  |
| `oreb` | Float64 |  |
| `dreb` | Float64 |  |
| `reb` | Float64 |  |
| `ast` | Float64 |  |
| `stl` | Float64 |  |
| `blk` | Float64 |  |
| `tov` | Float64 |  |
| `pf` | Float64 |  |
| `pts` | Float64 |  |

**CareerTotalsRegularSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 |  |
| `league_id` | String |  |
| `team_id` | Int64 |  |
| `gp` | Int64 |  |
| `gs` | Int64 |  |
| `min` | Float64 |  |
| `fgm` | Float64 |  |
| `fga` | Float64 |  |
| `fg_pct` | Float64 |  |
| `fg3_m` | Float64 | Three-point field goals made. |
| `fg3_a` | Float64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 |  |
| `ftm` | Float64 |  |
| `fta` | Float64 |  |
| `ft_pct` | Float64 |  |
| `oreb` | Float64 |  |
| `dreb` | Float64 |  |
| `reb` | Float64 |  |
| `ast` | Float64 |  |
| `stl` | Float64 |  |
| `blk` | Float64 |  |
| `tov` | Float64 |  |
| `pf` | Float64 |  |
| `pts` | Float64 |  |

**SeasonTotalsPostSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 |  |
| `season_id` | String |  |
| `league_id` | String |  |
| `team_id` | Int64 |  |
| `team_abbreviation` | String |  |
| `player_age` | Float64 | NBA or WNBA Stats value for player age in the playercareerstats result set. |
| `gp` | Int64 |  |
| `gs` | Int64 |  |
| `min` | Float64 |  |
| `fgm` | Float64 |  |
| `fga` | Float64 |  |
| `fg_pct` | Float64 |  |
| `fg3_m` | Float64 | Three-point field goals made. |
| `fg3_a` | Float64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 |  |
| `ftm` | Float64 |  |
| `fta` | Float64 |  |
| `ft_pct` | Float64 |  |
| `oreb` | Float64 |  |
| `dreb` | Float64 |  |
| `reb` | Float64 |  |
| `ast` | Float64 |  |
| `stl` | Float64 |  |
| `blk` | Float64 |  |
| `tov` | Float64 |  |
| `pf` | Float64 |  |
| `pts` | Float64 |  |

**CareerTotalsPostSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 |  |
| `league_id` | String |  |
| `team_id` | Int64 |  |
| `gp` | Int64 |  |
| `gs` | Int64 |  |
| `min` | Float64 |  |
| `fgm` | Float64 |  |
| `fga` | Float64 |  |
| `fg_pct` | Float64 |  |
| `fg3_m` | Float64 | Three-point field goals made. |
| `fg3_a` | Float64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 |  |
| `ftm` | Float64 |  |
| `fta` | Float64 |  |
| `ft_pct` | Float64 |  |
| `oreb` | Float64 |  |
| `dreb` | Float64 |  |
| `reb` | Float64 |  |
| `ast` | Float64 |  |
| `stl` | Float64 |  |
| `blk` | Float64 |  |
| `tov` | Float64 |  |
| `pf` | Float64 |  |
| `pts` | Float64 |  |

**SeasonTotalsAllStarSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 |  |
| `season_id` | String |  |
| `league_id` | String |  |
| `team_id` | Int64 |  |
| `team_abbreviation` | String |  |
| `player_age` | Float64 | NBA or WNBA Stats value for player age in the playercareerstats result set. |
| `gp` | Int64 |  |
| `gs` | Int64 |  |
| `min` | Float64 |  |
| `fgm` | Float64 |  |
| `fga` | Float64 |  |
| `fg_pct` | Float64 |  |
| `fg3_m` | Float64 | Three-point field goals made. |
| `fg3_a` | Float64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 |  |
| `ftm` | Float64 |  |
| `fta` | Float64 |  |
| `ft_pct` | Float64 |  |
| `oreb` | Float64 |  |
| `dreb` | Float64 |  |
| `reb` | Float64 |  |
| `ast` | Float64 |  |
| `stl` | Float64 |  |
| `blk` | Float64 |  |
| `tov` | Float64 |  |
| `pf` | Float64 |  |
| `pts` | Float64 |  |

**CareerTotalsAllStarSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 |  |
| `league_id` | String |  |
| `team_id` | Int64 |  |
| `gp` | Int64 |  |
| `gs` | Int64 |  |
| `min` | Float64 |  |
| `fgm` | Float64 |  |
| `fga` | Float64 |  |
| `fg_pct` | Float64 |  |
| `fg3_m` | Float64 | Three-point field goals made. |
| `fg3_a` | Float64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 |  |
| `ftm` | Float64 |  |
| `fta` | Float64 |  |
| `ft_pct` | Float64 |  |
| `oreb` | Float64 |  |
| `dreb` | Float64 |  |
| `reb` | Float64 |  |
| `ast` | Float64 |  |
| `stl` | Float64 |  |
| `blk` | Float64 |  |
| `tov` | Float64 |  |
| `pf` | Float64 |  |
| `pts` | Float64 |  |

**SeasonTotalsCollegeSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | String |  |
| `season_id` | String |  |
| `league_id` | String |  |
| `organization_id` | String |  |
| `school_name` | String | College or school of the season row (SeasonTotalsCollegeSeason set). |
| `player_age` | String | NBA or WNBA Stats value for player age in the playercareerstats result set. |
| `gp` | String |  |
| `gs` | String |  |
| `min` | String |  |
| `fgm` | String |  |
| `fga` | String |  |
| `fg_pct` | String |  |
| `fg3_m` | String | Three-point field goals made. |
| `fg3_a` | String | Three-point field goal attempts. |
| `fg3_pct` | String |  |
| `ftm` | String |  |
| `fta` | String |  |
| `ft_pct` | String |  |
| `oreb` | String |  |
| `dreb` | String |  |
| `reb` | String |  |
| `ast` | String |  |
| `stl` | String |  |
| `blk` | String |  |
| `tov` | String |  |
| `pf` | String |  |
| `pts` | String |  |

**CareerTotalsCollegeSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | String |  |
| `league_id` | String |  |
| `organization_id` | String |  |
| `gp` | String |  |
| `gs` | String |  |
| `min` | String |  |
| `fgm` | String |  |
| `fga` | String |  |
| `fg_pct` | String |  |
| `fg3_m` | String | Three-point field goals made. |
| `fg3_a` | String | Three-point field goal attempts. |
| `fg3_pct` | String |  |
| `ftm` | String |  |
| `fta` | String |  |
| `ft_pct` | String |  |
| `oreb` | String |  |
| `dreb` | String |  |
| `reb` | String |  |
| `ast` | String |  |
| `stl` | String |  |
| `blk` | String |  |
| `tov` | String |  |
| `pf` | String |  |
| `pts` | String |  |

**SeasonTotalsShowcaseSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 |  |
| `season_id` | String |  |
| `league_id` | String |  |
| `team_id` | Int64 |  |
| `team_abbreviation` | String |  |
| `player_age` | Float64 | NBA or WNBA Stats value for player age in the playercareerstats result set. |
| `gp` | Int64 |  |
| `gs` | Int64 |  |
| `min` | Float64 |  |
| `fgm` | Float64 |  |
| `fga` | Float64 |  |
| `fg_pct` | Float64 |  |
| `fg3_m` | Float64 | Three-point field goals made. |
| `fg3_a` | Float64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 |  |
| `ftm` | Float64 |  |
| `fta` | Float64 |  |
| `ft_pct` | Float64 |  |
| `oreb` | Float64 |  |
| `dreb` | Float64 |  |
| `reb` | Float64 |  |
| `ast` | Float64 |  |
| `stl` | Float64 |  |
| `blk` | Float64 |  |
| `tov` | Float64 |  |
| `pf` | Float64 |  |
| `pts` | Float64 |  |

**CareerTotalsShowcaseSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 |  |
| `league_id` | String |  |
| `team_id` | Int64 |  |
| `gp` | Int64 |  |
| `gs` | Int64 |  |
| `min` | Float64 |  |
| `fgm` | Float64 |  |
| `fga` | Float64 |  |
| `fg_pct` | Float64 |  |
| `fg3_m` | Float64 | Three-point field goals made. |
| `fg3_a` | Float64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 |  |
| `ftm` | Float64 |  |
| `fta` | Float64 |  |
| `ft_pct` | Float64 |  |
| `oreb` | Float64 |  |
| `dreb` | Float64 |  |
| `reb` | Float64 |  |
| `ast` | Float64 |  |
| `stl` | Float64 |  |
| `blk` | Float64 |  |
| `tov` | Float64 |  |
| `pf` | Float64 |  |
| `pts` | Float64 |  |

**SeasonRankingsRegularSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 |  |
| `season_id` | String |  |
| `league_id` | String |  |
| `team_id` | Int64 |  |
| `team_abbreviation` | String |  |
| `player_age` | String | NBA or WNBA Stats value for player age in the playercareerstats result set. |
| `gp` | String |  |
| `gs` | String |  |
| `rank_pg_min` | Int64 |  |
| `rank_pg_fgm` | Int64 |  |
| `rank_pg_fga` | Int64 |  |
| `rank_fg_pct` | Int64 |  |
| `rank_pg_fg3_m` | Int64 |  |
| `rank_pg_fg3_a` | Int64 |  |
| `rank_fg3_pct` | Int64 |  |
| `rank_pg_ftm` | Int64 |  |
| `rank_pg_fta` | Int64 |  |
| `rank_ft_pct` | Int64 |  |
| `rank_pg_oreb` | Int64 |  |
| `rank_pg_dreb` | Int64 |  |
| `rank_pg_reb` | Int64 |  |
| `rank_pg_ast` | Int64 |  |
| `rank_pg_stl` | Int64 |  |
| `rank_pg_blk` | Int64 |  |
| `rank_pg_tov` | Int64 |  |
| `rank_pg_pts` | Int64 |  |
| `rank_pg_eff` | Int64 |  |

**SeasonRankingsPostSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 |  |
| `season_id` | String |  |
| `league_id` | String |  |
| `team_id` | Int64 |  |
| `team_abbreviation` | String |  |
| `player_age` | String | NBA or WNBA Stats value for player age in the playercareerstats result set. |
| `gp` | String |  |
| `gs` | String |  |
| `rank_pg_min` | Int64 |  |
| `rank_pg_fgm` | Int64 |  |
| `rank_pg_fga` | Int64 |  |
| `rank_fg_pct` | Int64 |  |
| `rank_pg_fg3_m` | Int64 |  |
| `rank_pg_fg3_a` | Int64 |  |
| `rank_fg3_pct` | String |  |
| `rank_pg_ftm` | Int64 |  |
| `rank_pg_fta` | Int64 |  |
| `rank_ft_pct` | Int64 |  |
| `rank_pg_oreb` | Int64 |  |
| `rank_pg_dreb` | Int64 |  |
| `rank_pg_reb` | Int64 |  |
| `rank_pg_ast` | Int64 |  |
| `rank_pg_stl` | Int64 |  |
| `rank_pg_blk` | Int64 |  |
| `rank_pg_tov` | Int64 |  |
| `rank_pg_pts` | Int64 |  |
| `rank_pg_eff` | Int64 |  |

**SeasonHighs**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 |  |
| `game_date` | String |  |
| `vs_team_id` | Int64 |  |
| `vs_team_city` | String |  |
| `vs_team_name` | String |  |
| `vs_team_abbreviation` | String |  |
| `stat` | String |  |
| `stats_value` | Int64 |  |
| `stat_order` | Int64 |  |
| `date_est` | String |  |

**CareerHighs**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 |  |
| `game_date` | String |  |
| `vs_team_id` | Int64 |  |
| `vs_team_city` | String |  |
| `vs_team_name` | String |  |
| `vs_team_abbreviation` | String |  |
| `stat` | String |  |
| `stats_value` | Int64 |  |
| `stat_order` | Int64 |  |
| `date_est` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_playercareerstats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_playercareerstats-example}

```python
nbagl_stats_playercareerstats(league_id='20')
```

## nbagl_stats_playerestimatedmetrics

Player estimated metrics.

**Endpoint URL:** `GET https://stats.nba.com/stats/playerestimatedmetrics`

**Valid URL:** [https://stats.nba.com/stats/playerestimatedmetrics?LeagueID=20&Season=2024-25&SeasonType=Regular+Season](https://stats.nba.com/stats/playerestimatedmetrics?LeagueID=20&Season=2024-25&SeasonType=Regular+Season)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A G League regular season starts in late December and becomes the default in January (``2025-26`` from January 2026); a draft combine (May) in June. With season type ``Playoffs`` (or ``commonplayoffseries``) it rolls over in May, after the late-March playoffs start; the G League has no All-Star rows. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonType` | `season_type` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season`` or ``Playoffs`` (each endpoint takes a subset; the G League has no ``All Star`` rows). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in May. |

### Returns {#nbagl_stats_playerestimatedmetrics-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 |  |
| `player_name` | String |  |
| `gp` | Int64 |  |
| `w` | Int64 |  |
| `l` | Int64 |  |
| `w_pct` | Float64 |  |
| `min` | Float64 |  |
| `e_off_rating` | Float64 | Estimated offensive rating for the requested NBA or WNBA Stats split. |
| `e_def_rating` | Float64 | Estimated defensive rating for the requested NBA or WNBA Stats split. |
| `e_net_rating` | Float64 | Estimated net rating for the requested NBA or WNBA Stats split. |
| `e_ast_ratio` | Float64 | Estimated assist ratio for the requested NBA or WNBA Stats split. |
| `e_oreb_pct` | Float64 | Estimated offensive rebound percentage for the requested NBA or WNBA Stats split. |
| `e_dreb_pct` | Float64 | Estimated defensive rebound percentage for the requested NBA or WNBA Stats split. |
| `e_reb_pct` | Float64 | Estimated rebound percentage for the requested NBA or WNBA Stats split. |
| `e_tov_pct` | Float64 | Estimated turnovers percentage for the requested NBA or WNBA Stats split. |
| `e_usg_pct` | Float64 | Estimated usage percentage for the requested NBA or WNBA Stats split. |
| `e_pace` | Float64 | Estimated pace for the requested NBA or WNBA Stats split. |
| `gp_rank` | Int64 | Rank for games played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_rank` | Int64 | Rank for wins within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `l_rank` | Int64 | Rank for losses within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_pct_rank` | Int64 | Rank for winning percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `min_rank` | Int64 | Rank for minutes played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_off_rating_rank` | Int64 | Rank for e offensive rating within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_def_rating_rank` | Int64 | Rank for e defensive rating within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_net_rating_rank` | Int64 | Rank for e net rating within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_ast_ratio_rank` | Int64 | Rank for e assists ratio within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_oreb_pct_rank` | Int64 | Rank for e offensive rebounds percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_dreb_pct_rank` | Int64 | Rank for e defensive rebounds percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_reb_pct_rank` | Int64 | Rank for e rebounds percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_tov_pct_rank` | Int64 | Rank for e turnovers percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_usg_pct_rank` | Int64 | Rank for e usage percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_pace_rank` | Int64 | Rank for e pace within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_playerestimatedmetrics-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_playerestimatedmetrics-example}

```python
nbagl_stats_playerestimatedmetrics(league_id='20', season='2024-25')
```

## nbagl_stats_playerfantasyprofile

Player fantasy profile.

**Endpoint URL:** `GET https://stats.nba.com/stats/playerfantasyprofile`

**Valid URL:** [https://stats.nba.com/stats/playerfantasyprofile?LeagueID=20&MeasureType=Base&PaceAdjust=N&PerMode=Totals&PlayerID=1642443&PlusMinus=N&Rank=N&Season=2024-25&SeasonType=Regular+Season](https://stats.nba.com/stats/playerfantasyprofile?LeagueID=20&MeasureType=Base&PaceAdjust=N&PerMode=Totals&PlayerID=1642443&PlusMinus=N&Rank=N&Season=2024-25&SeasonType=Regular+Season)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `MeasureType` | `measure_type` |  |  | `Y` |  |
| `PaceAdjust` | `pace_adjust` |  |  | `Y` |  |
| `PerMode` | `per_mode` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A G League regular season starts in late December and becomes the default in January (``2025-26`` from January 2026); a draft combine (May) in June. With season type ``Playoffs`` (or ``commonplayoffseries``) it rolls over in May, after the late-March playoffs start; the G League has no All-Star rows. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonType` | `season_type` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season`` or ``Playoffs`` (each endpoint takes a subset; the G League has no ``All Star`` rows). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in May. |

### Returns {#nbagl_stats_playerfantasyprofile-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`Overall`, `Location`, `LastNGames`, `DaysRestModified`, `Opponent`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**Overall**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | String | Value of the split within the group (e.g. a specific opponent, month, or result). |
| `gp` | Int64 |  |
| `w` | Int64 |  |
| `l` | Int64 |  |
| `w_pct` | Float64 |  |
| `min` | Float64 |  |
| `fgm` | Float64 |  |
| `fga` | Float64 |  |
| `fg_pct` | Float64 |  |
| `fg3_m` | Float64 | Three-point field goals made. |
| `fg3_a` | Float64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 |  |
| `ftm` | Float64 |  |
| `fta` | Float64 |  |
| `ft_pct` | Float64 |  |
| `oreb` | Float64 |  |
| `dreb` | Float64 |  |
| `reb` | Float64 |  |
| `ast` | Float64 |  |
| `tov` | Float64 |  |
| `stl` | Float64 |  |
| `blk` | Float64 |  |
| `blka` | Float64 | Shot attempts blocked by opponents (blocks against). |
| `pf` | Float64 |  |
| `pfd` | Float64 | Personal fouls drawn. |
| `pts` | Float64 |  |
| `plus_minus` | Float64 |  |
| `dd2` | Int64 | Double-doubles recorded over the split. |
| `td3` | Int64 | Triple-doubles recorded over the split. |
| `fan_duel_pts` | Float64 | Fantasy points under FanDuel's scoring formula. |
| `nba_fantasy_pts` | Float64 | Fantasy points under the NBA's fantasy scoring formula. |

**Location**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | String | Value of the split within the group (e.g. a specific opponent, month, or result). |
| `gp` | Int64 |  |
| `w` | Int64 |  |
| `l` | Int64 |  |
| `w_pct` | Float64 |  |
| `min` | Float64 |  |
| `fgm` | Float64 |  |
| `fga` | Float64 |  |
| `fg_pct` | Float64 |  |
| `fg3_m` | Float64 | Three-point field goals made. |
| `fg3_a` | Float64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 |  |
| `ftm` | Float64 |  |
| `fta` | Float64 |  |
| `ft_pct` | Float64 |  |
| `oreb` | Float64 |  |
| `dreb` | Float64 |  |
| `reb` | Float64 |  |
| `ast` | Float64 |  |
| `tov` | Float64 |  |
| `stl` | Float64 |  |
| `blk` | Float64 |  |
| `blka` | Float64 | Shot attempts blocked by opponents (blocks against). |
| `pf` | Float64 |  |
| `pfd` | Float64 | Personal fouls drawn. |
| `pts` | Float64 |  |
| `plus_minus` | Float64 |  |
| `dd2` | Int64 | Double-doubles recorded over the split. |
| `td3` | Int64 | Triple-doubles recorded over the split. |
| `fan_duel_pts` | Float64 | Fantasy points under FanDuel's scoring formula. |
| `nba_fantasy_pts` | Float64 | Fantasy points under the NBA's fantasy scoring formula. |

**LastNGames**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | String | Value of the split within the group (e.g. a specific opponent, month, or result). |
| `gp` | Int64 |  |
| `w` | Int64 |  |
| `l` | Int64 |  |
| `w_pct` | Float64 |  |
| `min` | Float64 |  |
| `fgm` | Float64 |  |
| `fga` | Float64 |  |
| `fg_pct` | Float64 |  |
| `fg3_m` | Float64 | Three-point field goals made. |
| `fg3_a` | Float64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 |  |
| `ftm` | Float64 |  |
| `fta` | Float64 |  |
| `ft_pct` | Float64 |  |
| `oreb` | Float64 |  |
| `dreb` | Float64 |  |
| `reb` | Float64 |  |
| `ast` | Float64 |  |
| `tov` | Float64 |  |
| `stl` | Float64 |  |
| `blk` | Float64 |  |
| `blka` | Float64 | Shot attempts blocked by opponents (blocks against). |
| `pf` | Float64 |  |
| `pfd` | Float64 | Personal fouls drawn. |
| `pts` | Float64 |  |
| `plus_minus` | Float64 |  |
| `dd2` | Int64 | Double-doubles recorded over the split. |
| `td3` | Int64 | Triple-doubles recorded over the split. |
| `fan_duel_pts` | Float64 | Fantasy points under FanDuel's scoring formula. |
| `nba_fantasy_pts` | Float64 | Fantasy points under the NBA's fantasy scoring formula. |

**DaysRestModified**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | String | Value of the split within the group (e.g. a specific opponent, month, or result). |
| `season_year` | String |  |
| `gp` | Int64 |  |
| `w` | Int64 |  |
| `l` | Int64 |  |
| `w_pct` | Float64 |  |
| `min` | Float64 |  |
| `fgm` | Float64 |  |
| `fga` | Float64 |  |
| `fg_pct` | Float64 |  |
| `fg3_m` | Float64 | Three-point field goals made. |
| `fg3_a` | Float64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 |  |
| `ftm` | Float64 |  |
| `fta` | Float64 |  |
| `ft_pct` | Float64 |  |
| `oreb` | Float64 |  |
| `dreb` | Float64 |  |
| `reb` | Float64 |  |
| `ast` | Float64 |  |
| `tov` | Float64 |  |
| `stl` | Float64 |  |
| `blk` | Float64 |  |
| `blka` | Float64 | Shot attempts blocked by opponents (blocks against). |
| `pf` | Float64 |  |
| `pfd` | Float64 | Personal fouls drawn. |
| `pts` | Float64 |  |
| `plus_minus` | Float64 |  |
| `dd2` | Int64 | Double-doubles recorded over the split. |
| `td3` | Int64 | Triple-doubles recorded over the split. |
| `fan_duel_pts` | Float64 | Fantasy points under FanDuel's scoring formula. |
| `nba_fantasy_pts` | Float64 | Fantasy points under the NBA's fantasy scoring formula. |

**Opponent**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | String | Value of the split within the group (e.g. a specific opponent, month, or result). |
| `gp` | Int64 |  |
| `w` | Int64 |  |
| `l` | Int64 |  |
| `w_pct` | Float64 |  |
| `min` | Float64 |  |
| `fgm` | Float64 |  |
| `fga` | Float64 |  |
| `fg_pct` | Float64 |  |
| `fg3_m` | Float64 | Three-point field goals made. |
| `fg3_a` | Float64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 |  |
| `ftm` | Float64 |  |
| `fta` | Float64 |  |
| `ft_pct` | Float64 |  |
| `oreb` | Float64 |  |
| `dreb` | Float64 |  |
| `reb` | Float64 |  |
| `ast` | Float64 |  |
| `tov` | Float64 |  |
| `stl` | Float64 |  |
| `blk` | Float64 |  |
| `blka` | Float64 | Shot attempts blocked by opponents (blocks against). |
| `pf` | Float64 |  |
| `pfd` | Float64 | Personal fouls drawn. |
| `pts` | Float64 |  |
| `plus_minus` | Float64 |  |
| `dd2` | Int64 | Double-doubles recorded over the split. |
| `td3` | Int64 | Triple-doubles recorded over the split. |
| `fan_duel_pts` | Float64 | Fantasy points under FanDuel's scoring formula. |
| `nba_fantasy_pts` | Float64 | Fantasy points under the NBA's fantasy scoring formula. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_playerfantasyprofile-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_playerfantasyprofile-example}

```python
nbagl_stats_playerfantasyprofile(league_id='20', season='2024-25')
```

## nbagl_stats_playerfantasyprofilebargraph

Player fantasy profile bar graph.

**Endpoint URL:** `GET https://stats.nba.com/stats/playerfantasyprofilebargraph`

**Valid URL:** [https://stats.nba.com/stats/playerfantasyprofilebargraph?LeagueID=20&PlayerID=1642443&Season=2024-25&SeasonType=Regular+Season](https://stats.nba.com/stats/playerfantasyprofilebargraph?LeagueID=20&PlayerID=1642443&Season=2024-25&SeasonType=Regular+Season)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A G League regular season starts in late December and becomes the default in January (``2025-26`` from January 2026); a draft combine (May) in June. With season type ``Playoffs`` (or ``commonplayoffseries``) it rolls over in May, after the late-March playoffs start; the G League has no All-Star rows. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonType` | `season_type_all_star_nullable` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season`` or ``Playoffs`` (each endpoint takes a subset; the G League has no ``All Star`` rows). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in May. |

### Returns {#nbagl_stats_playerfantasyprofilebargraph-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`SeasonAvg`, `LastFiveGamesAvg`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**SeasonAvg**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 |  |
| `player_name` | String |  |
| `team_id` | Int64 |  |
| `team_abbreviation` | String |  |
| `fan_duel_pts` | Float64 | Scoring or score-margin metric for fan duel points in the requested NBA or WNBA Stats split. |
| `nba_fantasy_pts` | Float64 | Nba fantasy points for the requested NBA or WNBA Stats split. |
| `pts` | Float64 |  |
| `reb` | Float64 |  |
| `ast` | Float64 |  |
| `fg3_m` | Float64 | Three-point field goals made. |
| `ft_pct` | Float64 |  |
| `stl` | Float64 |  |
| `blk` | Float64 |  |
| `tov` | Float64 |  |
| `fg_pct` | Float64 |  |

**LastFiveGamesAvg**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 |  |
| `player_name` | String |  |
| `team_id` | Int64 |  |
| `team_abbreviation` | String |  |
| `fan_duel_pts` | Float64 | Scoring or score-margin metric for fan duel points in the requested NBA or WNBA Stats split. |
| `nba_fantasy_pts` | Float64 | Nba fantasy points for the requested NBA or WNBA Stats split. |
| `pts` | Float64 |  |
| `reb` | Float64 |  |
| `ast` | Float64 |  |
| `fg3_m` | Float64 | Three-point field goals made. |
| `ft_pct` | Float64 |  |
| `stl` | Float64 |  |
| `blk` | Float64 |  |
| `tov` | Float64 |  |
| `fg_pct` | Float64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_playerfantasyprofilebargraph-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_playerfantasyprofilebargraph-example}

```python
nbagl_stats_playerfantasyprofilebargraph(league_id='20', season='2024-25')
```

## nbagl_stats_playergamelog

Player game log.

**Endpoint URL:** `GET https://stats.nba.com/stats/playergamelog`

**Valid URL:** [https://stats.nba.com/stats/playergamelog?DateFrom=&DateTo=&LeagueID=20&PlayerID=1642443&Season=2024-25&SeasonType=Regular+Season](https://stats.nba.com/stats/playergamelog?DateFrom=&DateTo=&LeagueID=20&PlayerID=1642443&Season=2024-25&SeasonType=Regular+Season)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A G League regular season starts in late December and becomes the default in January (``2025-26`` from January 2026); a draft combine (May) in June. With season type ``Playoffs`` (or ``commonplayoffseries``) it rolls over in May, after the late-March playoffs start; the G League has no All-Star rows. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonType` | `season_type_all_star` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season`` or ``Playoffs`` (each endpoint takes a subset; the G League has no ``All Star`` rows). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in May. |

### Returns {#nbagl_stats_playergamelog-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season_id` | String |  |
| `player_id` | Int64 |  |
| `game_id` | String |  |
| `game_date` | String |  |
| `matchup` | String |  |
| `wl` | String |  |
| `min` | Int64 |  |
| `fgm` | Int64 |  |
| `fga` | Int64 |  |
| `fg_pct` | Float64 |  |
| `fg3_m` | Int64 | Three-point field goals made. |
| `fg3_a` | Int64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 |  |
| `ftm` | Int64 |  |
| `fta` | Int64 |  |
| `ft_pct` | Float64 |  |
| `oreb` | Int64 |  |
| `dreb` | Int64 |  |
| `reb` | Int64 |  |
| `ast` | Int64 |  |
| `stl` | Int64 |  |
| `blk` | Int64 |  |
| `tov` | Int64 |  |
| `pf` | Int64 |  |
| `pts` | Int64 |  |
| `plus_minus` | Int64 |  |
| `video_available` | Int64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_playergamelog-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_playergamelog-example}

```python
nbagl_stats_playergamelog(league_id='20', season='2024-25')
```

## nbagl_stats_playergamelogs

Player game logs.

**Endpoint URL:** `GET https://stats.nba.com/stats/playergamelogs`

**Valid URL:** [https://stats.nba.com/stats/playergamelogs?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=20&Location=&MeasureType=Base&Month=0&Outcome=&PORound=&PerMode=Totals&Period=0&PlayerID=&Season=2024-25&SeasonSegment=&SeasonType=Regular+Season&TeamID=0&VsConference=&VsDivision=](https://stats.nba.com/stats/playergamelogs?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=20&Location=&MeasureType=Base&Month=0&Outcome=&PORound=&PerMode=Totals&Period=0&PlayerID=&Season=2024-25&SeasonSegment=&SeasonType=Regular+Season&TeamID=0&VsConference=&VsDivision=)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `MeasureType` | `measure_type_player_game_logs_nullable` |  |  | `Y` |  |
| `Month` | `month_nullable` |  |  | `Y` |  |
| `OppTeamID` | `oppteamid` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PerMode` | `per_mode_simple_nullable` |  |  | `Y` |  |
| `Period` | `period_nullable` |  |  | `Y` |  |
| `PlayerID` | `player_id_nullable` |  |  | `Y` |  |
| `Season` | `season_nullable` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A G League regular season starts in late December and becomes the default in January (``2025-26`` from January 2026); a draft combine (May) in June. With season type ``Playoffs`` (or ``commonplayoffseries``) it rolls over in May, after the late-March playoffs start; the G League has no All-Star rows. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_nullable` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season`` or ``Playoffs`` (each endpoint takes a subset; the G League has no ``All Star`` rows). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in May. |
| `ShotClockRange` | `shot_clock_range_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns {#nbagl_stats_playergamelogs-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season_year` | String |  |
| `player_id` | Int64 |  |
| `player_name` | String |  |
| `nickname` | String |  |
| `team_id` | Int64 |  |
| `team_abbreviation` | String |  |
| `team_name` | String |  |
| `game_id` | String |  |
| `game_date` | String |  |
| `matchup` | String |  |
| `wl` | String |  |
| `min` | Float64 |  |
| `fgm` | Float64 |  |
| `fga` | Float64 |  |
| `fg_pct` | Float64 |  |
| `fg3_m` | Float64 | Three-point field goals made. |
| `fg3_a` | Float64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 |  |
| `ftm` | Float64 |  |
| `fta` | Float64 |  |
| `ft_pct` | Float64 |  |
| `oreb` | Float64 |  |
| `dreb` | Float64 |  |
| `reb` | Float64 |  |
| `ast` | Float64 |  |
| `tov` | Float64 |  |
| `stl` | Float64 |  |
| `blk` | Float64 |  |
| `blka` | Float64 | Blocked field-goal attempts against for the requested NBA or WNBA Stats split. |
| `pf` | Float64 |  |
| `pfd` | Float64 | Personal fouls drawn for the requested NBA or WNBA Stats split. |
| `pts` | Float64 |  |
| `plus_minus` | Float64 |  |
| `nba_fantasy_pts` | Float64 | Nba fantasy points for the requested NBA or WNBA Stats split. |
| `dd2` | Int64 | Double-doubles for the requested NBA or WNBA Stats split. |
| `td3` | Int64 | Triple-doubles for the requested NBA or WNBA Stats split. |
| `wnba_fantasy_pts` | Float64 | Wnba fantasy points for the requested NBA or WNBA Stats split. |
| `gp_rank` | Int64 | Rank for games played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_rank` | Int64 | Rank for wins within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `l_rank` | Int64 | Rank for losses within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_pct_rank` | Int64 | Rank for winning percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `min_rank` | Int64 | Rank for minutes played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fgm_rank` | Int64 | Rank for field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fga_rank` | Int64 | Rank for field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg_pct_rank` | Int64 | Rank for field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_m_rank` | Int64 | Rank for three-point field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_a_rank` | Int64 | Rank for three-point field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_pct_rank` | Int64 | Rank for three-point field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ftm_rank` | Int64 | Rank for free throws made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fta_rank` | Int64 | Rank for free throws attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ft_pct_rank` | Int64 | Rank for free-throw percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `oreb_rank` | Int64 | Rank for offensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `dreb_rank` | Int64 | Rank for defensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `reb_rank` | Int64 | Rank for total rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ast_rank` | Int64 | Rank for assists within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `tov_rank` | Int64 | Rank for turnovers within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `stl_rank` | Int64 | Rank for steals within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blk_rank` | Int64 | Rank for blocks within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blka_rank` | Int64 | Rank for blocked field-goal attempts against within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pf_rank` | Int64 | Rank for personal fouls within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pfd_rank` | Int64 | Rank for personal fouls drawn within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pts_rank` | Int64 | Rank for points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `plus_minus_rank` | Int64 | Rank for plus-minus within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `nba_fantasy_pts_rank` | Int64 | Rank for NBA fantasy points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `dd2_rank` | Int64 | Rank for double-doubles within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `td3_rank` | Int64 | Rank for triple-doubles within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `wnba_fantasy_pts_rank` | Int64 | Rank for WNBA fantasy points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `available_flag` | Int64 | Flag indicating whether the requested NBA or WNBA Stats video or data asset is available. |
| `min_sec` | String | Minutes and seconds played, formatted as a game-clock duration string. |
| `team_count` | Int64 | NBA or WNBA Stats value for team count in the playergamelogs result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_playergamelogs-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_playergamelogs-example}

```python
nbagl_stats_playergamelogs(league_id='20', season_nullable='2024-25')
```

## nbagl_stats_playerindex

Player index.

**Endpoint URL:** `GET https://stats.nba.com/stats/playerindex`

**Valid URL:** [https://stats.nba.com/stats/playerindex?College=&Country=&DraftPick=&DraftRound=&DraftYear=&Height=&Historical=1&LeagueID=20&Season=2024-25&TeamID=0&Weight=](https://stats.nba.com/stats/playerindex?College=&Country=&DraftPick=&DraftRound=&DraftYear=&Height=&Historical=1&LeagueID=20&Season=2024-25&TeamID=0&Weight=)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `Active` | `active_nullable` |  |  | `Y` |  |
| `AllStar` | `allstar_nullable` |  |  | `Y` |  |
| `College` | `college_nullable` |  |  | `Y` |  |
| `Country` | `country_nullable` |  |  | `Y` |  |
| `DraftPick` | `draft_pick_nullable` |  |  | `Y` |  |
| `DraftRound` | `draft_round_nullable` |  |  | `Y` |  |
| `DraftYear` | `draft_year_nullable` |  |  | `Y` |  |
| `Height` | `height_nullable` |  |  | `Y` |  |
| `Historical` | `historical_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A G League regular season starts in late December and becomes the default in January (``2025-26`` from January 2026); a draft combine (May) in June. With season type ``Playoffs`` (or ``commonplayoffseries``) it rolls over in May, after the late-March playoffs start; the G League has no All-Star rows. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `Weight` | `weight_nullable` |  |  | `Y` |  |

### Returns {#nbagl_stats_playerindex-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `person_id` | Int64 |  |
| `player_last_name` | String | Player's last name. |
| `player_first_name` | String | Player's first name. |
| `player_slug` | String |  |
| `team_id` | Int64 |  |
| `team_slug` | String |  |
| `team_city` | String |  |
| `team_name` | String |  |
| `team_abbreviation` | String |  |
| `jersey_number` | String |  |
| `position` | String |  |
| `height` | String |  |
| `weight` | String |  |
| `college` | String |  |
| `country` | String |  |
| `draft_year` | Int64 |  |
| `draft_round` | Int64 |  |
| `draft_number` | Int64 | Overall pick the player was selected with, as a string; 'Undrafted' when never drafted. |
| `roster_status` | Float64 | Roster status flag: 1 when the player is on a current roster, 0 otherwise. |
| `from_year` | String |  |
| `to_year` | String |  |
| `pts` | Float64 |  |
| `reb` | Float64 |  |
| `ast` | Float64 |  |
| `stats_timeframe` | String | Time value for stats timeframe in the NBA or WNBA Stats result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_playerindex-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_playerindex-example}

```python
nbagl_stats_playerindex(league_id='20', season='2024-25')
```
