# NBA G League — G League Stats API (stats.nba.com, LeagueID=20) — Player: playerprofilev2–playervsplayer

> NBA G League — G League Stats API (stats.nba.com, LeagueID=20) — Player: playerprofilev2–playervsplayer — function reference in sdv-py, the SportsDataverse Python package.

## nbagl_stats_playerprofilev2

Player profile v2.

**Endpoint URL:** `GET https://stats.nba.com/stats/playerprofilev2`

**Valid URL:** [https://stats.nba.com/stats/playerprofilev2?LeagueID=20&PerMode=Totals&PlayerID=1642443](https://stats.nba.com/stats/playerprofilev2?LeagueID=20&PerMode=Totals&PlayerID=1642443)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PerMode` | `per_mode36` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |

### Returns {#nbagl_stats_playerprofilev2-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`SeasonTotalsRegularSeason`, `CareerTotalsRegularSeason`, `SeasonTotalsPostSeason`, `CareerTotalsPostSeason`, `SeasonTotalsAllStarSeason`, `CareerTotalsAllStarSeason`, `SeasonTotalsCollegeSeason`, `CareerTotalsCollegeSeason`, `SeasonTotalsPreseason`, `CareerTotalsPreseason`, `SeasonRankingsRegularSeason`, `SeasonRankingsPostSeason`, `SeasonHighs`, `CareerHighs`, `NextGame`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**SeasonTotalsRegularSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 |  |
| `season_id` | String |  |
| `league_id` | String |  |
| `team_id` | Int64 |  |
| `team_abbreviation` | String |  |
| `player_age` | Float64 | NBA or WNBA Stats value for player age in the playerprofilev2 result set. |
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
| `player_age` | Float64 | NBA or WNBA Stats value for player age in the playerprofilev2 result set. |
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
| `player_age` | Float64 | NBA or WNBA Stats value for player age in the playerprofilev2 result set. |
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
| `player_age` | String | NBA or WNBA Stats value for player age in the playerprofilev2 result set. |
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

**SeasonTotalsPreseason**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 |  |
| `season_id` | String |  |
| `league_id` | String |  |
| `team_id` | Int64 |  |
| `team_abbreviation` | String |  |
| `player_age` | Float64 | NBA or WNBA Stats value for player age in the playerprofilev2 result set. |
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

**CareerTotalsPreseason**

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
| `player_age` | String | NBA or WNBA Stats value for player age in the playerprofilev2 result set. |
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
| `player_age` | String | NBA or WNBA Stats value for player age in the playerprofilev2 result set. |
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

**NextGame**

| col_name | type | description |
|---|---|---|
| `game_id` | String |  |
| `game_date` | String |  |
| `game_time` | String |  |
| `location` | String |  |
| `player_team_id` | Int64 | Team id of the player's current team in the NextGame set. |
| `player_team_city` | String |  |
| `player_team_nickname` | String |  |
| `player_team_abbreviation` | String |  |
| `vs_team_id` | Int64 |  |
| `vs_team_city` | String |  |
| `vs_team_nickname` | String |  |
| `vs_team_abbreviation` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_playerprofilev2-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_playerprofilev2-example}

```python
nbagl_stats_playerprofilev2(league_id='20')
```

## nbagl_stats_playervsplayer

Player vs player.

**Endpoint URL:** `GET https://stats.nba.com/stats/playervsplayer`

**Valid URL:** [https://stats.nba.com/stats/playervsplayer?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=20&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PaceAdjust=N&PerMode=Totals&Period=0&PlayerID=1642443&PlusMinus=N&Rank=N&Season=2024-25&SeasonSegment=&SeasonType=Regular+Season&VsConference=&VsDivision=&VsPlayerID=1631457](https://stats.nba.com/stats/playervsplayer?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=20&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PaceAdjust=N&PerMode=Totals&Period=0&PlayerID=1642443&PlusMinus=N&Rank=N&Season=2024-25&SeasonSegment=&SeasonType=Regular+Season&VsConference=&VsDivision=&VsPlayerID=1631457)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `MeasureType` | `measure_type_detailed_defense` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PaceAdjust` | `pace_adjust` |  |  | `Y` |  |
| `PerMode` | `per_mode_detailed` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A G League regular season starts in late December and becomes the default in January (``2025-26`` from January 2026); a draft combine (May) in June. With season type ``Playoffs`` (or ``commonplayoffseries``) it rolls over in May, after the late-March playoffs start; the G League has no All-Star rows. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season`` or ``Playoffs`` (each endpoint takes a subset; the G League has no ``All Star`` rows). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in May. |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |
| `VsPlayerID` | `vs_player_id` |  |  | `Y` |  |

### Returns {#nbagl_stats_playervsplayer-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`Overall`, `OnOffCourt`, `ShotDistanceOverall`, `ShotDistanceOnCourt`, `ShotDistanceOffCourt`, `ShotAreaOverall`, `ShotAreaOnCourt`, `ShotAreaOffCourt`, `PlayerInfo`, `VsPlayerInfo`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**Overall**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | String |  |
| `player_id` | Int64 |  |
| `player_name` | String |  |
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
| `nba_fantasy_pts` | Float64 | Fantasy points under the NBA's fantasy scoring formula. |

**OnOffCourt**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `player_id` | Int64 |  |
| `player_name` | String |  |
| `vs_player_id` | Int64 | Stats API player id of the comparison (vs.) player. |
| `vs_player_name` | String | Name of the comparison (vs.) player. |
| `court_status` | String | Whether the split covers minutes with the vs. player on or off the court. |
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
| `nba_fantasy_pts` | Float64 | Fantasy points under the NBA's fantasy scoring formula. |

**ShotDistanceOverall**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | String |  |
| `player_id` | Int64 |  |
| `player_name` | String |  |
| `fgm` | Float64 |  |
| `fga` | Float64 |  |
| `fg_pct` | Float64 |  |

**ShotDistanceOnCourt**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `player_id` | Int64 |  |
| `player_name` | String |  |
| `vs_player_id` | Int64 | Stats API player id of the comparison (vs.) player. |
| `vs_player_name` | String | Name of the comparison (vs.) player. |
| `court_status` | String | Whether the split covers minutes with the vs. player on or off the court. |
| `group_value` | String |  |
| `fgm` | Float64 |  |
| `fga` | Float64 |  |
| `fg_pct` | Float64 |  |

**ShotDistanceOffCourt**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `player_id` | Int64 |  |
| `player_name` | String |  |
| `vs_player_id` | Int64 | Stats API player id of the comparison (vs.) player. |
| `vs_player_name` | String | Name of the comparison (vs.) player. |
| `court_status` | String | Whether the split covers minutes with the vs. player on or off the court. |
| `group_value` | String |  |
| `fgm` | Float64 |  |
| `fga` | Float64 |  |
| `fg_pct` | Float64 |  |

**ShotAreaOverall**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | String |  |
| `player_id` | Int64 |  |
| `player_name` | String |  |
| `fgm` | Float64 |  |
| `fga` | Float64 |  |
| `fg_pct` | Float64 |  |

**ShotAreaOnCourt**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `player_id` | Int64 |  |
| `player_name` | String |  |
| `vs_player_id` | Int64 | Stats API player id of the comparison (vs.) player. |
| `vs_player_name` | String | Name of the comparison (vs.) player. |
| `court_status` | String | Whether the split covers minutes with the vs. player on or off the court. |
| `group_value` | String |  |
| `fgm` | Float64 |  |
| `fga` | Float64 |  |
| `fg_pct` | Float64 |  |

**ShotAreaOffCourt**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `player_id` | Int64 |  |
| `player_name` | String |  |
| `vs_player_id` | Int64 | Stats API player id of the comparison (vs.) player. |
| `vs_player_name` | String | Name of the comparison (vs.) player. |
| `court_status` | String | Whether the split covers minutes with the vs. player on or off the court. |
| `group_value` | String |  |
| `fgm` | Float64 |  |
| `fga` | Float64 |  |
| `fg_pct` | Float64 |  |

**PlayerInfo**

| col_name | type | description |
|---|---|---|
| `person_id` | Int64 |  |
| `first_name` | String |  |
| `last_name` | String |  |
| `display_first_last` | String |  |
| `display_last_comma_first` | String |  |
| `display_fi_last` | String |  |
| `birthdate` | String | Date of birth as an ISO timestamp string (e.g. '1984-12-30T00:00:00'). |
| `school` | String |  |
| `country` | String |  |
| `last_affiliation` | String |  |

**VsPlayerInfo**

| col_name | type | description |
|---|---|---|
| `person_id` | Int64 |  |
| `first_name` | String |  |
| `last_name` | String |  |
| `display_first_last` | String |  |
| `display_last_comma_first` | String |  |
| `display_fi_last` | String |  |
| `birthdate` | String | Date of birth as an ISO timestamp string (e.g. '1984-12-30T00:00:00'). |
| `school` | String |  |
| `country` | String |  |
| `last_affiliation` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_playervsplayer-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_playervsplayer-example}

```python
nbagl_stats_playervsplayer(league_id='20', season='2024-25')
```
