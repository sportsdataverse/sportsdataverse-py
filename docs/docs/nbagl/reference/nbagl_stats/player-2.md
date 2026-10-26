---
title: "NBAGL — G League Stats API (stats.nba.com, LeagueID=20) — Player: playerprofilev2–playervsplayer"
sidebar_label: "Player: playerprofilev2–playervsplayer"
sidebar_position: 8
description: "NBAGL — G League Stats API (stats.nba.com, LeagueID=20) — Player: playerprofilev2–playervsplayer — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NBAGL — G League Stats API (stats.nba.com, LeagueID=20) — Player: playerprofilev2–playervsplayer

## nbagl_stats_playerprofilev2

GET /stats/playerprofilev2

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
| `player_id` | integer |  |
| `season_id` | character |  |
| `league_id` | character |  |
| `team_id` | integer |  |
| `team_abbreviation` | character |  |
| `player_age` | numeric | NBA or WNBA Stats value for player age in the playerprofilev2 result set. |
| `gp` | integer |  |
| `gs` | integer |  |
| `min` | numeric |  |
| `fgm` | numeric |  |
| `fga` | numeric |  |
| `fg_pct` | numeric |  |
| `fg3_m` | numeric | Three-point field goals made. |
| `fg3_a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric |  |
| `ftm` | numeric |  |
| `fta` | numeric |  |
| `ft_pct` | numeric |  |
| `oreb` | numeric |  |
| `dreb` | numeric |  |
| `reb` | numeric |  |
| `ast` | numeric |  |
| `stl` | numeric |  |
| `blk` | numeric |  |
| `tov` | numeric |  |
| `pf` | numeric |  |
| `pts` | numeric |  |

**CareerTotalsRegularSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | integer |  |
| `league_id` | character |  |
| `team_id` | integer |  |
| `gp` | integer |  |
| `gs` | integer |  |
| `min` | numeric |  |
| `fgm` | numeric |  |
| `fga` | numeric |  |
| `fg_pct` | numeric |  |
| `fg3_m` | numeric | Three-point field goals made. |
| `fg3_a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric |  |
| `ftm` | numeric |  |
| `fta` | numeric |  |
| `ft_pct` | numeric |  |
| `oreb` | numeric |  |
| `dreb` | numeric |  |
| `reb` | numeric |  |
| `ast` | numeric |  |
| `stl` | numeric |  |
| `blk` | numeric |  |
| `tov` | numeric |  |
| `pf` | numeric |  |
| `pts` | numeric |  |

**SeasonTotalsPostSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | integer |  |
| `season_id` | character |  |
| `league_id` | character |  |
| `team_id` | integer |  |
| `team_abbreviation` | character |  |
| `player_age` | numeric | NBA or WNBA Stats value for player age in the playerprofilev2 result set. |
| `gp` | integer |  |
| `gs` | integer |  |
| `min` | numeric |  |
| `fgm` | numeric |  |
| `fga` | numeric |  |
| `fg_pct` | numeric |  |
| `fg3_m` | numeric | Three-point field goals made. |
| `fg3_a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric |  |
| `ftm` | numeric |  |
| `fta` | numeric |  |
| `ft_pct` | numeric |  |
| `oreb` | numeric |  |
| `dreb` | numeric |  |
| `reb` | numeric |  |
| `ast` | numeric |  |
| `stl` | numeric |  |
| `blk` | numeric |  |
| `tov` | numeric |  |
| `pf` | numeric |  |
| `pts` | numeric |  |

**CareerTotalsPostSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | integer |  |
| `league_id` | character |  |
| `team_id` | integer |  |
| `gp` | integer |  |
| `gs` | integer |  |
| `min` | numeric |  |
| `fgm` | numeric |  |
| `fga` | numeric |  |
| `fg_pct` | numeric |  |
| `fg3_m` | numeric | Three-point field goals made. |
| `fg3_a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric |  |
| `ftm` | numeric |  |
| `fta` | numeric |  |
| `ft_pct` | numeric |  |
| `oreb` | numeric |  |
| `dreb` | numeric |  |
| `reb` | numeric |  |
| `ast` | numeric |  |
| `stl` | numeric |  |
| `blk` | numeric |  |
| `tov` | numeric |  |
| `pf` | numeric |  |
| `pts` | numeric |  |

**SeasonTotalsAllStarSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | integer |  |
| `season_id` | character |  |
| `league_id` | character |  |
| `team_id` | integer |  |
| `team_abbreviation` | character |  |
| `player_age` | numeric | NBA or WNBA Stats value for player age in the playerprofilev2 result set. |
| `gp` | integer |  |
| `gs` | integer |  |
| `min` | numeric |  |
| `fgm` | numeric |  |
| `fga` | numeric |  |
| `fg_pct` | numeric |  |
| `fg3_m` | numeric | Three-point field goals made. |
| `fg3_a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric |  |
| `ftm` | numeric |  |
| `fta` | numeric |  |
| `ft_pct` | numeric |  |
| `oreb` | numeric |  |
| `dreb` | numeric |  |
| `reb` | numeric |  |
| `ast` | numeric |  |
| `stl` | numeric |  |
| `blk` | numeric |  |
| `tov` | numeric |  |
| `pf` | numeric |  |
| `pts` | numeric |  |

**CareerTotalsAllStarSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | integer |  |
| `league_id` | character |  |
| `team_id` | integer |  |
| `gp` | integer |  |
| `gs` | integer |  |
| `min` | numeric |  |
| `fgm` | numeric |  |
| `fga` | numeric |  |
| `fg_pct` | numeric |  |
| `fg3_m` | numeric | Three-point field goals made. |
| `fg3_a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric |  |
| `ftm` | numeric |  |
| `fta` | numeric |  |
| `ft_pct` | numeric |  |
| `oreb` | numeric |  |
| `dreb` | numeric |  |
| `reb` | numeric |  |
| `ast` | numeric |  |
| `stl` | numeric |  |
| `blk` | numeric |  |
| `tov` | numeric |  |
| `pf` | numeric |  |
| `pts` | numeric |  |

**SeasonTotalsCollegeSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | character |  |
| `season_id` | character |  |
| `league_id` | character |  |
| `organization_id` | character |  |
| `school_name` | character | College or school of the season row (SeasonTotalsCollegeSeason set). |
| `player_age` | character | NBA or WNBA Stats value for player age in the playerprofilev2 result set. |
| `gp` | character |  |
| `gs` | character |  |
| `min` | character |  |
| `fgm` | character |  |
| `fga` | character |  |
| `fg_pct` | character |  |
| `fg3_m` | character | Three-point field goals made. |
| `fg3_a` | character | Three-point field goal attempts. |
| `fg3_pct` | character |  |
| `ftm` | character |  |
| `fta` | character |  |
| `ft_pct` | character |  |
| `oreb` | character |  |
| `dreb` | character |  |
| `reb` | character |  |
| `ast` | character |  |
| `stl` | character |  |
| `blk` | character |  |
| `tov` | character |  |
| `pf` | character |  |
| `pts` | character |  |

**CareerTotalsCollegeSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | character |  |
| `league_id` | character |  |
| `organization_id` | character |  |
| `gp` | character |  |
| `gs` | character |  |
| `min` | character |  |
| `fgm` | character |  |
| `fga` | character |  |
| `fg_pct` | character |  |
| `fg3_m` | character | Three-point field goals made. |
| `fg3_a` | character | Three-point field goal attempts. |
| `fg3_pct` | character |  |
| `ftm` | character |  |
| `fta` | character |  |
| `ft_pct` | character |  |
| `oreb` | character |  |
| `dreb` | character |  |
| `reb` | character |  |
| `ast` | character |  |
| `stl` | character |  |
| `blk` | character |  |
| `tov` | character |  |
| `pf` | character |  |
| `pts` | character |  |

**SeasonTotalsPreseason**

| col_name | type | description |
|---|---|---|
| `player_id` | integer |  |
| `season_id` | character |  |
| `league_id` | character |  |
| `team_id` | integer |  |
| `team_abbreviation` | character |  |
| `player_age` | numeric | NBA or WNBA Stats value for player age in the playerprofilev2 result set. |
| `gp` | integer |  |
| `gs` | integer |  |
| `min` | numeric |  |
| `fgm` | numeric |  |
| `fga` | numeric |  |
| `fg_pct` | numeric |  |
| `fg3_m` | numeric | Three-point field goals made. |
| `fg3_a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric |  |
| `ftm` | numeric |  |
| `fta` | numeric |  |
| `ft_pct` | numeric |  |
| `oreb` | numeric |  |
| `dreb` | numeric |  |
| `reb` | numeric |  |
| `ast` | numeric |  |
| `stl` | numeric |  |
| `blk` | numeric |  |
| `tov` | numeric |  |
| `pf` | numeric |  |
| `pts` | numeric |  |

**CareerTotalsPreseason**

| col_name | type | description |
|---|---|---|
| `player_id` | integer |  |
| `league_id` | character |  |
| `team_id` | integer |  |
| `gp` | integer |  |
| `gs` | integer |  |
| `min` | numeric |  |
| `fgm` | numeric |  |
| `fga` | numeric |  |
| `fg_pct` | numeric |  |
| `fg3_m` | numeric | Three-point field goals made. |
| `fg3_a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric |  |
| `ftm` | numeric |  |
| `fta` | numeric |  |
| `ft_pct` | numeric |  |
| `oreb` | numeric |  |
| `dreb` | numeric |  |
| `reb` | numeric |  |
| `ast` | numeric |  |
| `stl` | numeric |  |
| `blk` | numeric |  |
| `tov` | numeric |  |
| `pf` | numeric |  |
| `pts` | numeric |  |

**SeasonRankingsRegularSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | integer |  |
| `season_id` | character |  |
| `league_id` | character |  |
| `team_id` | integer |  |
| `team_abbreviation` | character |  |
| `player_age` | character | NBA or WNBA Stats value for player age in the playerprofilev2 result set. |
| `gp` | character |  |
| `gs` | character |  |
| `rank_pg_min` | integer |  |
| `rank_pg_fgm` | integer |  |
| `rank_pg_fga` | integer |  |
| `rank_fg_pct` | integer |  |
| `rank_pg_fg3_m` | integer |  |
| `rank_pg_fg3_a` | integer |  |
| `rank_fg3_pct` | integer |  |
| `rank_pg_ftm` | integer |  |
| `rank_pg_fta` | integer |  |
| `rank_ft_pct` | integer |  |
| `rank_pg_oreb` | integer |  |
| `rank_pg_dreb` | integer |  |
| `rank_pg_reb` | integer |  |
| `rank_pg_ast` | integer |  |
| `rank_pg_stl` | integer |  |
| `rank_pg_blk` | integer |  |
| `rank_pg_tov` | integer |  |
| `rank_pg_pts` | integer |  |
| `rank_pg_eff` | integer |  |

**SeasonRankingsPostSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | integer |  |
| `season_id` | character |  |
| `league_id` | character |  |
| `team_id` | integer |  |
| `team_abbreviation` | character |  |
| `player_age` | character | NBA or WNBA Stats value for player age in the playerprofilev2 result set. |
| `gp` | character |  |
| `gs` | character |  |
| `rank_pg_min` | integer |  |
| `rank_pg_fgm` | integer |  |
| `rank_pg_fga` | integer |  |
| `rank_fg_pct` | integer |  |
| `rank_pg_fg3_m` | integer |  |
| `rank_pg_fg3_a` | integer |  |
| `rank_fg3_pct` | integer |  |
| `rank_pg_ftm` | integer |  |
| `rank_pg_fta` | integer |  |
| `rank_ft_pct` | integer |  |
| `rank_pg_oreb` | integer |  |
| `rank_pg_dreb` | integer |  |
| `rank_pg_reb` | integer |  |
| `rank_pg_ast` | integer |  |
| `rank_pg_stl` | integer |  |
| `rank_pg_blk` | integer |  |
| `rank_pg_tov` | integer |  |
| `rank_pg_pts` | integer |  |
| `rank_pg_eff` | integer |  |

**SeasonHighs**

| col_name | type | description |
|---|---|---|
| `player_id` | integer |  |
| `game_date` | character |  |
| `vs_team_id` | integer |  |
| `vs_team_city` | character |  |
| `vs_team_name` | character |  |
| `vs_team_abbreviation` | character |  |
| `stat` | character |  |
| `stats_value` | integer |  |
| `stat_order` | integer |  |
| `date_est` | character |  |

**CareerHighs**

| col_name | type | description |
|---|---|---|
| `player_id` | integer |  |
| `game_date` | character |  |
| `vs_team_id` | integer |  |
| `vs_team_city` | character |  |
| `vs_team_name` | character |  |
| `vs_team_abbreviation` | character |  |
| `stat` | character |  |
| `stats_value` | integer |  |
| `stat_order` | integer |  |
| `date_est` | character |  |

**NextGame**

| col_name | type | description |
|---|---|---|
| `game_id` | character |  |
| `game_date` | character |  |
| `game_time` | character |  |
| `location` | character |  |
| `player_team_id` | integer | Team id of the player's current team in the NextGame set. |
| `player_team_city` | character |  |
| `player_team_nickname` | character |  |
| `player_team_abbreviation` | character |  |
| `vs_team_id` | integer |  |
| `vs_team_city` | character |  |
| `vs_team_nickname` | character |  |
| `vs_team_abbreviation` | character |  |

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

GET /stats/playervsplayer

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
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | character |  |
| `player_id` | integer |  |
| `player_name` | character |  |
| `gp` | integer |  |
| `w` | integer |  |
| `l` | integer |  |
| `w_pct` | numeric |  |
| `min` | numeric |  |
| `fgm` | numeric |  |
| `fga` | numeric |  |
| `fg_pct` | numeric |  |
| `fg3_m` | numeric | Three-point field goals made. |
| `fg3_a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric |  |
| `ftm` | numeric |  |
| `fta` | numeric |  |
| `ft_pct` | numeric |  |
| `oreb` | numeric |  |
| `dreb` | numeric |  |
| `reb` | numeric |  |
| `ast` | numeric |  |
| `tov` | numeric |  |
| `stl` | numeric |  |
| `blk` | numeric |  |
| `blka` | numeric | Shot attempts blocked by opponents (blocks against). |
| `pf` | numeric |  |
| `pfd` | numeric | Personal fouls drawn. |
| `pts` | numeric |  |
| `plus_minus` | numeric |  |
| `nba_fantasy_pts` | numeric | Fantasy points under the NBA's fantasy scoring formula. |

**OnOffCourt**

| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `player_id` | integer |  |
| `player_name` | character |  |
| `vs_player_id` | integer | Stats API player id of the comparison (vs.) player. |
| `vs_player_name` | character | Name of the comparison (vs.) player. |
| `court_status` | character | Whether the split covers minutes with the vs. player on or off the court. |
| `gp` | integer |  |
| `w` | integer |  |
| `l` | integer |  |
| `w_pct` | numeric |  |
| `min` | numeric |  |
| `fgm` | numeric |  |
| `fga` | numeric |  |
| `fg_pct` | numeric |  |
| `fg3_m` | numeric | Three-point field goals made. |
| `fg3_a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric |  |
| `ftm` | numeric |  |
| `fta` | numeric |  |
| `ft_pct` | numeric |  |
| `oreb` | numeric |  |
| `dreb` | numeric |  |
| `reb` | numeric |  |
| `ast` | numeric |  |
| `tov` | numeric |  |
| `stl` | numeric |  |
| `blk` | numeric |  |
| `blka` | numeric | Shot attempts blocked by opponents (blocks against). |
| `pf` | numeric |  |
| `pfd` | numeric | Personal fouls drawn. |
| `pts` | numeric |  |
| `plus_minus` | numeric |  |
| `nba_fantasy_pts` | numeric | Fantasy points under the NBA's fantasy scoring formula. |

**ShotDistanceOverall**

| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | character |  |
| `player_id` | integer |  |
| `player_name` | character |  |
| `fgm` | numeric |  |
| `fga` | numeric |  |
| `fg_pct` | numeric |  |

**ShotDistanceOnCourt**

| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `player_id` | integer |  |
| `player_name` | character |  |
| `vs_player_id` | integer | Stats API player id of the comparison (vs.) player. |
| `vs_player_name` | character | Name of the comparison (vs.) player. |
| `court_status` | character | Whether the split covers minutes with the vs. player on or off the court. |
| `group_value` | character |  |
| `fgm` | numeric |  |
| `fga` | numeric |  |
| `fg_pct` | numeric |  |

**ShotDistanceOffCourt**

| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `player_id` | integer |  |
| `player_name` | character |  |
| `vs_player_id` | integer | Stats API player id of the comparison (vs.) player. |
| `vs_player_name` | character | Name of the comparison (vs.) player. |
| `court_status` | character | Whether the split covers minutes with the vs. player on or off the court. |
| `group_value` | character |  |
| `fgm` | numeric |  |
| `fga` | numeric |  |
| `fg_pct` | numeric |  |

**ShotAreaOverall**

| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | character |  |
| `player_id` | integer |  |
| `player_name` | character |  |
| `fgm` | numeric |  |
| `fga` | numeric |  |
| `fg_pct` | numeric |  |

**ShotAreaOnCourt**

| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `player_id` | integer |  |
| `player_name` | character |  |
| `vs_player_id` | integer | Stats API player id of the comparison (vs.) player. |
| `vs_player_name` | character | Name of the comparison (vs.) player. |
| `court_status` | character | Whether the split covers minutes with the vs. player on or off the court. |
| `group_value` | character |  |
| `fgm` | numeric |  |
| `fga` | numeric |  |
| `fg_pct` | numeric |  |

**ShotAreaOffCourt**

| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `player_id` | integer |  |
| `player_name` | character |  |
| `vs_player_id` | integer | Stats API player id of the comparison (vs.) player. |
| `vs_player_name` | character | Name of the comparison (vs.) player. |
| `court_status` | character | Whether the split covers minutes with the vs. player on or off the court. |
| `group_value` | character |  |
| `fgm` | numeric |  |
| `fga` | numeric |  |
| `fg_pct` | numeric |  |

**PlayerInfo**

| col_name | type | description |
|---|---|---|
| `person_id` | integer |  |
| `first_name` | character |  |
| `last_name` | character |  |
| `display_first_last` | character |  |
| `display_last_comma_first` | character |  |
| `display_fi_last` | character |  |
| `birthdate` | character | Date of birth as an ISO timestamp string (e.g. '1984-12-30T00:00:00'). |
| `school` | character |  |
| `country` | character |  |
| `last_affiliation` | character |  |

**VsPlayerInfo**

| col_name | type | description |
|---|---|---|
| `person_id` | integer |  |
| `first_name` | character |  |
| `last_name` | character |  |
| `display_first_last` | character |  |
| `display_last_comma_first` | character |  |
| `display_fi_last` | character |  |
| `birthdate` | character | Date of birth as an ISO timestamp string (e.g. '1984-12-30T00:00:00'). |
| `school` | character |  |
| `country` | character |  |
| `last_affiliation` | character |  |

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
