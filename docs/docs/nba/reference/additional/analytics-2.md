---
title: "NBA — additional Python functions — Analytics: nba_raw–players_on"
sidebar_label: "Analytics: nba_raw–players_on"
sidebar_position: 14
description: "NBA — additional Python functions — Analytics: nba_raw–players_on — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — Analytics: nba_raw–players_on

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

### nbadraft_mock_draft {#nbadraft_mock_draft}

`nbadraft_mock_draft(year: 'Optional[int]' = None, *, proxy: 'Any' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

The current consensus mock draft from NBADraft.net.

One row per pick across both rounds. The page renders round 1 and round 2 as
the first two pick tables and then **repeats round 1 in a third table**, so
only the first two are taken -- concatenating all three double-counts round 1.
The `<noscript>` fallback is another false-positive JS challenge; the pick
tables are static. A traded pick's team cell carries `*`, which is stripped.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `year` | `Optional[int]` | `None` | Draft year (e.g. `2025`). `None` (default) reads the site's current mock; a year uses the `/nba-mock-drafts/{year}/` path where NBADraft.net has one. |
| `proxy` | `Any` | `None` | Proxy configuration forwarded to `~sportsdataverse.dl_utils.download` (`requests` `proxies=` shape). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per pick with `round` (1 or 2), `pick`, `team`, `player`, `height`, `weight`, `position`, `school` and `class`. An unreachable or table-less page yields a zero-row frame with that schema.

| col_name | type | description |
|---|---|---|
| `round` | Int64 | Tournament / playoff round. |
| `pick` | Int64 | Pick number within the round. |
| `team` | String | Team-side label or team identifier. |
| `player` | String | Player name. |
| `height` | String | Player height (string e.g. '6-2' or inches). |
| `weight` | Int64 | Player weight in pounds. |
| `position` | String | Listed roster position (G, F, C, etc.). |
| `school` | String | Player school / pre-draft team. |
| `class` | String | College class / draft eligibility note. |

**Raises**

| Exception | Description |
|---|---|
| `UserWarning` | Emitted (not raised) when the page cannot be fetched or carries no pick table; the call still returns an empty frame. |

**Example**

```python
from sportsdataverse.nba import nbadraft_mock_draft

mock = nbadraft_mock_draft()
print(mock.shape)

# A specific draft year, as pandas

mock_pd = nbadraft_mock_draft(year=2025, return_as_pandas=True)

# Pipeline next step (lottery only)

mock.filter((pl.col("round") == 1) & (pl.col("pick") <= 14))
```

### player_play_context {#player_play_context}

`player_play_context(possessions: 'pl.DataFrame', *, league_non_transition_ppp: 'Optional[float]' = None, apply_ctg_filters: 'bool' = True, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Per-player offensive On/Off Play-Context table (CTG's On/Off page, offense half).

For each player: their team's offensive play-context **with them on the floor**
(`on_*`), **without them** (`off_*`), and the on-minus-off difference
(`diff_*`) — which is the number CTG actually displays.

The OFF side is derived by **subtraction** (team total minus on-court), not by a
second scan. That is deliberate: it makes the partition exact by construction —
`on_poss + off_poss == team_poss` and the same for points — so a leak (a
double-counted possession, a dropped lineup slot) is impossible to hide. The
test suite asserts that identity directly.

Like CTG's on/off, this is a **raw** split: no luck adjustment, no opponent
adjustment, no minutes threshold. It is a descriptive difference, not a causal
estimate — for that, use the RAPM surface
(`~sportsdataverse.nba.nba_rapm.nba_rapm`).

Requires `off_player_1..5` from
`~sportsdataverse.nba.nba_possessions.attach_possession_lineups`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Frame from `add_play_context` **with lineups attached**. |
| `league_non_transition_ppp` | `Optional[float]` | `None` | Pts+/Poss baseline; see `team_play_context`. One baseline is shared across the on and off sides so the diffs are comparable. |
| `apply_ctg_filters` | `bool` | `True` | Drop garbage-time / heave / non-counting possessions first. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per (player, team) with `PLAYER_PLAY_CONTEXT_SCHEMA`. Empty input returns a zero-row frame with that schema.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `offense_team_id` | Int64 | Unique identifier for offense team. |
| `on_poss` | Int64 |  |
| `off_poss` | Int64 |  |
| `on_points` | Int64 |  |
| `off_points` | Int64 |  |
| `on_pts_per_100` | Float64 |  |
| `off_pts_per_100` | Float64 |  |
| `diff_pts_per_100` | Float64 |  |
| `on_transition_freq` | Float64 |  |
| `off_transition_freq` | Float64 |  |
| `diff_transition_freq` | Float64 |  |
| `on_transition_pts_per_100` | Float64 |  |
| `off_transition_pts_per_100` | Float64 |  |
| `diff_transition_pts_per_100` | Float64 |  |
| `on_halfcourt_pts_per_100` | Float64 |  |
| `off_halfcourt_pts_per_100` | Float64 |  |
| `diff_halfcourt_pts_per_100` | Float64 |  |
| `on_transition_pts_added_per_100` | Float64 |  |
| `off_transition_pts_added_per_100` | Float64 |  |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | When the `off_player_*` columns are missing. |

**Example**

```python
poss = attach_possession_lineups(add_play_context(enh), oncourt, enh, home_team_id=home)
onoff = player_play_context(poss)
print(onoff.sort("diff_pts_per_100", descending=True).head())

# Who makes their team run?

print(onoff.sort("diff_transition_freq", descending=True).head())
```

### player_rates {#player_rates}

`player_rates(box_logs: 'pl.DataFrame') -> 'pl.DataFrame'`

Per-player per-minute rate stats from box logs.

Rows with null minutes (DNPs) are dropped. Rate = total stat / total
minutes across the player's games; `minutes_pg` is the mean minutes.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `box_logs` | `DataFrame` |  | Per-player-per-game frame with `player_id, team_id, minutes, pts, reb, ast, fg3m`. |

**Returns**

One row per player: `player_id, team_id, games, minutes_pg, pts_per_min, reb_per_min, ast_per_min, fg3m_per_min`. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `player_id` | String | Unique player identifier. |
| `team_id` | String | Unique team identifier. |
| `games` | Int64 | Games played. |
| `minutes_pg` | Float64 |  |
| `pts_per_min` | Float64 |  |
| `reb_per_min` | Float64 |  |
| `ast_per_min` | Float64 |  |
| `fg3m_per_min` | Float64 |  |

**Example**

```python
from sportsdataverse.nba.nba_player_props import player_rates
rates = player_rates(box_logs)
```

### players_on_court_from_pbp {#players_on_court_from_pbp}

`players_on_court_from_pbp(enhanced_pbp: 'pl.DataFrame', raw_box: 'dict', *, home_team_id: 'int', away_team_id: 'int') -> 'pl.DataFrame'`

Reconstruct the 5-on-5 on-court lineup from pbp subs + boxscore starters.

Pure function (no network). A gamerotation-free alternative to
`players_on_court_from_rotation` returning the identical
`LINEUPS_SCHEMA` frame (one row per action, slots sorted ascending or
`None`). See the module design for the algorithm.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `enhanced_pbp` | `DataFrame` |  | Output of `enhanced_pbp_from_payload`. Must carry `game_id`, `action_number`, `order_index`, `period`, `team_id`, `person_id`, `description`, `is_substitution`. |
| `raw_box` | `dict` |  | Raw `boxscoretraditionalv3` dict (starters + name map). |
| `home_team_id` | `int` |  | Home team id (from `boxscore_home_away`). |
| `away_team_id` | `int` |  | Away team id (from `boxscore_home_away`). |

**Returns**

`polars.DataFrame` conforming to `LINEUPS_SCHEMA`. Empty input returns a zero-row frame (never raises).

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `action_number` | Int64 | Sequential action number within a game (V3 PBP). |
| `period` | Int64 | Period of the game (1-4 quarters; 5+ for OT). |
| `home_player_1` | Int64 |  |
| `home_player_2` | Int64 |  |
| `home_player_3` | Int64 |  |
| `home_player_4` | Int64 |  |
| `home_player_5` | Int64 |  |
| `away_player_1` | Int64 |  |
| `away_player_2` | Int64 |  |
| `away_player_3` | Int64 |  |
| `away_player_4` | Int64 |  |
| `away_player_5` | Int64 |  |

**Example**

```python
import json, pathlib
from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
from sportsdataverse.nba.nba_lineups import (
    boxscore_home_away, players_on_court_from_pbp,
)
box = json.loads(pathlib.Path("boxscoretraditionalv3.json").read_text())
pbp = json.loads(pathlib.Path("playbyplayv3.json").read_text())
enh = enhanced_pbp_from_payload(pbp)
home, away = boxscore_home_away(box)
oc = players_on_court_from_pbp(enh, box, home_team_id=home, away_team_id=away)
print(oc.shape)
```
