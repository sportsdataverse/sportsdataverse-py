---
title: "NBA — additional Python functions — Analytics: nba_team"
sidebar_label: "Analytics: nba_team"
sidebar_position: 20
description: "NBA — additional Python functions — Analytics: nba_team — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — Analytics: nba_team

### nba_team_season_flow {#nba_team_season_flow}

`nba_team_season_flow(seasons: 'int | list[int]', *, league_id: 'str' = '00', season_type: 'str' = 'regular-season', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Game flow per team and season, with ranks, percentiles and `_n`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26). The WNBA season is its calendar year. |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA. |
| `season_type` | `str` | `'regular-season'` | `"regular-season"` (default), `"preseason"`, `"playoffs"` or `"play-in"`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, team_id)`: `games`, `wins`; `lead_pct`, `trail_pct`, `tied_pct`, `avg_margin` and `avg_wp` (weighted by game clock); `lead_changes_pg` and `times_tied_pg`; `largest_lead`, `largest_comeback` and `max_run` (season bests); `runs_10` / `opp_runs_10` and their per-game rates; `avg_longest_drought_sec`; `def_poss`, `stops`, `stop_pct`; `kills`, `double_kills`, `kills_pg`, `double_kills_pg` and `kills_per_100`. The ten rated metrics carry `<m>_rank` (1 = best; `opp_runs_10_pg` and `avg_longest_drought_sec` rank low first), `<m>_pct` and `<m>_n` (games; the games with possessions for `kills_pg` / `double_kills_pg` and with a win probability for `avg_wp`; defensive possessions for `stop_pct` and `kills_per_100`), among teams with at least half the season's most games, null under five teams. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `team_id` | Int64 | stats.nba.com team id. |
| `games` | Int64 | Games played (completed games of the season type). |
| `wins` | Int64 | Games the team won among its completed games of the season type (pts > opp_pts). |
| `trail_pct` | Float64 | Share of the team's game clock spent behind (sum trail_sec / sum game_seconds). |
| `tied_pct` | Float64 | Share of the team's game clock spent tied. |
| `lead_changes_pg` | Float64 | Lead changes per game. |
| `times_tied_pg` | Float64 | Times tied per game. |
| `largest_lead` | Int64 | The team's biggest lead of the season. |
| `largest_comeback` | Int64 | The biggest deficit the team overcame in a win this season (0 if none). |
| `max_run` | Int64 | The team's biggest n-0 run of the season. |
| `runs_10` | Int64 | The team's runs of 10 or more unanswered points, summed over the season. |
| `opp_runs_10` | Int64 | The opponents' runs of 10 or more unanswered points against the team, summed. |
| `def_poss` | Int64 | Defensive possessions, summed. |
| `stops` | Int64 | Stops (scoreless defensive possessions), summed. |
| `kills` | Int64 | Kills (three straight stops), summed. |
| `double_kills` | Int64 | Double kills (six straight stops), summed. |
| `avg_margin` | Float64 | Game control: the score margin averaged over the season's game clock. |
| `avg_wp` | Float64 | Game control: the in-game win probability averaged over the season's game clock. |
| `lead_pct` | Float64 | Share of the team's game clock spent ahead (sum lead_sec / sum game_seconds). |
| `runs_10_pg` | Float64 | The team's 10-0 (or better) runs per game. |
| `opp_runs_10_pg` | Float64 | The opponents' 10-0 (or better) runs against the team per game. |
| `avg_longest_drought_sec` | Float64 | The team's longest scoring drought, in seconds, averaged over its games. |
| `stop_pct` | Float64 | stops / def_poss; null with no possessions. |
| `kills_pg` | Float64 | Kills per game. |
| `double_kills_pg` | Float64 | Double kills per game. |
| `kills_per_100` | Float64 | Kills per 100 defensive possessions; null with no possessions. |
| `avg_margin_rank` | Float64 | Rank of avg_margin within the season (the highest value ranks 1; ties average), among teams with at least half the season's most games; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `avg_wp_rank` | Float64 | Rank of avg_wp within the season (the highest value ranks 1; ties average), among teams with at least half the season's most games; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `lead_pct_rank` | Float64 | Rank of lead_pct within the season (the highest value ranks 1; ties average), among teams with at least half the season's most games; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `runs_10_pg_rank` | Float64 | Rank of runs_10_pg within the season (the highest value ranks 1; ties average), among teams with at least half the season's most games; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `opp_runs_10_pg_rank` | Float64 | Rank of opp_runs_10_pg within the season (the lowest value ranks 1; ties average), among teams with at least half the season's most games; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `avg_longest_drought_sec_rank` | Float64 | Rank of avg_longest_drought_sec within the season (the lowest value ranks 1; ties average), among teams with at least half the season's most games; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `stop_pct_rank` | Float64 | Rank of stop_pct within the season (the highest value ranks 1; ties average), among teams with at least half the season's most games; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `kills_pg_rank` | Float64 | Rank of kills_pg within the season (the highest value ranks 1; ties average), among teams with at least half the season's most games; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `double_kills_pg_rank` | Float64 | Rank of double_kills_pg within the season (the highest value ranks 1; ties average), among teams with at least half the season's most games; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `kills_per_100_rank` | Float64 | Rank of kills_per_100 within the season (the highest value ranks 1; ties average), among teams with at least half the season's most games; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `avg_margin_pct` | Float64 | Percentile of avg_margin among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `avg_wp_pct` | Float64 | Percentile of avg_wp among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `lead_pct_pct` | Float64 | Percentile of lead_pct among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `runs_10_pg_pct` | Float64 | Percentile of runs_10_pg among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_runs_10_pg_pct` | Float64 | Percentile of opp_runs_10_pg among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `avg_longest_drought_sec_pct` | Float64 | Percentile of avg_longest_drought_sec among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `stop_pct_pct` | Float64 | Percentile of stop_pct among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `kills_pg_pct` | Float64 | Percentile of kills_pg among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `double_kills_pg_pct` | Float64 | Percentile of double_kills_pg among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `kills_per_100_pct` | Float64 | Percentile of kills_per_100 among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `avg_margin_n` | Int64 | Sample size of avg_margin: games. |
| `avg_wp_n` | Int64 | Sample size of avg_wp: the games with a win probability. |
| `lead_pct_n` | Int64 | Sample size of lead_pct: games. |
| `runs_10_pg_n` | Int64 | Sample size of runs_10_pg: games. |
| `opp_runs_10_pg_n` | Int64 | Sample size of opp_runs_10_pg: games. |
| `avg_longest_drought_sec_n` | Int64 | Sample size of avg_longest_drought_sec: games. |
| `stop_pct_n` | Int64 | Sample size of stop_pct: the teams defensive possessions. |
| `kills_pg_n` | Int64 | Sample size of kills_pg: the games with published possessions. |
| `double_kills_pg_n` | Int64 | Sample size of double_kills_pg: the games with published possessions. |
| `kills_per_100_n` | Int64 | Sample size of kills_per_100: the teams defensive possessions. |
| `kills_per75` | Float64 | Kills per 75 defensive possessions: 0.75 * kills_per_100 (BB-G8's per-75 basis); null with no possessions. |
| `kills_per75_rank` | Float64 | Rank of kills_per75: kills_per_100_rank, copied (scaling by 0.75 keeps the order). |
| `kills_per75_pct` | Float64 | Percentile of kills_per75: kills_per_100_pct, copied (scaling by 0.75 keeps the order). |
| `kills_per75_n` | Int64 | Sample size of kills_per75: kills_per_100_n, copied. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not `"00"` or `"10"`, or `season_type` is not one of the four above. |

**Example**

```python
from sportsdataverse.nba.nba_game_flow import nba_team_season_flow
nba_team_season_flow(2026).sort("kills_pg_rank").select("team_id", "kills_pg", "avg_wp").head()
```

### nba_team_season_rates {#nba_team_season_rates}

`nba_team_season_rates(seasons: 'int | list[int]', *, league_id: 'str' = '00', season_type: 'str' = 'regular-season', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Season team rates from possessions: ORtg/DRtg/net, pace and the four factors, with splits.

One row per team, season and split (`all`, `home`, `away`, `close`, `clutch`).
Clutch is a possession in play with 5:00 or less left (starting there, or starting before 5:00
and ending after it; one that ends at exactly 5:00 is out) in the 4th quarter or an overtime, the
score within 5 as it starts (stats.nba.com's clutch window); a close game is one with a clutch
possession. For the WNBA (`league_id="10"`) through 2005, two 20-minute halves, the regulation
window is the last 5:00 of the 2nd half; overtime counts as above. The full definitions are in
`sportsdataverse._common.team_season_rates`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26). The WNBA season is its calendar year. |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA. |
| `season_type` | `str` | `'regular-season'` | `"regular-season"` (default), `"preseason"`, `"playoffs"` or `"play-in"`, matched to the schedule's `season_type`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, team_id, split)`. `team_id` is the stats.nba.com team id (Int64). `games`, `poss` / `opp_poss` (offensive / defensive possessions) and `pts` / `opp_pts` are the split's totals. The twelve rates are `off_rtg`, `def_rtg`, `net_rtg` (per 100 possessions), `pace` (possessions per 48 minutes, 40 in the WNBA), the four factors `efg_pct`, `tov_pct`, `orb_pct`, `ft_rate` and the same four against (`opp_*`), all fractions. Each rate has `<m>_rank` (1 = best; `def_rtg`, `tov_pct`, `opp_efg_pct`, `opp_orb_pct` and `opp_ft_rate` rank low first; pace ranks high first), `<m>_pct` (the Weibull percentile) and `<m>_n` (the sample: possessions for the ratings, the clock in regulation games for pace, field-goal attempts for eFG% and FT rate, possessions for turnover rate, rebound chances for ORB%). Ranks are within the season and split, among teams with at least half the most games whose `<m>_n` reaches the league's `~sportsdataverse._common.rate_gates.TEAM_MIN_N` gate (a clutch split reaches none), and null under five teams. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2026 is 2025-26). |
| `team_id` | Int64 | stats.nba.com team id (the 10-digit franchise id). |
| `split` | String | Which possessions the row covers: all; home or away (the team's home or road games; neutral-site games are in neither: the curated nba_neutral_site_games list, plus every game dated inside the 2020 bubble (from the schedule's game_date)); close (every possession of a game that had a clutch possession); clutch (a possession in play with 5:00 or less left (starting there, or starting before 5:00 and ending after it) in the 4th quarter (the 2nd half for league_id='10' through 2005, when the WNBA played two 20-minute halves) or an overtime, the score within 5 as it starts). |
| `games` | Int64 | Games with at least one possession in the split. |
| `poss` | Int64 | Offensive possessions in the split, counted from possession rows. |
| `opp_poss` | Int64 | Defensive possessions in the split (the opponents' offensive possessions). |
| `pts` | Int64 | Points scored on the team's offensive possessions in the split. Counted from each possession's made shots and free throws. |
| `opp_pts` | Int64 | Points allowed on the team's defensive possessions in the split. |
| `off_rtg` | Float64 | Offensive rating: points scored per 100 offensive possessions. |
| `def_rtg` | Float64 | Defensive rating: points allowed per 100 defensive possessions. |
| `net_rtg` | Float64 | Net rating: off_rtg minus def_rtg. |
| `pace` | Float64 | Pace: possessions per 48 minutes (40 with league_id="10", the WNBA), the mean of poss and opp_poss over the game clock those possessions used. |
| `efg_pct` | Float64 | Effective field-goal percentage, (FGM + 0.5 * 3PM) / FGA, as a fraction. |
| `tov_pct` | Float64 | Turnovers per offensive possession, as a fraction. |
| `orb_pct` | Float64 | Offensive rebound rate: offensive rebounds over offensive-rebound chances (own OREB plus the defense's DREB), as a fraction. |
| `ft_rate` | Float64 | Free-throw rate, FTA / FGA. |
| `opp_efg_pct` | Float64 | Opponents' effective field-goal percentage, (FGM + 0.5 * 3PM) / FGA, as a fraction. |
| `opp_tov_pct` | Float64 | Opponents' turnovers per possession (turnovers forced), as a fraction. |
| `opp_orb_pct` | Float64 | Opponents' offensive rebound rate, as a fraction (one minus the team's defensive rebound rate). |
| `opp_ft_rate` | Float64 | Opponents' free-throw rate, FTA / FGA. |
| `off_rtg_rank` | Float64 | Rank of off_rtg within the season and split (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the split, whose off_rtg_n is at least 1,300 offensive possessions (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `def_rtg_rank` | Float64 | Rank of def_rtg within the season and split (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the split, whose def_rtg_n is at least 2,000 defensive possessions (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `net_rtg_rank` | Float64 | Rank of net_rtg within the season and split (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the split, whose net_rtg_n is at least 2,000 possessions (offense plus defense) (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `pace_rank` | Float64 | Rank of pace within the season and split (the highest value ranks 1; pace has no better direction; ties average), among teams that played at least half the most games any team played in the split, whose pace_n is at least 5 games (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `efg_pct_rank` | Float64 | Rank of efg_pct within the season and split (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the split, whose efg_pct_n is at least 1,200 field-goal attempts (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `tov_pct_rank` | Float64 | Rank of tov_pct within the season and split (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the split, whose tov_pct_n is at least 1,400 offensive possessions (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `orb_pct_rank` | Float64 | Rank of orb_pct within the season and split (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the split, whose orb_pct_n is at least 410 offensive-rebound chances (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `ft_rate_rank` | Float64 | Rank of ft_rate within the season and split (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the split, whose ft_rate_n is at least 1,300 field-goal attempts (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `opp_efg_pct_rank` | Float64 | Rank of opp_efg_pct within the season and split (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the split, whose opp_efg_pct_n is at least 1,900 opponent field-goal attempts (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `opp_tov_pct_rank` | Float64 | Rank of opp_tov_pct within the season and split (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the split, whose opp_tov_pct_n is at least 960 defensive possessions (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `opp_orb_pct_rank` | Float64 | Rank of opp_orb_pct within the season and split (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the split, whose opp_orb_pct_n is at least 1,200 opponent offensive-rebound chances (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `opp_ft_rate_rank` | Float64 | Rank of opp_ft_rate within the season and split (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the split, whose opp_ft_rate_n is at least 1,500 opponent field-goal attempts (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
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
| `pace_n` | Int64 | Sample size of pace: the game clock the split's possessions used, in regulation games (48 minutes, 40 in the WNBA and college), rounded: a full game counts 1, a clutch split's five-minute windows about 0.1 each. |
| `efg_pct_n` | Int64 | Sample size of efg_pct: the team's field-goal attempts in the split. |
| `tov_pct_n` | Int64 | Sample size of tov_pct: the team's offensive possessions in the split. |
| `orb_pct_n` | Int64 | Sample size of orb_pct: the team's offensive-rebound chances in the split. |
| `ft_rate_n` | Int64 | Sample size of ft_rate: the team's field-goal attempts in the split. |
| `opp_efg_pct_n` | Int64 | Sample size of opp_efg_pct: the opponents' field-goal attempts in the split. |
| `opp_tov_pct_n` | Int64 | Sample size of opp_tov_pct: the team's defensive possessions in the split. |
| `opp_orb_pct_n` | Int64 | Sample size of opp_orb_pct: the opponents' offensive-rebound chances in the split. |
| `opp_ft_rate_n` | Int64 | Sample size of opp_ft_rate: the opponents' field-goal attempts in the split. |
| `lg_ft_pct` | Float64 | League free-throw percentage in the season and split: every team's FTM over every team's FTA, so free throws can be priced without a second read. |
| `plus_minus` | Int64 | Point margin in the split: pts - opp_pts. |
| `pts_pg` | Float64 | Per game: points scored divided by games (BB-G8's per-game basis). |
| `opp_pts_pg` | Float64 | Per game: points allowed divided by games (BB-G8's per-game basis). |
| `plus_minus_pg` | Float64 | Per game: point margin (pts - opp_pts) divided by games (BB-G8's per-game basis). |
| `pts_per75` | Float64 | Per 75 possessions: 0.75 * off_rtg (BB-G8's per-75 basis). |
| `opp_pts_per75` | Float64 | Per 75 possessions: 0.75 * def_rtg (BB-G8's per-75 basis). |
| `plus_minus_per75` | Float64 | Per 75 possessions: 0.75 * net_rtg (BB-G8's per-75 basis). |
| `pts_pg_rank` | Float64 | Rank of pts_pg within the season and split (the highest value ranks 1; ties average), ranked where off_rtg is: among the teams off_rtg_rank qualifies (half the split's most games, and off_rtg_n at least off_rtg's rate_gates gate); null otherwise or with fewer than five qualifiers. Per game is not per possession, so pace reorders it against off_rtg. |
| `opp_pts_pg_rank` | Float64 | Rank of opp_pts_pg within the season and split (the lowest value ranks 1; ties average), ranked where def_rtg is: among the teams def_rtg_rank qualifies (half the split's most games, and def_rtg_n at least def_rtg's rate_gates gate); null otherwise or with fewer than five qualifiers. Per game is not per possession, so pace reorders it against def_rtg. |
| `plus_minus_pg_rank` | Float64 | Rank of plus_minus_pg within the season and split (the highest value ranks 1; ties average), ranked where net_rtg is: among the teams net_rtg_rank qualifies (half the split's most games, and net_rtg_n at least net_rtg's rate_gates gate); null otherwise or with fewer than five qualifiers. Per game is not per possession, so pace reorders it against net_rtg. |
| `pts_per75_rank` | Float64 | Rank of pts_per75: off_rtg_rank, copied (scaling by 0.75 keeps the order: same gate, same ties). |
| `opp_pts_per75_rank` | Float64 | Rank of opp_pts_per75: def_rtg_rank, copied (scaling by 0.75 keeps the order: same gate, same ties). |
| `plus_minus_per75_rank` | Float64 | Rank of plus_minus_per75: net_rtg_rank, copied (scaling by 0.75 keeps the order: same gate, same ties). |
| `pts_pg_pct` | Float64 | Percentile of pts_pg among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_pts_pg_pct` | Float64 | Percentile of opp_pts_pg among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `plus_minus_pg_pct` | Float64 | Percentile of plus_minus_pg among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `pts_per75_pct` | Float64 | Percentile of pts_per75: off_rtg_pct, copied (scaling by 0.75 keeps the order). |
| `opp_pts_per75_pct` | Float64 | Percentile of opp_pts_per75: def_rtg_pct, copied (scaling by 0.75 keeps the order). |
| `plus_minus_per75_pct` | Float64 | Percentile of plus_minus_per75: net_rtg_pct, copied (scaling by 0.75 keeps the order). |
| `pts_pg_n` | Int64 | Sample size of pts_pg: off_rtg_n, the rating's own sample. |
| `opp_pts_pg_n` | Int64 | Sample size of opp_pts_pg: def_rtg_n, the rating's own sample. |
| `plus_minus_pg_n` | Int64 | Sample size of plus_minus_pg: net_rtg_n, the rating's own sample. |
| `pts_per75_n` | Int64 | Sample size of pts_per75: off_rtg_n, copied. |
| `opp_pts_per75_n` | Int64 | Sample size of opp_pts_per75: def_rtg_n, copied. |
| `plus_minus_per75_n` | Int64 | Sample size of plus_minus_per75: net_rtg_n, copied. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not `"00"` or `"10"`, or `season_type` is not one of the four above. |

**Example**

```python
from sportsdataverse.nba.nba_team_season_rates import nba_team_season_rates
rates = nba_team_season_rates(2026)
rates.filter(split="all").sort("net_rtg_rank").head()

# WNBA via ``league_id``

wnba = nba_team_season_rates(2025, league_id="10")
```

### nba_team_shot_zones {#nba_team_shot_zones}

`nba_team_shot_zones(seasons: 'int | list[int]', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Season shot profile by zone per team, for and allowed: rim / mid / corner 3 / above-the-break 3.

One row per team and season over the regular season: the team's own attempts and, with an
`opp_` prefix, its opponents' attempts against it.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26). The WNBA season is its calendar year. |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, team_id)` (the stats.nba.com team id, Int64) with `games` and the columns of `nba_player_shot_zones` for the team and again with an `opp_` prefix for its opponents (`opp_fga`, `opp_rim_fg_pct`, ...), each rate with `<m>_n`, `<m>_rank` and `<m>_pct`, ranked within the season among teams whose `<m>_n` reaches the gate and null under five teams. `<m>_rank` is 1 = highest, except the allowed `opp_{z}_fg_pct`, where 1 is the lowest (the stingiest defense). Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2026 is 2025-26). |
| `team_id` | Int64 | stats.nba.com team id. |
| `games` | Int64 | The team's regular-season games with a field-goal attempt by or against it. |
| `fga` | Int64 | Field-goal attempts by the team (regular-season games). |
| `fgm` | Int64 | Field goals made by the team. |
| `unlocated_fga` | Int64 | Attempts by the team with no real location (the WNBA's 1997-2010 hoop-centre placeholders; 0 elsewhere): counted in fga, in no zone. |
| `rim_fga` | Int64 | Attempts by the team at the rim (the restricted area: shot_distance under 4 ft). |
| `rim_fgm` | Int64 | Makes by the team at the rim (the restricted area: shot_distance under 4 ft). |
| `rim_ast` | Int64 | Assisted makes by the team at the rim (the restricted area: shot_distance under 4 ft). |
| `mid_fga` | Int64 | Attempts by the team in the mid zone (every other two: the paint outside the restricted area and the mid-range). |
| `mid_fgm` | Int64 | Makes by the team in the mid zone (every other two: the paint outside the restricted area and the mid-range). |
| `mid_ast` | Int64 | Assisted makes by the team in the mid zone (every other two: the paint outside the restricted area and the mid-range). |
| `corner_3_fga` | Int64 | Attempts by the team from the corner (a three from the baseline band, y_legacy <= 87.5, below the break). |
| `corner_3_fgm` | Int64 | Makes by the team from the corner (a three from the baseline band, y_legacy <= 87.5, below the break). |
| `corner_3_ast` | Int64 | Assisted makes by the team from the corner (a three from the baseline band, y_legacy <= 87.5, below the break). |
| `above_break_3_fga` | Int64 | Attempts by the team from above the break (every other three). |
| `above_break_3_fgm` | Int64 | Makes by the team from above the break (every other three). |
| `above_break_3_ast` | Int64 | Assisted makes by the team from above the break (every other three). |
| `opp_fga` | Int64 | Allowed: Field-goal attempts by the team's opponents (regular-season games). |
| `opp_fgm` | Int64 | Allowed: Field goals made by the team's opponents. |
| `opp_unlocated_fga` | Int64 | Allowed: Attempts by the team's opponents with no real location (the WNBA's 1997-2010 hoop-centre placeholders; 0 elsewhere): counted in opp_fga, in no zone. |
| `opp_rim_fga` | Int64 | Allowed: Attempts by the team's opponents at the rim (the restricted area: shot_distance under 4 ft). |
| `opp_rim_fgm` | Int64 | Allowed: Makes by the team's opponents at the rim (the restricted area: shot_distance under 4 ft). |
| `opp_rim_ast` | Int64 | Allowed: Assisted makes by the team's opponents at the rim (the restricted area: shot_distance under 4 ft). |
| `opp_mid_fga` | Int64 | Allowed: Attempts by the team's opponents in the mid zone (every other two: the paint outside the restricted area and the mid-range). |
| `opp_mid_fgm` | Int64 | Allowed: Makes by the team's opponents in the mid zone (every other two: the paint outside the restricted area and the mid-range). |
| `opp_mid_ast` | Int64 | Allowed: Assisted makes by the team's opponents in the mid zone (every other two: the paint outside the restricted area and the mid-range). |
| `opp_corner_3_fga` | Int64 | Allowed: Attempts by the team's opponents from the corner (a three from the baseline band, y_legacy <= 87.5, below the break). |
| `opp_corner_3_fgm` | Int64 | Allowed: Makes by the team's opponents from the corner (a three from the baseline band, y_legacy <= 87.5, below the break). |
| `opp_corner_3_ast` | Int64 | Allowed: Assisted makes by the team's opponents from the corner (a three from the baseline band, y_legacy <= 87.5, below the break). |
| `opp_above_break_3_fga` | Int64 | Allowed: Attempts by the team's opponents from above the break (every other three). |
| `opp_above_break_3_fgm` | Int64 | Allowed: Makes by the team's opponents from above the break (every other three). |
| `opp_above_break_3_ast` | Int64 | Allowed: Assisted makes by the team's opponents from above the break (every other three). |
| `rim_fg_pct` | Float64 | rim_fgm / rim_fga; null with no attempt. |
| `rim_freq` | Float64 | rim_fga / (fga - unlocated_fga): the zone's share of the located attempts (the four zones add up to 1); null with none. |
| `rim_ast_pct` | Float64 | Share of rim_fgm that were assisted, over the makes (every make's assist is in its description); null with none. |
| `mid_fg_pct` | Float64 | mid_fgm / mid_fga; null with no attempt. |
| `mid_freq` | Float64 | mid_fga / (fga - unlocated_fga): the zone's share of the located attempts (the four zones add up to 1); null with none. |
| `mid_ast_pct` | Float64 | Share of mid_fgm that were assisted, over the makes (every make's assist is in its description); null with none. |
| `corner_3_fg_pct` | Float64 | corner_3_fgm / corner_3_fga; null with no attempt. |
| `corner_3_freq` | Float64 | corner_3_fga / (fga - unlocated_fga): the zone's share of the located attempts (the four zones add up to 1); null with none. |
| `corner_3_ast_pct` | Float64 | Share of corner_3_fgm that were assisted, over the makes (every make's assist is in its description); null with none. |
| `above_break_3_fg_pct` | Float64 | above_break_3_fgm / above_break_3_fga; null with no attempt. |
| `above_break_3_freq` | Float64 | above_break_3_fga / (fga - unlocated_fga): the zone's share of the located attempts (the four zones add up to 1); null with none. |
| `above_break_3_ast_pct` | Float64 | Share of above_break_3_fgm that were assisted, over the makes (every make's assist is in its description); null with none. |
| `opp_rim_fg_pct` | Float64 | Allowed: opp_rim_fgm / opp_rim_fga; null with no attempt. |
| `opp_rim_freq` | Float64 | Allowed: opp_rim_fga / (opp_fga - opp_unlocated_fga): the zone's share of the located attempts (the four zones add up to 1); null with none. |
| `opp_rim_ast_pct` | Float64 | Allowed: Share of opp_rim_fgm that were assisted, over the makes (every make's assist is in its description); null with none. |
| `opp_mid_fg_pct` | Float64 | Allowed: opp_mid_fgm / opp_mid_fga; null with no attempt. |
| `opp_mid_freq` | Float64 | Allowed: opp_mid_fga / (opp_fga - opp_unlocated_fga): the zone's share of the located attempts (the four zones add up to 1); null with none. |
| `opp_mid_ast_pct` | Float64 | Allowed: Share of opp_mid_fgm that were assisted, over the makes (every make's assist is in its description); null with none. |
| `opp_corner_3_fg_pct` | Float64 | Allowed: opp_corner_3_fgm / opp_corner_3_fga; null with no attempt. |
| `opp_corner_3_freq` | Float64 | Allowed: opp_corner_3_fga / (opp_fga - opp_unlocated_fga): the zone's share of the located attempts (the four zones add up to 1); null with none. |
| `opp_corner_3_ast_pct` | Float64 | Allowed: Share of opp_corner_3_fgm that were assisted, over the makes (every make's assist is in its description); null with none. |
| `opp_above_break_3_fg_pct` | Float64 | Allowed: opp_above_break_3_fgm / opp_above_break_3_fga; null with no attempt. |
| `opp_above_break_3_freq` | Float64 | Allowed: opp_above_break_3_fga / (opp_fga - opp_unlocated_fga): the zone's share of the located attempts (the four zones add up to 1); null with none. |
| `opp_above_break_3_ast_pct` | Float64 | Allowed: Share of opp_above_break_3_fgm that were assisted, over the makes (every make's assist is in its description); null with none. |
| `rim_fg_pct_n` | Int64 | Sample size of rim_fg_pct: rim_fga. |
| `rim_freq_n` | Int64 | Sample size of rim_freq: located attempts (fga - unlocated_fga). |
| `rim_ast_pct_n` | Int64 | Sample size of rim_ast_pct: makes in the zone. |
| `mid_fg_pct_n` | Int64 | Sample size of mid_fg_pct: mid_fga. |
| `mid_freq_n` | Int64 | Sample size of mid_freq: located attempts (fga - unlocated_fga). |
| `mid_ast_pct_n` | Int64 | Sample size of mid_ast_pct: makes in the zone. |
| `corner_3_fg_pct_n` | Int64 | Sample size of corner_3_fg_pct: corner_3_fga. |
| `corner_3_freq_n` | Int64 | Sample size of corner_3_freq: located attempts (fga - unlocated_fga). |
| `corner_3_ast_pct_n` | Int64 | Sample size of corner_3_ast_pct: makes in the zone. |
| `above_break_3_fg_pct_n` | Int64 | Sample size of above_break_3_fg_pct: above_break_3_fga. |
| `above_break_3_freq_n` | Int64 | Sample size of above_break_3_freq: located attempts (fga - unlocated_fga). |
| `above_break_3_ast_pct_n` | Int64 | Sample size of above_break_3_ast_pct: makes in the zone. |
| `opp_rim_fg_pct_n` | Int64 | Sample size of opp_rim_fg_pct: opp_rim_fga. |
| `opp_rim_freq_n` | Int64 | Sample size of opp_rim_freq: located attempts (opp_fga - opp_unlocated_fga). |
| `opp_rim_ast_pct_n` | Int64 | Sample size of opp_rim_ast_pct: makes in the zone. |
| `opp_mid_fg_pct_n` | Int64 | Sample size of opp_mid_fg_pct: opp_mid_fga. |
| `opp_mid_freq_n` | Int64 | Sample size of opp_mid_freq: located attempts (opp_fga - opp_unlocated_fga). |
| `opp_mid_ast_pct_n` | Int64 | Sample size of opp_mid_ast_pct: makes in the zone. |
| `opp_corner_3_fg_pct_n` | Int64 | Sample size of opp_corner_3_fg_pct: opp_corner_3_fga. |
| `opp_corner_3_freq_n` | Int64 | Sample size of opp_corner_3_freq: located attempts (opp_fga - opp_unlocated_fga). |
| `opp_corner_3_ast_pct_n` | Int64 | Sample size of opp_corner_3_ast_pct: makes in the zone. |
| `opp_above_break_3_fg_pct_n` | Int64 | Sample size of opp_above_break_3_fg_pct: opp_above_break_3_fga. |
| `opp_above_break_3_freq_n` | Int64 | Sample size of opp_above_break_3_freq: located attempts (opp_fga - opp_unlocated_fga). |
| `opp_above_break_3_ast_pct_n` | Int64 | Sample size of opp_above_break_3_ast_pct: makes in the zone. |
| `rim_fg_pct_rank` | Float64 | Rank of rim_fg_pct within the season (the highest ranks 1; ties average), among teams whose rim_fg_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `rim_freq_rank` | Float64 | Rank of rim_freq within the season (the highest ranks 1; ties average), among teams whose rim_freq_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `rim_ast_pct_rank` | Float64 | Rank of rim_ast_pct within the season (the highest ranks 1; ties average), among teams whose rim_ast_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `mid_fg_pct_rank` | Float64 | Rank of mid_fg_pct within the season (the highest ranks 1; ties average), among teams whose mid_fg_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `mid_freq_rank` | Float64 | Rank of mid_freq within the season (the highest ranks 1; ties average), among teams whose mid_freq_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `mid_ast_pct_rank` | Float64 | Rank of mid_ast_pct within the season (the highest ranks 1; ties average), among teams whose mid_ast_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `corner_3_fg_pct_rank` | Float64 | Rank of corner_3_fg_pct within the season (the highest ranks 1; ties average), among teams whose corner_3_fg_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `corner_3_freq_rank` | Float64 | Rank of corner_3_freq within the season (the highest ranks 1; ties average), among teams whose corner_3_freq_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `corner_3_ast_pct_rank` | Float64 | Rank of corner_3_ast_pct within the season (the highest ranks 1; ties average), among teams whose corner_3_ast_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `above_break_3_fg_pct_rank` | Float64 | Rank of above_break_3_fg_pct within the season (the highest ranks 1; ties average), among teams whose above_break_3_fg_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `above_break_3_freq_rank` | Float64 | Rank of above_break_3_freq within the season (the highest ranks 1; ties average), among teams whose above_break_3_freq_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `above_break_3_ast_pct_rank` | Float64 | Rank of above_break_3_ast_pct within the season (the highest ranks 1; ties average), among teams whose above_break_3_ast_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `opp_rim_fg_pct_rank` | Float64 | Rank of opp_rim_fg_pct within the season (the lowest ranks 1, the stingiest defense; ties average), among teams whose opp_rim_fg_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `opp_rim_freq_rank` | Float64 | Rank of opp_rim_freq within the season (the highest ranks 1; ties average), among teams whose opp_rim_freq_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `opp_rim_ast_pct_rank` | Float64 | Rank of opp_rim_ast_pct within the season (the highest ranks 1; ties average), among teams whose opp_rim_ast_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `opp_mid_fg_pct_rank` | Float64 | Rank of opp_mid_fg_pct within the season (the lowest ranks 1, the stingiest defense; ties average), among teams whose opp_mid_fg_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `opp_mid_freq_rank` | Float64 | Rank of opp_mid_freq within the season (the highest ranks 1; ties average), among teams whose opp_mid_freq_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `opp_mid_ast_pct_rank` | Float64 | Rank of opp_mid_ast_pct within the season (the highest ranks 1; ties average), among teams whose opp_mid_ast_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `opp_corner_3_fg_pct_rank` | Float64 | Rank of opp_corner_3_fg_pct within the season (the lowest ranks 1, the stingiest defense; ties average), among teams whose opp_corner_3_fg_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `opp_corner_3_freq_rank` | Float64 | Rank of opp_corner_3_freq within the season (the highest ranks 1; ties average), among teams whose opp_corner_3_freq_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `opp_corner_3_ast_pct_rank` | Float64 | Rank of opp_corner_3_ast_pct within the season (the highest ranks 1; ties average), among teams whose opp_corner_3_ast_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `opp_above_break_3_fg_pct_rank` | Float64 | Rank of opp_above_break_3_fg_pct within the season (the lowest ranks 1, the stingiest defense; ties average), among teams whose opp_above_break_3_fg_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `opp_above_break_3_freq_rank` | Float64 | Rank of opp_above_break_3_freq within the season (the highest ranks 1; ties average), among teams whose opp_above_break_3_freq_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `opp_above_break_3_ast_pct_rank` | Float64 | Rank of opp_above_break_3_ast_pct within the season (the highest ranks 1; ties average), among teams whose opp_above_break_3_ast_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `rim_fg_pct_pct` | Float64 | Percentile of rim_fg_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `rim_freq_pct` | Float64 | Percentile of rim_freq within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `rim_ast_pct_pct` | Float64 | Percentile of rim_ast_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `mid_fg_pct_pct` | Float64 | Percentile of mid_fg_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `mid_freq_pct` | Float64 | Percentile of mid_freq within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `mid_ast_pct_pct` | Float64 | Percentile of mid_ast_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `corner_3_fg_pct_pct` | Float64 | Percentile of corner_3_fg_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `corner_3_freq_pct` | Float64 | Percentile of corner_3_freq within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `corner_3_ast_pct_pct` | Float64 | Percentile of corner_3_ast_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `above_break_3_fg_pct_pct` | Float64 | Percentile of above_break_3_fg_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `above_break_3_freq_pct` | Float64 | Percentile of above_break_3_freq within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `above_break_3_ast_pct_pct` | Float64 | Percentile of above_break_3_ast_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_rim_fg_pct_pct` | Float64 | Percentile of opp_rim_fg_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_rim_freq_pct` | Float64 | Percentile of opp_rim_freq within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_rim_ast_pct_pct` | Float64 | Percentile of opp_rim_ast_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_mid_fg_pct_pct` | Float64 | Percentile of opp_mid_fg_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_mid_freq_pct` | Float64 | Percentile of opp_mid_freq within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_mid_ast_pct_pct` | Float64 | Percentile of opp_mid_ast_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_corner_3_fg_pct_pct` | Float64 | Percentile of opp_corner_3_fg_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_corner_3_freq_pct` | Float64 | Percentile of opp_corner_3_freq within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_corner_3_ast_pct_pct` | Float64 | Percentile of opp_corner_3_ast_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_above_break_3_fg_pct_pct` | Float64 | Percentile of opp_above_break_3_fg_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_above_break_3_freq_pct` | Float64 | Percentile of opp_above_break_3_freq within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_above_break_3_ast_pct_pct` | Float64 | Percentile of opp_above_break_3_ast_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not `"00"` or `"10"` (checked before any data is loaded). |

**Example**

```python
from sportsdataverse.nba.nba_shot_zone_profiles import nba_team_shot_zones
teams = nba_team_shot_zones(2025)
teams.sort("opp_rim_freq").select("team_id", "opp_rim_freq", "opp_rim_fg_pct").head()

# WNBA via ``league_id``

wnba = nba_team_shot_zones(2025, league_id="10")
```
