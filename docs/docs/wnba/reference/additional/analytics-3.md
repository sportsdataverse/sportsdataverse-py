---
title: "WNBA — additional Python functions — Analytics: wnba_player–wnba_situational"
sidebar_label: "Analytics: wnba_player–wnba_situational"
sidebar_position: 9
description: "WNBA — additional Python functions — Analytics: wnba_player–wnba_situational — function reference in sdv-py, the SportsDataverse Python package."
---
# WNBA — additional Python functions — Analytics: wnba_player–wnba_situational

### wnba_player_props {#wnba_player_props}

`wnba_player_props(season: 'int', game_id: 'str', home_team_id: 'str', away_team_id: 'str', *, league_id: 'str' = '10', as_of_date: 'Union[dt.date, None]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

WNBA player props (league_id='10'). See sportsdataverse.nba.nba_player_props.nba_player_props.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  |  |
| `game_id` | `str` |  |  |
| `home_team_id` | `str` |  |  |
| `away_team_id` | `str` |  |  |
| `league_id` | `str` | `'10'` |  |
| `as_of_date` | `Union[date, None]` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**

One row per player on either team: `player_id, team_id`, `stat_<stat>_exp` and `stat_<stat>_recent_exp` for the nine stats (null if the logs lack one), `recent_weight, minutes_exp, pace_proj`. `team_id` is his latest team (All-Star games left out). Lines assume he plays (rate x mean minutes when playing x pace); the recent line and weight feed the `prob_over` mixture (weight 0 with <= 10 earlier games, null outside the NBA). `minutes_exp` is his share of his team's logged minutes x `5 * game_minutes`. A team with no game before the cut has no rows and the league pace; empty input gives that schema.

| col_name | type | description |
|---|---|---|
| `player_id` | String | ESPN athlete id (Utf8). |
| `team_id` | String | ESPN team id (Utf8) of the player's team in his latest game with positive minutes before the as-of cut (All-Star exhibition games left out), so a traded player projects for his new team. |
| `stat_pts_exp` | Float64 | Expected points if he plays, per-minute rate (shrunk toward his minutes-and-role tier's) x mean minutes when playing x the pace factor. |
| `stat_reb_exp` | Float64 | Expected rebounds if he plays, on the same basis. |
| `stat_ast_exp` | Float64 | Expected assists if he plays, on the same basis. |
| `stat_fg3m_exp` | Float64 | Expected made threes if he plays, on the same basis. |
| `minutes_exp` | Float64 | Expected minutes: the player's minutes for his current team over the summed minutes for that team of every player returned on it (minutes a player since traded away logged for it are left out), all before the as-of cut (a game sat counts as zero), x 200; a team's rows sum to 200. The stat_*_exp lines assume the player plays; this does not. |
| `pace_proj` | Float64 | Projected game possessions from the two teams' adjusted paces; the league-norm pace stands in for a team with no ratings row yet. |
| `stat_stl_exp` | Float64 | Expected steals if he plays, on the same basis; null when the box logs carry no steals. |
| `stat_blk_exp` | Float64 | Expected blocks if he plays, on the same basis; null when the box logs carry no blocks. |
| `stat_tov_exp` | Float64 | Expected turnovers if he plays, on the same basis; null when the box logs carry no turnovers. |
| `stat_fgm_exp` | Float64 | Expected made field goals if he plays, on the same basis; null when the box logs carry none. |
| `stat_ftm_exp` | Float64 | Expected made free throws if he plays, on the same basis; null when the box logs carry none. |
| `stat_pts_recent_exp` | Float64 | Expected points if he plays, from his last 10 games (rates shrunk toward the same tier prior) x his last-10 mean minutes x the pace factor; the mixture's second line. Null in the WNBA (no fitted mixture). |
| `stat_reb_recent_exp` | Float64 | Expected rebounds if he plays, from his last 10 games (rates shrunk toward the same tier prior) x his last-10 mean minutes x the pace factor; the mixture's second line. Null in the WNBA (no fitted mixture). |
| `stat_ast_recent_exp` | Float64 | Expected assists if he plays, from his last 10 games (rates shrunk toward the same tier prior) x his last-10 mean minutes x the pace factor; the mixture's second line. Null in the WNBA (no fitted mixture). |
| `stat_fg3m_recent_exp` | Float64 | Expected made threes if he plays, from his last 10 games (rates shrunk toward the same tier prior) x his last-10 mean minutes x the pace factor; the mixture's second line. Null in the WNBA (no fitted mixture). |
| `stat_stl_recent_exp` | Float64 | Expected steals if he plays, from his last 10 games (rates shrunk toward the same tier prior) x his last-10 mean minutes x the pace factor; the mixture's second line. Null in the WNBA (no fitted mixture). |
| `stat_blk_recent_exp` | Float64 | Expected blocks if he plays, from his last 10 games (rates shrunk toward the same tier prior) x his last-10 mean minutes x the pace factor; the mixture's second line. Null in the WNBA (no fitted mixture). |
| `stat_tov_recent_exp` | Float64 | Expected turnovers if he plays, from his last 10 games (rates shrunk toward the same tier prior) x his last-10 mean minutes x the pace factor; the mixture's second line. Null in the WNBA (no fitted mixture). |
| `stat_fgm_recent_exp` | Float64 | Expected field goals made if he plays, from his last 10 games (rates shrunk toward the same tier prior) x his last-10 mean minutes x the pace factor; the mixture's second line. Null in the WNBA (no fitted mixture). |
| `stat_ftm_recent_exp` | Float64 | Expected free throws made if he plays, from his last 10 games (rates shrunk toward the same tier prior) x his last-10 mean minutes x the pace factor; the mixture's second line. Null in the WNBA (no fitted mixture). |
| `recent_weight` | Float64 | Weight on the recent line in the prop mixture, logistic(a + b x \|last-10 minus season mean minutes\| / his season minutes SD); 0 with 10 or fewer earlier games, where the two windows coincide. Null in the WNBA (no fitted mixture). |

### wnba_player_season_box_rates {#wnba_player_season_box_rates}

`wnba_player_season_box_rates(seasons: 'int | list[int]', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

WNBA per-36 points, rebounds and assists per player-season, ranked, with the season usage.

Over the regular season (a playoff or play-in game is measured against it), from ESPN's player box. `pts_per36` / `reb_per36` /
`ast_per36` are 36 x the total / minutes, each with `_n` (the minutes) and
`_rank` / `_pct` among the season's players whose minutes reach the rate's gate (the
minutes at which it is half signal on odd / even games). `usg_pct` is the season usage
(FGA + 0.44 FTA + TOV over the team's while on the floor), not ranked.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (2025 is the 2025 season). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, athlete_id)` (the ESPN id): `team_id` (his last game's team), `games`, `minutes`, `pts` / `reb` / `ast`, `usg_pct` with `usg_pct_n`, and the three per-36 rates with `_n` / `_rank` / `_pct`. A season without a release returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `athlete_id` | Int64 | ESPN athlete id of the player, the join key to the player box score. |
| `team_id` | Int64 | ESPN team id of the player's last game of the season (a traded player has one row). |
| `games` | Int64 | Games played (the pros' regular season, every college game). |
| `minutes` | Float64 | Minutes played in those games. |
| `pts` | Int64 | Points in those games. |
| `reb` | Int64 | Rebounds in those games. |
| `ast` | Int64 | Assists in those games. |
| `usg_pct` | Float64 | Season usage: 100 x sum(uses) / sum(minutes x team uses / (team minutes / 5)), uses = FGA + 0.44 FTA + TOV; not ranked. |
| `usg_pct_n` | Int64 | Minutes of the games with a usage behind usg_pct. |
| `pts_per36` | Float64 | Points per 36 minutes: 36 x pts / minutes. |
| `pts_per36_n` | Int64 | Minutes behind pts_per36. |
| `reb_per36` | Float64 | Rebounds per 36 minutes: 36 x reb / minutes. |
| `reb_per36_n` | Int64 | Minutes behind reb_per36. |
| `ast_per36` | Float64 | Assists per 36 minutes: 36 x ast / minutes. |
| `ast_per36_n` | Int64 | Minutes behind ast_per36. |
| `pts_per36_rank` | Float64 | Rank of pts_per36 in the league-season (1 best, average ties) among players whose minutes reach its gate (split-half r = 0.5); null under the gate or with fewer than 10 qualifiers. |
| `pts_per36_pct` | Float64 | Percentile of pts_per36 among the same qualifiers, 100 (n + 1 - rank) / (n + 1). |
| `reb_per36_rank` | Float64 | Rank of reb_per36 in the league-season (1 best, average ties) among players whose minutes reach its gate (split-half r = 0.5); null under the gate or with fewer than 10 qualifiers. |
| `reb_per36_pct` | Float64 | Percentile of reb_per36 among the same qualifiers, 100 (n + 1 - rank) / (n + 1). |
| `ast_per36_rank` | Float64 | Rank of ast_per36 in the league-season (1 best, average ties) among players whose minutes reach its gate (split-half r = 0.5); null under the gate or with fewer than 10 qualifiers. |
| `ast_per36_pct` | Float64 | Percentile of ast_per36 among the same qualifiers, 100 (n + 1 - rank) / (n + 1). |

**Example**

```python
from sportsdataverse.wnba.wnba_player_box_expected import wnba_player_season_box_rates
rates = wnba_player_season_box_rates(2025)
rates.sort("pts_per36_rank").select("athlete_id", "minutes", "pts_per36", "pts_per36_pct").head()
```

### wnba_player_shot_zones {#wnba_player_shot_zones}

`wnba_player_shot_zones(seasons: 'int | list[int]', *, league_id: 'str' = '10', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

WNBA season shot profile by zone per player: rim / mid / corner 3 / above-the-break 3 (league_id='10'). See sportsdataverse.nba.nba_shot_zone_profiles.nba_player_shot_zones.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  |  |
| `league_id` | `str` | `'10'` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**

One row per `(season, team_id, player_id)` (stats.nba.com ids, Int64) with `player_name`, `games`, `fga`, `fgm`, `unlocated_fga` and, for each zone `z` (`rim`, `mid`, `corner_3`, `above_break_3`), `{z}_fga`, `{z}_fgm`, `{z}_ast` (assisted makes), `{z}_fg_pct`, `{z}_freq` (share of the located attempts) and `{z}_ast_pct` (share of the makes assisted). The four `{z}_fga` plus `unlocated_fga` add up to `fga`. Every rate has `<m>_n` (its denominator), `<m>_rank` (1 = highest) and `<m>_pct` (the Weibull percentile), ranked within the season among players whose `<m>_n` reaches the rate's gate and null under ten players. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season: the WNBA season is its calendar year. |
| `team_id` | Int64 | stats.wnba.com team id. |
| `player_id` | Int64 | stats.wnba.com player (person) id. |
| `player_name` | String | Player name as the shots release spells it. |
| `games` | Int64 | The player's regular-season games with at least one field-goal attempt for this team. |
| `fga` | Int64 | Field-goal attempts by the player (regular-season games). |
| `fgm` | Int64 | Field goals made by the player. |
| `unlocated_fga` | Int64 | Attempts by the player with no real location (the WNBA's 1997-2010 hoop-centre placeholders; 0 elsewhere): counted in fga, in no zone. |
| `rim_fga` | Int64 | Attempts by the player at the rim (the restricted area: shot_distance under 4 ft). |
| `rim_fgm` | Int64 | Makes by the player at the rim (the restricted area: shot_distance under 4 ft). |
| `rim_ast` | Int64 | Assisted makes by the player at the rim (the restricted area: shot_distance under 4 ft). |
| `mid_fga` | Int64 | Attempts by the player in the mid zone (every other two: the paint outside the restricted area and the mid-range). |
| `mid_fgm` | Int64 | Makes by the player in the mid zone (every other two: the paint outside the restricted area and the mid-range). |
| `mid_ast` | Int64 | Assisted makes by the player in the mid zone (every other two: the paint outside the restricted area and the mid-range). |
| `corner_3_fga` | Int64 | Attempts by the player from the corner (a three from the baseline band, y_legacy <= 87.5, below the break). |
| `corner_3_fgm` | Int64 | Makes by the player from the corner (a three from the baseline band, y_legacy <= 87.5, below the break). |
| `corner_3_ast` | Int64 | Assisted makes by the player from the corner (a three from the baseline band, y_legacy <= 87.5, below the break). |
| `above_break_3_fga` | Int64 | Attempts by the player from above the break (every other three). |
| `above_break_3_fgm` | Int64 | Makes by the player from above the break (every other three). |
| `above_break_3_ast` | Int64 | Assisted makes by the player from above the break (every other three). |
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
| `rim_fg_pct_rank` | Float64 | Rank of rim_fg_pct within the season (the highest ranks 1; ties average), among players whose rim_fg_pct_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `rim_freq_rank` | Float64 | Rank of rim_freq within the season (the highest ranks 1; ties average), among players whose rim_freq_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `rim_ast_pct_rank` | Float64 | Rank of rim_ast_pct within the season (the highest ranks 1; ties average), among players whose rim_ast_pct_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `mid_fg_pct_rank` | Float64 | Rank of mid_fg_pct within the season (the highest ranks 1; ties average), among players whose mid_fg_pct_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `mid_freq_rank` | Float64 | Rank of mid_freq within the season (the highest ranks 1; ties average), among players whose mid_freq_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `mid_ast_pct_rank` | Float64 | Rank of mid_ast_pct within the season (the highest ranks 1; ties average), among players whose mid_ast_pct_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `corner_3_fg_pct_rank` | Float64 | Rank of corner_3_fg_pct within the season (the highest ranks 1; ties average), among players whose corner_3_fg_pct_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `corner_3_freq_rank` | Float64 | Rank of corner_3_freq within the season (the highest ranks 1; ties average), among players whose corner_3_freq_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `corner_3_ast_pct_rank` | Float64 | Rank of corner_3_ast_pct within the season (the highest ranks 1; ties average), among players whose corner_3_ast_pct_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `above_break_3_fg_pct_rank` | Float64 | Rank of above_break_3_fg_pct within the season (the highest ranks 1; ties average), among players whose above_break_3_fg_pct_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `above_break_3_freq_rank` | Float64 | Rank of above_break_3_freq within the season (the highest ranks 1; ties average), among players whose above_break_3_freq_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `above_break_3_ast_pct_rank` | Float64 | Rank of above_break_3_ast_pct within the season (the highest ranks 1; ties average), among players whose above_break_3_ast_pct_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
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

### wnba_player_war {#wnba_player_war}

`wnba_player_war(seasons: 'int | list[int]', *, league_id: 'str' = '10', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

WNBA WAR and VORP per player-season from our BPM, RAPM and BOBAN (league_id='10'). See sportsdataverse.nba.nba_player_war.nba_player_war.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  |  |
| `league_id` | `str` | `'10'` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**

One row per `(season, player_id)` (stats.nba.com ids, Int64): `player_name`, `team_id` (the team he played most possessions for), `teams` (how many he played for), `min`, `poss` (on-court possessions, offensive plus defensive over 2), `rating` and `rating_bpm` / `rating_rapm` / `rating_boban` (anchored, per 100), `vorp`, `war` with `war_lo` / `war_hi` (a nominal 90% interval from a bootstrap over each team-season's games: the sampling uncertainty of the team side and the playing time) and `war_bpm` / `war_rapm` / `war_boban` (the source spread), with `_rank` (1 = most), `_pct` and `_n` (possessions) for `vorp` and `war` among every player of the season. Sorted by season and `war`. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2026 is 2025-26; the WNBA season is its calendar year). |
| `player_id` | Int64 | stats.nba.com player id. |
| `player_name` | String | Player name, from the player impact release. |
| `team_id` | Int64 | stats.nba.com team id of the team he played the most possessions for. |
| `teams` | Int64 | How many teams he played for in the regular season. |
| `team_ids` | String | Every team he played for that season, comma-separated, most possessions first (the first is team_id); roster_tiers puts him on each. |
| `min` | Float64 | Regular-season minutes (the player impact release's). |
| `poss` | Float64 | On-court possessions, offensive plus defensive over 2, counted from the stats.nba.com possessions (every possession row, BB-D3's convention). The minutes basis of vorp and war. |
| `rating` | Float64 | Points per 100 possessions above a league-average player: the mean of rating_bpm, rating_rapm and rating_boban. |
| `rating_bpm` | Float64 | Our BPM per 100, team-anchored and centred: the release's bpm with its team constant repaired (minute-weighted team mean = net rating / 5, BPM 2.0's), minus the player's team mean, plus the team's net rating / 5, centred so the league's possession-weighted mean is 0. |
| `rating_rapm` | Float64 | Our prior-informed RAPM (adj_rapm) per 100, team-anchored and centred like rating_bpm. Only the net adj_rapm is read: the o_adj_rapm / d_adj_rapm split carries a +11 / -11 offset. |
| `rating_boban` | Float64 | Our BOBAN (boban_filtered_skill) per 100, team-anchored and centred like rating_bpm. |
| `vorp` | Float64 | Value over replacement player: (rating - R) * poss / the season's possessions per team, R the league's replacement level (REPLACEMENT_LEVEL; NBA -1.98, WNBA -3.18 per 100). On that scale a season of possessions at R + 1 is a VORP of 1. |
| `war` | Float64 | Wins above replacement: (rating - R) * poss / 100 / points per win (PTS_PER_WIN; NBA 31.56, WNBA 31.31). 0 at replacement level; a season's war adds up to the league's wins minus a team of replacement players' (.189 NBA 2024-25, .104 WNBA 2025). |
| `war_lo` | Float64 | Low end of the nominal 90% interval for war (5th percentile of 1000 bootstrap replicates, each team-season seeded on its own): each team-season's games resampled with replacement (a game's player possessions, team possessions and margin move together), then his possessions, the team net rating, the anchoring and the centring recomputed. The source ratings and the replacement level are held fixed, so this is the sampling uncertainty of the team side and the playing time; the war_bpm / war_rapm / war_boban spread is the source uncertainty. |
| `war_hi` | Float64 | High end of the nominal 90% interval for war (95th percentile of the same 1000 game-resampled replicates as war_lo). |
| `war_bpm` | Float64 | war from rating_bpm alone. Every source shares the league's replacement level, so war_bpm, war_rapm and war_boban add up to the same league total. |
| `war_rapm` | Float64 | war from rating_rapm alone. |
| `war_boban` | Float64 | war from rating_boban alone. BOBAN shrinks low-minute players toward average, so the replacement pool carries more war_boban than war_bpm (the replacement level is a box-score level). |
| `vorp_rank` | Float64 | Rank of vorp within the season among every player (1 = most; ties average); null under 10 players. |
| `war_rank` | Float64 | Rank of war within the season among every player (1 = most; ties average); null under 10 players. |
| `vorp_pct` | Float64 | Percentile of vorp within the season: 100 * (n + 1 - rank) / (n + 1), n the players ranked; null when the rank is null. |
| `war_pct` | Float64 | Percentile of war within the season: 100 * (n + 1 - rank) / (n + 1), n the players ranked; null when the rank is null. |
| `vorp_n` | Int64 | Sample size of vorp: on-court possessions, rounded. |
| `war_n` | Int64 | Sample size of war: on-court possessions, rounded. |
| `war_tier` | String | Depth-ladder tier by WAR percentile within the season's tier cohort, the players with on-court possessions of at least a tenth of the season's possessions per team (TIER_MIN_POSS_SHARE): Star (95 or more), Starter (80 to under 95), Rotation (50 to under 80), Bench (20 to under 50), Fringe (under 20), about the top 5%, then 15%, 30%, 30% and 20%, to within a player at each floor. The percentile is 100 * (n + 1 - rank) / (n + 1) among the cohort, not war_pct. Null under the possession floor (a call-up who barely played) and in a season whose cohort is under ten players. |
| `pts_pg` | Float64 | Points per game he played (minutes over 0), from the regular-season stats game logs. |
| `reb_pg` | Float64 | Rebounds per game he played (minutes over 0), from the regular-season stats game logs. |
| `ast_pg` | Float64 | Assists per game he played (minutes over 0), from the regular-season stats game logs. |
| `pts_pg_rank` | Float64 | Rank of pts_pg among the season's players with at least the games gate (1 game); 1 = most, ties averaged. Null under ten ranked players. |
| `reb_pg_rank` | Float64 | Rank of reb_pg among the season's players with at least the games gate (1 game); 1 = most, ties averaged. Null under ten ranked players. |
| `ast_pg_rank` | Float64 | Rank of ast_pg among the season's players with at least the games gate (1 game); 1 = most, ties averaged. Null under ten ranked players. |
| `pts_pg_pct` | Float64 | Percentile of pts_pg_rank: 100 * (n + 1 - rank) / (n + 1), n the ranked players. |
| `reb_pg_pct` | Float64 | Percentile of reb_pg_rank: 100 * (n + 1 - rank) / (n + 1), n the ranked players. |
| `ast_pg_pct` | Float64 | Percentile of ast_pg_rank: 100 * (n + 1 - rank) / (n + 1), n the ranked players. |
| `pts_pg_n` | Int64 | Games behind pts_pg: the games he played (0 with no box line). |
| `reb_pg_n` | Int64 | Games behind reb_pg: the games he played (0 with no box line). |
| `ast_pg_n` | Int64 | Games behind ast_pg: the games he played (0 with no box line). |

### wnba_player_wowy {#wnba_player_wowy}

`wnba_player_wowy(seasons: 'int | list[int]', *, league_id: 'str' = '10', season_type: 'str' = 'regular-season', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

WNBA WOWY pairs (league_id='10'). See sportsdataverse.nba.nba_player_on_off.nba_player_wowy.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  |  |
| `league_id` | `str` | `'10'` |  |
| `season_type` | `str` | `'regular-season'` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**

One row per `(season, team_id, player_id_a, player_id_b)` (`player_id_a` the smaller id) for every pair of the team's players: for `_both` (together), `_a_only`, `_b_only` and `_neither` the `minutes`, `poss` / `opp_poss`, `pts` / `opp_pts`, `plus_minus` and `off_rtg` / `def_rtg` / `net_rtg`. The four splits' totals add up to the team's season; the ratings do not add. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `team_id` | Int64 | stats.nba.com team id. |
| `player_id_a` | Int64 | The first player of the pair, the smaller id. |
| `player_id_b` | Int64 | The second player of the pair, the larger id. |
| `minutes_both` | Float64 | Game-clock minutes of the team's season with both A and B on the floor. |
| `poss_both` | Int64 | The team's offensive possessions with both A and B on the floor (a possession belongs to the five on the floor when it starts). |
| `opp_poss_both` | Int64 | The team's defensive possessions with both A and B on the floor. |
| `pts_both` | Int64 | The team's points with both A and B on the floor, each scoring action credited to the lineup at that action (a free throw to the lineup at the last foul). |
| `opp_pts_both` | Int64 | The opponent's points with both A and B on the floor, credited as pts_both is. |
| `plus_minus_both` | Int64 | pts_both - opp_pts_both. |
| `off_rtg_both` | Float64 | Offensive rating with both A and B on the floor: pts_both per 100 poss_both; null with no possessions. |
| `def_rtg_both` | Float64 | Defensive rating with both A and B on the floor: opp_pts_both per 100 opp_poss_both; null with no possessions. |
| `net_rtg_both` | Float64 | Net rating with both A and B on the floor: off_rtg_both - def_rtg_both; null when either is null. |
| `minutes_a_only` | Float64 | Game-clock minutes of the team's season with A on the floor and B off. |
| `poss_a_only` | Int64 | The team's offensive possessions with A on the floor and B off (a possession belongs to the five on the floor when it starts). |
| `opp_poss_a_only` | Int64 | The team's defensive possessions with A on the floor and B off. |
| `pts_a_only` | Int64 | The team's points with A on the floor and B off, each scoring action credited to the lineup at that action (a free throw to the lineup at the last foul). |
| `opp_pts_a_only` | Int64 | The opponent's points with A on the floor and B off, credited as pts_a_only is. |
| `plus_minus_a_only` | Int64 | pts_a_only - opp_pts_a_only. |
| `off_rtg_a_only` | Float64 | Offensive rating with A on the floor and B off: pts_a_only per 100 poss_a_only; null with no possessions. |
| `def_rtg_a_only` | Float64 | Defensive rating with A on the floor and B off: opp_pts_a_only per 100 opp_poss_a_only; null with no possessions. |
| `net_rtg_a_only` | Float64 | Net rating with A on the floor and B off: off_rtg_a_only - def_rtg_a_only; null when either is null. |
| `minutes_b_only` | Float64 | Game-clock minutes of the team's season with B on the floor and A off. |
| `poss_b_only` | Int64 | The team's offensive possessions with B on the floor and A off (a possession belongs to the five on the floor when it starts). |
| `opp_poss_b_only` | Int64 | The team's defensive possessions with B on the floor and A off. |
| `pts_b_only` | Int64 | The team's points with B on the floor and A off, each scoring action credited to the lineup at that action (a free throw to the lineup at the last foul). |
| `opp_pts_b_only` | Int64 | The opponent's points with B on the floor and A off, credited as pts_b_only is. |
| `plus_minus_b_only` | Int64 | pts_b_only - opp_pts_b_only. |
| `off_rtg_b_only` | Float64 | Offensive rating with B on the floor and A off: pts_b_only per 100 poss_b_only; null with no possessions. |
| `def_rtg_b_only` | Float64 | Defensive rating with B on the floor and A off: opp_pts_b_only per 100 opp_poss_b_only; null with no possessions. |
| `net_rtg_b_only` | Float64 | Net rating with B on the floor and A off: off_rtg_b_only - def_rtg_b_only; null when either is null. |
| `minutes_neither` | Float64 | Game-clock minutes of the team's season with neither A nor B on the floor. |
| `poss_neither` | Int64 | The team's offensive possessions with neither A nor B on the floor (a possession belongs to the five on the floor when it starts). |
| `opp_poss_neither` | Int64 | The team's defensive possessions with neither A nor B on the floor. |
| `pts_neither` | Int64 | The team's points with neither A nor B on the floor, each scoring action credited to the lineup at that action (a free throw to the lineup at the last foul). |
| `opp_pts_neither` | Int64 | The opponent's points with neither A nor B on the floor, credited as pts_neither is. |
| `plus_minus_neither` | Int64 | pts_neither - opp_pts_neither. |
| `off_rtg_neither` | Float64 | Offensive rating with neither A nor B on the floor: pts_neither per 100 poss_neither; null with no possessions. |
| `def_rtg_neither` | Float64 | Defensive rating with neither A nor B on the floor: opp_pts_neither per 100 opp_poss_neither; null with no possessions. |
| `net_rtg_neither` | Float64 | Net rating with neither A nor B on the floor: off_rtg_neither - def_rtg_neither; null when either is null. |

### wnba_playoff_series {#wnba_playoff_series}

`wnba_playoff_series(seasons: 'int | list[int]', *, league_id: 'str' = '10', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

WNBA postseason games keyed to their series (league_id='10'). See sportsdataverse.nba.nba_playoff_series.nba_playoff_series.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  |  |
| `league_id` | `str` | `'10'` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**

One row per postseason game (ESPN `season_type` 3): `season`, `series_id`, `round` (1 = first round .. 4 = Finals, from ESPN's `type_abbreviation`; null where ESPN gives none), `round_code`, `round_name` (the game headline without "Game N"), `game_number` (the headline's "Game N"; tip-off order in a series with a headline lacking one), `game_id`, `game_date`, `completed`, the home and away `team_id` / `abbreviation` / `display_name` / `score`, `winner_team_id` (null until final), `team_a_id`, `team_b_id`, `team_a_wins` / `team_b_wins` (the series score after this game), and on every row of the series `series_games`, `decided` and `series_winner_team_id` (null until decided): the series' outcome as of the release, not as of that game, so they leak the result into an earlier game's row. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2025 is 2024-25; the WNBA season is its calendar year). |
| `series_id` | String | Series key: '{season}-{lower ESPN team id}-{higher ESPN team id}'. A pair of teams meets at most once per postseason, so the pair is the series. |
| `round` | Int64 | Round number from ESPN's type_abbreviation: 1 first round, 2 second round (NBA conference semifinals), 3 semifinals (NBA conference finals), 4 Finals; null where ESPN gives no round (older seasons). |
| `round_code` | String | ESPN's type_abbreviation for the round (RD16, QTR, SEMI, FINAL); null where it carries none. |
| `round_name` | String | The round as ESPN's game headline names it, without its 'Game N' (the latest game's headline in the series). |
| `game_number` | Int64 | Game number within the series: the "Game N" of the ESPN headline, so a game missing from the release leaves a gap; tip-off order for the whole series when one of its headlines has no number. |
| `game_id` | Int64 | ESPN game id of the series game (joins the ESPN schedule, summary and box score). |
| `game_date` | String | Game date (US). |
| `completed` | Boolean | True when the game is final. |
| `home_team_id` | Int64 | Home team's ESPN team id. |
| `home_abbreviation` | String | Home team's abbreviation. |
| `home_display_name` | String | Home team's display name. |
| `home_score` | Int64 | Home points (null before tip-off). |
| `away_team_id` | Int64 | Away team's ESPN team id. |
| `away_abbreviation` | String | Away team's abbreviation. |
| `away_display_name` | String | Away team's display name. |
| `away_score` | Int64 | Away points (null before tip-off). |
| `winner_team_id` | Int64 | The game winner's ESPN team id; null until the game is final. |
| `team_a_id` | Int64 | ESPN team id of game 1's home team (the side with home court). |
| `team_b_id` | Int64 | ESPN team id of the other side. |
| `team_a_wins` | Int64 | team_a's series wins after this game (finals up to and including it). |
| `team_b_wins` | Int64 | team_b's series wins after this game. |
| `series_games` | Int64 | Games listed in the series so far (played and scheduled); the same on every row of the series. |
| `decided` | Boolean | True once the series leader has the wins to clinch (NBA: four, every round since 2003; WNBA: the Finals only, two through 2004, three 2005-2024, four from 2025) or plays a later series that postseason. The final outcome as of the release, repeated on every row of the series, not as of that game: an earlier game's row already shows it, so never use it as a feature for that game. |
| `series_winner_team_id` | Int64 | The series winner's ESPN team id; null until decided. The final outcome as of the release, repeated on every row of the series, not as of that game: an earlier game's row already shows it, so never use it as a feature for that game. |

### wnba_playoff_series_players {#wnba_playoff_series_players}

`wnba_playoff_series_players(seasons: 'int | list[int]', *, league_id: 'str' = '10', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

WNBA player totals per playoff series against the regular season (league_id='10'). See sportsdataverse.nba.nba_playoff_series.nba_playoff_series_players.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  |  |
| `league_id` | `str` | `'10'` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**

One row per player who played (minutes above zero) in a final game of the series, keyed `(season, series_id, team_id, athlete_id)` with `athlete_display_name`: `games`, `min` and the summed counts as `nba_playoff_series_teams`; `<stat>_pg` per game for `min`, `pts`, `reb`, `ast`, `stl`, `blk`, `tov` and `fg3m`, plus `ts_pct` (`PTS / (2 * (FGA + 0.44 * FTA))`); `reg_games` and `reg_<stat>_pg` / `reg_ts_pct` over the player's regular season (every team played for; null with none); and `<stat>_pg_diff` / `ts_pct_diff`, series minus regular season.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2025 is 2024-25; the WNBA season is its calendar year). |
| `series_id` | String | Series key: '{season}-{lower ESPN team id}-{higher ESPN team id}'. A pair of teams meets at most once per postseason, so the pair is the series. |
| `team_id` | Int64 | ESPN team id of the player's team in the series. |
| `athlete_id` | Int64 | ESPN athlete id (joins the ESPN rosters and player box scores). |
| `athlete_display_name` | String | The player's name. |
| `games` | Int64 | Series games played (not marked did-not-play, with minutes or any box count). |
| `min` | Float64 | Total minutes in the series. |
| `pts` | Int64 | Total points in the series: the sum of the player's ESPN game boxes. |
| `fgm` | Int64 | Total field goals made in the series: the sum of the player's ESPN game boxes. |
| `fga` | Int64 | Total field goals attempted in the series: the sum of the player's ESPN game boxes. |
| `fg3m` | Int64 | Total three-pointers made in the series: the sum of the player's ESPN game boxes. |
| `fg3a` | Int64 | Total three-pointers attempted in the series: the sum of the player's ESPN game boxes. |
| `ftm` | Int64 | Total free throws made in the series: the sum of the player's ESPN game boxes. |
| `fta` | Int64 | Total free throws attempted in the series: the sum of the player's ESPN game boxes. |
| `oreb` | Int64 | Total offensive rebounds in the series: the sum of the player's ESPN game boxes. |
| `dreb` | Int64 | Total defensive rebounds in the series: the sum of the player's ESPN game boxes. |
| `reb` | Int64 | Total rebounds in the series: the sum of the player's ESPN game boxes. |
| `ast` | Int64 | Total assists in the series: the sum of the player's ESPN game boxes. |
| `stl` | Int64 | Total steals in the series: the sum of the player's ESPN game boxes. |
| `blk` | Int64 | Total blocks in the series: the sum of the player's ESPN game boxes. |
| `tov` | Int64 | Total turnovers in the series: the sum of the player's ESPN game boxes. |
| `pf` | Int64 | Total personal fouls in the series: the sum of the player's ESPN game boxes. |
| `min_pg` | Float64 | Minutes per series game played. |
| `pts_pg` | Float64 | Points per series game played. |
| `reb_pg` | Float64 | Total rebounds per series game played. |
| `ast_pg` | Float64 | Assists per series game played. |
| `stl_pg` | Float64 | Steals per series game played. |
| `blk_pg` | Float64 | Blocks per series game played. |
| `tov_pg` | Float64 | Turnovers per series game played. |
| `fg3m_pg` | Float64 | Three-pointers made per series game played. |
| `ts_pct` | Float64 | True shooting in the series: PTS / (2 * (FGA + 0.44 * FTA)); null with no attempts. |
| `reg_games` | Int64 | Regular-season games played (every team the player played for that season); null with none. |
| `reg_min_pg` | Float64 | Minutes per regular-season game played. |
| `reg_pts_pg` | Float64 | Points per regular-season game played. |
| `reg_reb_pg` | Float64 | Total rebounds per regular-season game played. |
| `reg_ast_pg` | Float64 | Assists per regular-season game played. |
| `reg_stl_pg` | Float64 | Steals per regular-season game played. |
| `reg_blk_pg` | Float64 | Blocks per regular-season game played. |
| `reg_tov_pg` | Float64 | Turnovers per regular-season game played. |
| `reg_fg3m_pg` | Float64 | Three-pointers made per regular-season game played. |
| `reg_ts_pct` | Float64 | True shooting over the player's regular season. |
| `min_pg_diff` | Float64 | min_pg - reg_min_pg: the series against the player's regular season (positive is up). |
| `pts_pg_diff` | Float64 | pts_pg - reg_pts_pg: the series against the player's regular season (positive is up). |
| `reb_pg_diff` | Float64 | reb_pg - reg_reb_pg: the series against the player's regular season (positive is up). |
| `ast_pg_diff` | Float64 | ast_pg - reg_ast_pg: the series against the player's regular season (positive is up). |
| `stl_pg_diff` | Float64 | stl_pg - reg_stl_pg: the series against the player's regular season (positive is up). |
| `blk_pg_diff` | Float64 | blk_pg - reg_blk_pg: the series against the player's regular season (positive is up). |
| `tov_pg_diff` | Float64 | tov_pg - reg_tov_pg: the series against the player's regular season (positive is up). |
| `fg3m_pg_diff` | Float64 | fg3m_pg - reg_fg3m_pg: the series against the player's regular season (positive is up). |
| `ts_pct_diff` | Float64 | ts_pct - reg_ts_pct. |

### wnba_playoff_series_teams {#wnba_playoff_series_teams}

`wnba_playoff_series_teams(seasons: 'int | list[int]', *, league_id: 'str' = '10', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

WNBA team totals per playoff series (league_id='10'). See sportsdataverse.nba.nba_playoff_series.nba_playoff_series_teams.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  |  |
| `league_id` | `str` | `'10'` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**

Two rows per series with a final game, keyed `(season, series_id, team_id)` with `opponent_team_id` and `team_abbreviation`: `games`, `wins`, `losses`, `pts`, `opp_pts` and the summed counts `fgm`, `fga`, `fg3m`, `fg3a`, `ftm`, `fta`, `oreb`, `dreb`, `reb`, `ast`, `stl`, `blk`, `tov`, `pf` over the series' final games.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2025 is 2024-25; the WNBA season is its calendar year). |
| `series_id` | String | Series key: '{season}-{lower ESPN team id}-{higher ESPN team id}'. A pair of teams meets at most once per postseason, so the pair is the series. |
| `team_id` | Int64 | ESPN team id of the row's team (joins the ESPN schedule and team box scores). |
| `opponent_team_id` | Int64 | The opponent's ESPN team id. |
| `team_abbreviation` | String | Team abbreviation. |
| `games` | Int64 | Final games in the series. |
| `wins` | Int64 | Games the team won in the series, counting its final games only. |
| `losses` | Int64 | Games the team lost in the series, counting its final games only. |
| `opp_pts` | Int64 | Opponent points over the series' final games. |
| `pts` | Int64 | Total points over the series' final games: the sum of the team's ESPN game boxes. |
| `fgm` | Int64 | Total field goals made over the series' final games: the sum of the team's ESPN game boxes. |
| `fga` | Int64 | Total field goals attempted over the series' final games: the sum of the team's ESPN game boxes. |
| `fg3m` | Int64 | Total three-pointers made over the series' final games: the sum of the team's ESPN game boxes. |
| `fg3a` | Int64 | Total three-pointers attempted over the series' final games: the sum of the team's ESPN game boxes. |
| `ftm` | Int64 | Total free throws made over the series' final games: the sum of the team's ESPN game boxes. |
| `fta` | Int64 | Total free throws attempted over the series' final games: the sum of the team's ESPN game boxes. |
| `oreb` | Int64 | Total offensive rebounds over the series' final games: the sum of the team's ESPN game boxes. |
| `dreb` | Int64 | Total defensive rebounds over the series' final games: the sum of the team's ESPN game boxes. |
| `reb` | Int64 | Total rebounds over the series' final games: the sum of the team's ESPN game boxes. |
| `ast` | Int64 | Total assists over the series' final games: the sum of the team's ESPN game boxes. |
| `stl` | Int64 | Total steals over the series' final games: the sum of the team's ESPN game boxes. |
| `blk` | Int64 | Total blocks over the series' final games: the sum of the team's ESPN game boxes. |
| `tov` | Int64 | Total turnovers over the series' final games: the sum of the team's ESPN game boxes. |
| `pf` | Int64 | Total personal fouls over the series' final games: the sum of the team's ESPN game boxes. |

### wnba_playtype_ratings {#wnba_playtype_ratings}

`wnba_playtype_ratings(season: 'str', *, off_team: "'Optional[pl.DataFrame]'" = None, def_team: "'Optional[pl.DataFrame]'" = None, schedule: "'Optional[pl.DataFrame]'" = None, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

WNBA Synergy play-type-adjusted offense/defense (`league_id="10"`).

Thin wrapper binding
`sportsdataverse.nba.nba_playtype.nba_playtype_ratings` to the
women's league. Synergy coverage is sparse for the WNBA; an empty upstream
fetch degrades to a zero-row frame (never raises).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | Season string, e.g. `"2024"`. |
| `off_team` | `Optional[DataFrame]` | `None` | Injected Synergy offensive team frame (bypasses the live fetch). |
| `def_team` | `Optional[DataFrame]` | `None` | Injected Synergy defensive team frame. |
| `schedule` | `Optional[DataFrame]` | `None` | Injected `team_id`/`opp_team_id` schedule frame. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Same schema as `sportsdataverse.nba.nba_playtype.nba_playtype_ratings`.

No returns table is published for this function: no capture: it reads stats.nba.com (WNBA league id 10), which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.wnba import wnba_playtype_ratings
r = wnba_playtype_ratings("2024")
print(r.sort("adj_off", descending=True).head())
```

### wnba_referee_assignments {#wnba_referee_assignments}

`wnba_referee_assignments(date: 'str | _dt.date', *, raw: 'bool' = False, return_as_pandas: 'bool' = False, proxy: 'dict | None' = None) -> 'dict[str, Any]'`

Fetch and parse WNBA referee assignments for a given date from official.nba.com.

Retrieves the referee crew assignments and replay center officials for all WNBA
games on a given date. The `crew_position` column (1–4) represents the feed's
slot order; slot 1 is inferred to be the crew chief. The `season` column is
the WNBA single-year season (feed year converted as-is). This is a thin shim
over `sportsdataverse.nba.nba_officiating.nba_referee_assignments` that
sets `league="wnba"`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `date` | `str \| date` |  | The date to fetch assignments for (str in "YYYY-MM-DD" format or datetime.date). |
| `raw` | `bool` | `False` | If True, return the raw JSON payload (dict) with all three leagues instead of parsed DataFrames. |
| `return_as_pandas` | `bool` | `False` | If True, return pandas DataFrames instead of polars. |
| `proxy` | `dict \| None` | `None` | Optional proxy dict passed through to the HTTP layer. |

**Returns**

A dict with keys `"officials"` and `"replay_center"` mapping to DataFrames. If `raw=True`, returns the full three-league JSON payload instead.

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
| `ValueError` | `date` is not a valid "YYYY-MM-DD" date (checked before any request). |
| `NoDataError` | Official.nba.com answered 404, or 403 with S3's `AccessDenied` body. A date without WNBA games is not this (see the Note). |
| `AssetFetchError` | The fetch failed (network error, rate limit, or Akamai WAF block), the 200 body is not a JSON object, or the `wnba` block's `Table`/`Table1` rows are missing or malformed (a row without `game_id` included). With `raw=True` all three leagues (nba, gl and wnba) are checked, since the whole payload is returned. |

**Example**

```python
from sportsdataverse.wnba.wnba_officiating import wnba_referee_assignments
result = wnba_referee_assignments("2026-06-13")
officials = result["officials"]
print(f"Found {officials.height} official slots")
```

### wnba_season_sim {#wnba_season_sim}

`wnba_season_sim(season: 'int', *, as_of_date: 'dt.date | None' = None, league_id: 'str' = '10', n_sims: 'int' = 10000, seed: 'int' = 20261103, chances: 'list[float] | None' = None, cup_schedule: 'pl.DataFrame | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

WNBA season sim: projected W-L, seed, playoff and lottery odds (league_id='10'). See sportsdataverse.nba.nba_season_sim.nba_season_sim.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  |  |
| `as_of_date` | `date \| None` | `None` |  |
| `league_id` | `str` | `'10'` |  |
| `n_sims` | `int` | `10000` |  |
| `seed` | `int` | `20261103` |  |
| `chances` | `list[float] \| None` | `None` |  |
| `cup_schedule` | `DataFrame \| None` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**

One row per team (stats.nba.com `team_id`, Int64), sorted by conference and mean seed: * `wins` / `losses` so far, `games_remaining`, and the as-of `adj_net_rtg` with its posterior sd `adj_net_rtg_sd` (capped so two teams' shift carries at most 3/4 of a game's variance; it binds only near the prior, before many games are played). * `proj_wins` / `proj_losses` (means), `proj_wins_sd`, the 10th and 90th percentiles, and `proj_wins_se` (the Monte Carlo error of the mean). * `p_division` (NBA), `p_seed_1` .. `p_seed_15` (the regular-season finish in the conference, NBA, or the league, WNBA) and `mean_seed`. * `p_top4` (first-round home court), `p_top6` (NBA, straight to the playoffs), `p_play_in` (NBA, seeds 7-10), `p_playoffs` (NBA: top 6 or through the play-in; WNBA: top 8) and its Monte Carlo `p_playoffs_se`. * `p_lottery`, `p_pick_1`, `p_drawn_pick` (a pick from the drawing: the NBA's top four, the WNBA's top two) and `mean_pick` (given a lottery finish). The pick columns are null when the league has no published chances for that many lottery teams. * `n_sims`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2026 is 2025-26); the WNBA season is its calendar year. |
| `as_of_date` | Date | The as-of date: games on or after it are simulated, the ones before it are played. Null when every game with a score counts as played. |
| `team_id` | Int64 | stats.nba.com team id (Int64). |
| `conference` | String | NBA conference (East / West) from the 2004-05 alignment; "WNBA" for every WNBA team (one table). |
| `division` | String | NBA division (Atlantic, Central, Southeast, Northwest, Pacific, Southwest); "WNBA" for every WNBA team. |
| `wins` | Int64 | Regular-season wins in the games already played. |
| `losses` | Int64 | Regular-season losses in the games already played. |
| `games_remaining` | Int64 | Regular-season games on the schedule still to play (simulated). |
| `adj_net_rtg` | Float64 | As-of adjusted net rating from the possession ridge at the sim's penalty: points per 100 over an average opponent on a neutral court. |
| `adj_net_rtg_sd` | Float64 | Error sd of adj_net_rtg (points per 100), capped so two teams' shift carries at most 3/4 of a game's variance; the sd of the per-season rating shift each simulated season draws. With a preseason prior it is the prior's miss at the weight the ridge leaves it plus the sampling noise of the possessions played (PRIOR_MISS_POSS: fitted for the WNBA; the NBA keeps the ridge posterior); with none, the ridge posterior. |
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

### wnba_season_standings {#wnba_season_standings}

`wnba_season_standings(games: 'pl.DataFrame', *, league_id: 'str' = '10', seed: 'int' = 0, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

WNBA standings with the WNBA tiebreakers (league_id='10'). See sportsdataverse.nba.nba_season_rules.nba_season_standings.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games` | `DataFrame` |  |  |
| `league_id` | `str` | `'10'` |  |
| `seed` | `int` | `0` |  |
| `return_as_pandas` | `bool` | `False` |  |

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

### wnba_shot_value {#wnba_shot_value}

`wnba_shot_value(player_ids: "'list[int]'", season: 'str', *, include_context: 'bool' = False, return_as_pandas: 'bool' = False) -> "'dict[str, Union[pl.DataFrame, pd.DataFrame]]'"`

WNBA one-call shot-value spine (`league_id="10"`).

Thin wrapper binding `sportsdataverse.nba.nba_shot_value.nba_shot_value`
to the women's league; fetches each player's `shotchartdetail`, scores
per-shot expected points from the free `LeagueAverages` zone table, and
returns the scored shots plus shooter talent, selection quality, and
zone-value maps (and the defender/shot-clock context tables when
`include_context=True`). Women's court geometry + shrinkage constant are
keyed `"10"` in `nba_shot_value_constants`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_ids` | `list[int]` |  | Player ids to fetch. |
| `season` | `str` |  | Season string, e.g. `"2024"`. |
| `include_context` | `bool` | `False` | Also fetch + return the `playerdashptshots` defender/shot-clock context tables. |
| `return_as_pandas` | `bool` | `False` | Return pandas frames instead of polars. |

**Returns**

`{"shots", "talent", "selection", "zones"}` (plus `"context"` when requested). An empty fetch returns a dict of zero-row frames.

**Example**

```python
from sportsdataverse.wnba import wnba_shot_value
out = wnba_shot_value([1628886], "2024")
out["talent"].head()
```

### wnba_situational_lineup {#wnba_situational_lineup}

`wnba_situational_lineup(seasons: 'int | list[int]', *, league_id: 'str' = '10', season_type: 'str' = 'regular-season', top_n: 'int | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

WNBA five-man unit splits by situation (league_id='10'). See sportsdataverse.nba.nba_situational.nba_situational_lineup.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  |  |
| `league_id` | `str` | `'10'` |  |
| `season_type` | `str` | `'regular-season'` |  |
| `top_n` | `int \| None` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**

One row per `(season, team_id, lineup_id, family, split)`: `lineup_id` is the five player ids sorted and joined with `-` (BB-D4's id), `player_1..5` the same ids, then the columns of `nba_situational_team`. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `team_id` | Int64 | stats.nba.com team id. |
| `lineup_id` | String | The five player ids of the unit on the floor as the possession starts, sorted and joined with '-' (BB-D4's id). |
| `player_1` | Int64 | Player 1 of the unit, in lineup_id order. |
| `player_2` | Int64 | Player 2 of the unit, in lineup_id order. |
| `player_3` | Int64 | Player 3 of the unit, in lineup_id order. |
| `player_4` | Int64 | Player 4 of the unit, in lineup_id order. |
| `player_5` | Int64 | Player 5 of the unit, in lineup_id order. |
| `family` | String | The split family: all, clutch, period, half, garbage, score, venue, opponent, play_type or chance; within a family the splits add up to the all row. |
| `split` | String | The split: all; clutch / not_clutch; p1..p4 / ot; h1 / h2 / ot; garbage_time / not_garbage_time; leading / tied / trailing; home / away / neutral; vs_top / vs_rest; transition / half_court; second_chance / first_chance. |
| `games` | Int64 | Games with at least one possession in the split (does not add up across a family). |
| `minutes` | Float64 | Game-clock minutes of the split's possessions (seconds / 60). |
| `seconds` | Float64 | Game-clock seconds of the split's possessions: each possession's start clock minus its end clock. |
| `poss` | Int64 | Offensive possessions in the split. |
| `opp_poss` | Int64 | Defensive possessions in the split. |
| `pts` | Int64 | Points scored on the offensive possessions. |
| `opp_pts` | Int64 | Points allowed on the defensive possessions. |
| `plus_minus` | Int64 | Plus-minus over the split: pts - opp_pts. |
| `off_rtg` | Float64 | Offensive rating: pts per 100 poss; null with no offensive possessions. |
| `def_rtg` | Float64 | Defensive rating: opp_pts per 100 opp_poss; null with no defensive possessions. |
| `net_rtg` | Float64 | Net rating: off_rtg - def_rtg; null when either is null. |
| `off_rtg_n` | Int64 | Sample size of off_rtg: poss. |
| `def_rtg_n` | Int64 | Sample size of def_rtg: opp_poss. |
| `net_rtg_n` | Int64 | Sample size of net_rtg: poss + opp_poss. |
