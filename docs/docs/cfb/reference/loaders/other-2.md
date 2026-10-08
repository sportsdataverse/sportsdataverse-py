---
title: "CFB dataset loaders — Other: passing–group_aliases"
sidebar_label: "Other: passing–group_aliases"
sidebar_position: 16
description: "CFB dataset loaders — Other: passing–group_aliases — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# CFB dataset loaders — Other: passing–group_aliases

## load_cfb_passing

Release: [espn_cfb_passing](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_passing) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_passing/cfb_passing_{season}.parquet`
### Returns {#load_cfb_passing-returns}

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | ESPN team id. |
| `pos_team` | String | Team name in possession at the start of the play (offense, kickoff-aware). |
| `division` | String | Division in the conference for the team. |
| `conference` | String | Conference of the team. |
| `season` | Int64 | Season (4-digit year). |
| `player_id` | Int64 | ESPN player id from the roster entry. |
| `passer_player_name` | String | Display name of the passer -- the FIRST participant in that role on the play. |
| `plays` | UInt32 | Total qualifying passing plays included in the WEPA calculation. |
| `games` | UInt32 | Number of games included in the ATS summary. |
| `team_games` | UInt32 | Games the team played, used as the per-game denominator. |
| `TEPA` | Float64 | Total EPA summed over every play. |
| `EPAplay` | Float64 | EPA generated per play. |
| `yards` | Float64 | Total yards gained on the drive. |
| `success` | Float64 | Success rate across the team plays. |
| `comp` | Float64 | Completed passes. |
| `att` | Float64 | Pass attempts thrown. |
| `comppct` | Float64 | Completion percentage. |
| `passing_td` | Float64 | Passing touchdowns thrown. |
| `playsgame` | Float64 | Plays per game. |
| `EPAgame` | Float64 | EPA generated per game. |
| `yardsplay` | Float64 | Yards per play. |
| `yardsgame` | Float64 | Yards per game. |
| `sacked` | UInt32 | Times the passer was sacked. |
| `sack_yds` | Int64 | Yards lost to sacks. |
| `sack_epa` | Float64 | EPA lost on the sacks the team's passers took -- the expected-points cost of those plays. |
| `pass_int` | UInt32 | Interceptions thrown. |
| `int_epa` | Float64 | EPA lost on the team's interceptions thrown -- the expected-points cost of the turnovers, not a count. |
| `detmer` | Float64 | Detmer rating -- the composite passing-efficiency measure this pipeline publishes, named for the college passing-efficiency tradition. |
| `detmergame` | Float64 | Detmer rating expressed per game. |
| `dropbacks` | Float64 | Dropbacks taken by the passer. |
| `sack_adj_yards` | Float64 | Passing yards adjusted for sack yardage lost. |
| `yardsdropback` | Float64 | Yards per dropback. |
| `dispersion_games` | Int64 | Games with at least one dropback by the passer: the sample the dispersion columns (EPAplay_sd, EPAplay_p10, EPAplay_p90, boom_rate, bust_rate) are computed over. Those five are null below 3 games. |
| `EPAplay_sd` | Float64 | Population standard deviation (ddof=0) of the passer's per-game EPA per play (a game's EPA summed over the passer's dropbacks, divided by those dropbacks), across dispersion_games. Null when dispersion_games is below 3. |
| `EPAplay_p10` | Float64 | 10th percentile (the floor) of the passer's per-game EPA per play (a game's EPA summed over the passer's dropbacks, divided by those dropbacks), across dispersion_games; linear interpolation (R type 7). Null when dispersion_games is below 3. |
| `EPAplay_p90` | Float64 | 90th percentile (the ceiling) of the passer's per-game EPA per play (a game's EPA summed over the passer's dropbacks, divided by those dropbacks), across dispersion_games; linear interpolation (R type 7). Null when dispersion_games is below 3. |
| `boom_rate` | Float64 | Share of dispersion_games in which the passer's EPA per play was more than one EPAplay_sd ABOVE their own unweighted per-game mean (not the play-weighted EPAplay). Strict, so a player whose games are all equal is neither a boom nor a bust. Null when dispersion_games is below 3. |
| `bust_rate` | Float64 | Share of dispersion_games in which the passer's EPA per play was more than one EPAplay_sd BELOW their own unweighted per-game mean (not the play-weighted EPAplay). Strict, so a player whose games are all equal is neither a boom nor a bust. Null when dispersion_games is below 3. |
| `TEPA_rank` | Float64 | Rank of the passer's total EPA summed over every play among passers clearing the leaderboard minimum of 14 dropbacks per team game, where 1 is best. |
| `EPAgame_rank` | Float64 | Rank of the passer's EPA generated per game among passers clearing the leaderboard minimum of 14 dropbacks per team game, where 1 is best. |
| `EPAplay_rank` | Float64 | Rank of the passer's EPA generated per play among passers clearing the leaderboard minimum of 14 dropbacks per team game, where 1 is best. |
| `success_rank` | Float64 | Rank of the passer's success rate across their plays among passers clearing the leaderboard minimum of 14 dropbacks per team game, where 1 is best. |
| `comppct_rank` | Float64 | Rank of the passer's completion percentage among passers clearing the leaderboard minimum of 14 dropbacks per team game, where 1 is best. |
| `yards_rank` | Float64 | Rank of the passer's total yards among passers clearing the leaderboard minimum of 14 dropbacks per team game, where 1 is best. |
| `yardsplay_rank` | Float64 | Rank of the passer's yards per play among passers clearing the leaderboard minimum of 14 dropbacks per team game, where 1 is best. |
| `yardsgame_rank` | Float64 | Rank of the passer's yards per game among passers clearing the leaderboard minimum of 14 dropbacks per team game, where 1 is best. |
| `sack_adj_yards_rank` | Float64 | Rank of the passer's passing yards adjusted for sack yardage lost among passers clearing the leaderboard minimum of 14 dropbacks per team game, where 1 is best. |
| `yardsdropback_rank` | Float64 | Rank of the passer's yards per dropback among passers clearing the leaderboard minimum of 14 dropbacks per team game, where 1 is best. |
| `detmer_rank` | Float64 | Rank of the passer's detmer rating -- the composite passing-efficiency measure this pipeline publishes, named for the college passing-efficiency tradition among passers clearing the leaderboard minimum of 14 dropbacks per team game, where 1 is best. |
| `detmergame_rank` | Float64 | Rank of the passer's detmer rating expressed per game among passers clearing the leaderboard minimum of 14 dropbacks per team game, where 1 is best. |
| `passing_td_rank` | Float64 | Ordinal rank of the player's passing touchdowns among qualifying players that season; ties share a fractional rank. |
| `pass_int_rank` | Float64 | Ordinal rank of the player's interceptions thrown among qualifying players that season; ties share a fractional rank. |
| `sacked_rank` | Float64 | Ordinal rank of the player's times sacked among qualifying players that season; ties share a fractional rank. |
| `boom_rate_rank` | Float64 | Rank of the passer's boom rate (share of games more than one standard deviation above their own per-game EPA per play) among passers clearing the leaderboard minimum of 14 dropbacks per team game, where 1 is best. |
| `TEPA_pct` | Float64 | Percentile position (0-100) of the player's total EPA among qualifying players that season. |
| `EPAgame_pct` | Float64 | Percentile position (0-100) of the player's EPA per game among qualifying players that season. |
| `EPAplay_pct` | Float64 | Percentile position (0-100) of the player's EPA per play among qualifying players that season. |
| `success_pct` | Float64 | Percentile position (0-100) of the player's success rate among qualifying players that season. |
| `comppct_pct` | Float64 | Percentile position (0-100) of the player's completion percentage among qualifying players that season. |
| `yards_pct` | Float64 | Percentile position (0-100) of the player's yards among qualifying players that season. |
| `yardsplay_pct` | Float64 | Percentile position (0-100) of the player's yards per play among qualifying players that season. |
| `yardsgame_pct` | Float64 | Percentile position (0-100) of the player's yards per game among qualifying players that season. |
| `sack_adj_yards_pct` | Float64 | Percentile position (0-100) of the player's sack-adjusted yards among qualifying players that season. |
| `yardsdropback_pct` | Float64 | Percentile position (0-100) of the player's yards per dropback among qualifying players that season. |
| `detmer_pct` | Float64 | Percentile position (0-100) of the player's Detmer rating among qualifying players that season. |
| `detmergame_pct` | Float64 | Percentile position (0-100) of the player's Detmer rating per game among qualifying players that season. |
| `passing_td_pct` | Float64 | Percentile position (0-100) of the player's passing touchdowns among qualifying players that season. |
| `pass_int_pct` | Float64 | Percentile position (0-100) of the player's interceptions thrown among qualifying players that season. |
| `sacked_pct` | Float64 | Percentile position (0-100) of the player's times sacked among qualifying players that season. |
| `boom_rate_pct` | Float64 | Percentile (0-100) of boom rate (share of games more than one standard deviation above their own per-game EPA per play) among passers clearing the leaderboard minimum of 14 dropbacks per team game, where 100 is best. Direction is already encoded in the matching rank, so a lower-is-better metric still scores 100 at its best. Null when the metric is null, and null rows are excluded from the denominator. |
| `EPAplay_n` | Int64 | Sample size behind EPAplay: the number of dropbacks (completions, incompletions, sacks and interceptions) the passer's value is computed over. 0 where EPAplay is null. |
| `yardsdropback_n` | Int64 | Sample size behind yardsdropback: the number of dropbacks (completions, incompletions, sacks and interceptions) the passer's value is computed over. 0 where yardsdropback is null. |
| `comppct_n` | Int64 | Sample size behind comppct: the number of throws (including interceptions, excluding sacks) the passer's value is computed over. 0 where comppct is null. |
| `success_n` | Int64 | Sample size behind success: the number of completions and incompletions (interceptions and sacks excluded) the passer's value is computed over. 0 where success is null. nfl-data's nfl_passing table counts its success_n over dropbacks instead. |
| `yardsplay_n` | Int64 | Sample size behind yardsplay: the number of completions and incompletions (interceptions and sacks excluded) the passer's value is computed over. 0 where yardsplay is null. nfl-data's nfl_passing table counts its yardsplay_n over pass attempts instead: interceptions included, sacks still excluded. |
| `detmer_n` | Int64 | Sample size behind detmer: the number of games the passer's value is computed over. 0 where detmer is null. |
| `detmergame_n` | Int64 | Sample size behind detmergame: the number of games the passer's value is computed over. 0 where detmergame is null. |
| `EPAgame_n` | Int64 | Sample size behind EPAgame: the number of games the passer's value is computed over. 0 where EPAgame is null. |
| `yardsgame_n` | Int64 | Sample size behind yardsgame: the number of games the passer's value is computed over. 0 where yardsgame is null. |
| `playsgame_n` | Int64 | Sample size behind playsgame: the number of games the passer's value is computed over. 0 where playsgame is null. |
| `fbs_class` | String | Power/Group classification for the season: P4 or G6 from 2024 on, P5 or G5 through 2023, derived from conference membership. Null for teams outside FBS. |
| `position_group` | String | Position group from the season's ESPN roster (espn_cfb_rosters position_abbreviation): QB; RB (RB and FB); WR; TE; other for any other listed position. Null when the player is not on the season roster, is listed without a position ('-'), or is listed under two different groups. The cohort of the _pos_pct columns. |
| `TEPA_pos_pct` | Float64 | Percentile (0-100) of total EPA summed over every play among passers clearing the leaderboard minimum of 14 dropbacks per team game at the same position group (position_group), where 100 is best. Direction is already encoded in the matching rank, so a lower-is-better metric still scores 100 at its best. Null when the metric is null, when position_group is null, or when fewer than 10 qualifiers in the group have the metric. Rows with a null metric are excluded from the denominator. |
| `EPAgame_pos_pct` | Float64 | Percentile (0-100) of EPA generated per game among passers clearing the leaderboard minimum of 14 dropbacks per team game at the same position group (position_group), where 100 is best. Direction is already encoded in the matching rank, so a lower-is-better metric still scores 100 at its best. Null when the metric is null, when position_group is null, or when fewer than 10 qualifiers in the group have the metric. Rows with a null metric are excluded from the denominator. |
| `EPAplay_pos_pct` | Float64 | Percentile (0-100) of EPA generated per play among passers clearing the leaderboard minimum of 14 dropbacks per team game at the same position group (position_group), where 100 is best. Direction is already encoded in the matching rank, so a lower-is-better metric still scores 100 at its best. Null when the metric is null, when position_group is null, or when fewer than 10 qualifiers in the group have the metric. Rows with a null metric are excluded from the denominator. |
| `success_pos_pct` | Float64 | Percentile (0-100) of success rate across their plays among passers clearing the leaderboard minimum of 14 dropbacks per team game at the same position group (position_group), where 100 is best. Direction is already encoded in the matching rank, so a lower-is-better metric still scores 100 at its best. Null when the metric is null, when position_group is null, or when fewer than 10 qualifiers in the group have the metric. Rows with a null metric are excluded from the denominator. |
| `comppct_pos_pct` | Float64 | Percentile (0-100) of completion percentage among passers clearing the leaderboard minimum of 14 dropbacks per team game at the same position group (position_group), where 100 is best. Direction is already encoded in the matching rank, so a lower-is-better metric still scores 100 at its best. Null when the metric is null, when position_group is null, or when fewer than 10 qualifiers in the group have the metric. Rows with a null metric are excluded from the denominator. |
| `yards_pos_pct` | Float64 | Percentile (0-100) of total yards among passers clearing the leaderboard minimum of 14 dropbacks per team game at the same position group (position_group), where 100 is best. Direction is already encoded in the matching rank, so a lower-is-better metric still scores 100 at its best. Null when the metric is null, when position_group is null, or when fewer than 10 qualifiers in the group have the metric. Rows with a null metric are excluded from the denominator. |
| `yardsplay_pos_pct` | Float64 | Percentile (0-100) of yards per play among passers clearing the leaderboard minimum of 14 dropbacks per team game at the same position group (position_group), where 100 is best. Direction is already encoded in the matching rank, so a lower-is-better metric still scores 100 at its best. Null when the metric is null, when position_group is null, or when fewer than 10 qualifiers in the group have the metric. Rows with a null metric are excluded from the denominator. |
| `yardsgame_pos_pct` | Float64 | Percentile (0-100) of yards per game among passers clearing the leaderboard minimum of 14 dropbacks per team game at the same position group (position_group), where 100 is best. Direction is already encoded in the matching rank, so a lower-is-better metric still scores 100 at its best. Null when the metric is null, when position_group is null, or when fewer than 10 qualifiers in the group have the metric. Rows with a null metric are excluded from the denominator. |
| `sack_adj_yards_pos_pct` | Float64 | Percentile (0-100) of passing yards adjusted for sack yardage lost among passers clearing the leaderboard minimum of 14 dropbacks per team game at the same position group (position_group), where 100 is best. Direction is already encoded in the matching rank, so a lower-is-better metric still scores 100 at its best. Null when the metric is null, when position_group is null, or when fewer than 10 qualifiers in the group have the metric. Rows with a null metric are excluded from the denominator. |
| `yardsdropback_pos_pct` | Float64 | Percentile (0-100) of yards per dropback among passers clearing the leaderboard minimum of 14 dropbacks per team game at the same position group (position_group), where 100 is best. Direction is already encoded in the matching rank, so a lower-is-better metric still scores 100 at its best. Null when the metric is null, when position_group is null, or when fewer than 10 qualifiers in the group have the metric. Rows with a null metric are excluded from the denominator. |
| `detmer_pos_pct` | Float64 | Percentile (0-100) of detmer rating -- the composite passing-efficiency measure this pipeline publishes, named for the college passing-efficiency tradition among passers clearing the leaderboard minimum of 14 dropbacks per team game at the same position group (position_group), where 100 is best. Direction is already encoded in the matching rank, so a lower-is-better metric still scores 100 at its best. Null when the metric is null, when position_group is null, or when fewer than 10 qualifiers in the group have the metric. Rows with a null metric are excluded from the denominator. |
| `detmergame_pos_pct` | Float64 | Percentile (0-100) of detmer rating expressed per game among passers clearing the leaderboard minimum of 14 dropbacks per team game at the same position group (position_group), where 100 is best. Direction is already encoded in the matching rank, so a lower-is-better metric still scores 100 at its best. Null when the metric is null, when position_group is null, or when fewer than 10 qualifiers in the group have the metric. Rows with a null metric are excluded from the denominator. |
| `passing_td_pos_pct` | Float64 | Percentile (0-100) of passing touchdowns among passers clearing the leaderboard minimum of 14 dropbacks per team game at the same position group (position_group), where 100 is best. Direction is already encoded in the matching rank, so a lower-is-better metric still scores 100 at its best. Null when the metric is null, when position_group is null, or when fewer than 10 qualifiers in the group have the metric. Rows with a null metric are excluded from the denominator. |
| `pass_int_pos_pct` | Float64 | Percentile (0-100) of interceptions thrown among passers clearing the leaderboard minimum of 14 dropbacks per team game at the same position group (position_group), where 100 is best. Direction is already encoded in the matching rank, so a lower-is-better metric still scores 100 at its best. Null when the metric is null, when position_group is null, or when fewer than 10 qualifiers in the group have the metric. Rows with a null metric are excluded from the denominator. |
| `sacked_pos_pct` | Float64 | Percentile (0-100) of times sacked among passers clearing the leaderboard minimum of 14 dropbacks per team game at the same position group (position_group), where 100 is best. Direction is already encoded in the matching rank, so a lower-is-better metric still scores 100 at its best. Null when the metric is null, when position_group is null, or when fewer than 10 qualifiers in the group have the metric. Rows with a null metric are excluded from the denominator. |
| `boom_rate_pos_pct` | Float64 | Percentile (0-100) of boom rate (share of games more than one standard deviation above their own per-game EPA per play) among passers clearing the leaderboard minimum of 14 dropbacks per team game at the same position group (position_group), where 100 is best. Direction is already encoded in the matching rank, so a lower-is-better metric still scores 100 at its best. Null when the metric is null, when position_group is null, or when fewer than 10 qualifiers in the group have the metric. Rows with a null metric are excluded from the denominator. |

```python
load_cfb_passing(seasons=2024)
```

## load_cfb_percentiles

Release: [espn_cfb_percentiles](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_percentiles) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_percentiles/cfb_percentiles_{season}.parquet`
### Returns {#load_cfb_percentiles-returns}

| col_name | type | description |
|---|---|---|
| `pctile` | Float64 | Percentile bucket the row reports, from 0 to 100. |
| `GEI` | Float64 | Value of game excitement index at the percentile this row reports. |
| `EPAplay` | Float64 | Value of EPA generated per play at the percentile this row reports. |
| `pass_success` | Float64 | Value of success rate on pass plays at the percentile this row reports. |
| `rush_success` | Float64 | Value of success rate on rush plays at the percentile this row reports. |
| `early_down_success` | Float64 | Value of success rate on early downs at the percentile this row reports. |
| `early_down_EPA` | Float64 | Value of EPA per early-down play at the percentile this row reports. |
| `late_down_success` | Float64 | Value of success rate on late downs at the percentile this row reports. |
| `success` | Float64 | Value of success rate across the team plays at the percentile this row reports. |
| `yardsplay` | Float64 | Value of yards per play at the percentile this row reports. |
| `dropbacks` | Float64 | Value of dropbacks taken by the passer at the percentile this row reports. |
| `rushes` | Float64 | Value of rushing attempts at the percentile this row reports. |
| `EPAdropback` | Float64 | Value of EPA generated per dropback at the percentile this row reports. |
| `EPArush` | Float64 | Value of EPA generated per rushing attempt at the percentile this row reports. |
| `yardsdropback` | Float64 | Value of yards per dropback at the percentile this row reports. |
| `pass_explosive` | Float64 | Value of explosive-play rate on pass plays at the percentile this row reports. |
| `rush_explosive` | Float64 | Value of explosive-play rate on rush plays at the percentile this row reports. |
| `explosive` | Float64 | Value of explosive-play rate at the percentile this row reports. |
| `third_down_success` | Float64 | Value of success rate on third down at the percentile this row reports. |
| `red_zone_success` | Float64 | Value of success rate in the red zone at the percentile this row reports. |
| `play_stuffed` | Float64 | Value of stuffed-play rate at the percentile this row reports. |
| `nonExplosiveEpaPerPlay` | Float64 | Value of EPA per play excluding explosive plays at the percentile this row reports. |
| `havoc` | Float64 | Value of havoc rate at the percentile this row reports. |
| `yardsrush` | Float64 | Value of yards per rush at the percentile this row reports. |
| `lineyards` | Float64 | Value of line yards per rush at the percentile this row reports. |
| `opportunity_run` | Float64 | Value of opportunity-run rate at the percentile this row reports. |
| `third_down_distance` | Float64 | Value of average yards to go on third down at the percentile this row reports. |

```python
load_cfb_percentiles(seasons=2024)
```

## load_cfb_receiving

Release: [espn_cfb_receiving](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_receiving) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_receiving/cfb_receiving_{season}.parquet`
### Returns {#load_cfb_receiving-returns}

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | ESPN team id. |
| `pos_team` | String | Team name in possession at the start of the play (offense, kickoff-aware). |
| `division` | String | Division in the conference for the team. |
| `conference` | String | Conference of the team. |
| `season` | Int64 | Season (4-digit year). |
| `player_id` | Int64 | ESPN player id from the roster entry. |
| `receiver_player_name` | String | Display name of the targeted receiver -- the FIRST participant in that role on the play. |
| `plays` | UInt32 | Total qualifying passing plays included in the WEPA calculation. |
| `games` | UInt32 | Number of games included in the ATS summary. |
| `team_games` | UInt32 | Games the team played, used as the per-game denominator. |
| `TEPA` | Float64 | Total EPA summed over every play. |
| `EPAplay` | Float64 | EPA generated per play. |
| `yards` | Int64 | Total yards gained on the drive. |
| `success` | Float64 | Success rate across the team plays. |
| `comp` | UInt32 | Completed passes. |
| `targets` | UInt32 |  |
| `passing_td` | Float64 | Passing touchdowns thrown. |
| `fumbles` | Float64 | Count of the receiver's targeted pass plays across the season whose play text mentions a fumble by either team. |
| `playsgame` | Float64 | Plays per game. |
| `EPAgame` | Float64 | EPA generated per game. |
| `yardsplay` | Float64 | Yards per play. |
| `yardsgame` | Float64 | Yards per game. |
| `catchpct` | Float64 | Catch rate on a 0-to-1 scale, receptions divided by targets for the season. |
| `dispersion_games` | Int64 | Games with at least one target by the receiver: the sample the dispersion columns (EPAplay_sd, EPAplay_p10, EPAplay_p90, boom_rate, bust_rate) are computed over. Those five are null below 3 games. |
| `EPAplay_sd` | Float64 | Population standard deviation (ddof=0) of the receiver's per-game EPA per play (a game's EPA summed over the receiver's targets, divided by those targets), across dispersion_games. Null when dispersion_games is below 3. |
| `EPAplay_p10` | Float64 | 10th percentile (the floor) of the receiver's per-game EPA per play (a game's EPA summed over the receiver's targets, divided by those targets), across dispersion_games; linear interpolation (R type 7). Null when dispersion_games is below 3. |
| `EPAplay_p90` | Float64 | 90th percentile (the ceiling) of the receiver's per-game EPA per play (a game's EPA summed over the receiver's targets, divided by those targets), across dispersion_games; linear interpolation (R type 7). Null when dispersion_games is below 3. |
| `boom_rate` | Float64 | Share of dispersion_games in which the receiver's EPA per play was more than one EPAplay_sd ABOVE their own unweighted per-game mean (not the play-weighted EPAplay). Strict, so a player whose games are all equal is neither a boom nor a bust. Null when dispersion_games is below 3. |
| `bust_rate` | Float64 | Share of dispersion_games in which the receiver's EPA per play was more than one EPAplay_sd BELOW their own unweighted per-game mean (not the play-weighted EPAplay). Strict, so a player whose games are all equal is neither a boom nor a bust. Null when dispersion_games is below 3. |
| `TEPA_rank` | Float64 | Rank of the receiver's total EPA summed over every play among receivers clearing the leaderboard minimum of 1.875 targets per team game, where 1 is best. |
| `EPAgame_rank` | Float64 | Rank of the receiver's EPA generated per game among receivers clearing the leaderboard minimum of 1.875 targets per team game, where 1 is best. |
| `EPAplay_rank` | Float64 | Rank of the receiver's EPA generated per play among receivers clearing the leaderboard minimum of 1.875 targets per team game, where 1 is best. |
| `success_rank` | Float64 | Rank of the receiver's success rate across their plays among receivers clearing the leaderboard minimum of 1.875 targets per team game, where 1 is best. |
| `comp_rank` | Float64 | Ordinal rank of the player's completions among qualifying players that season; ties share a fractional rank. |
| `targets_rank` | Float64 | Ordinal rank of the player's targets among qualifying players that season; ties share a fractional rank. |
| `catchpct_rank` | Float64 | Season rank of catchpct with the best catch rate first, computed only for receivers clearing the leaderboard minimum of 1.875 targets per team game and using averaged ranks for ties. |
| `yards_rank` | Float64 | Rank of the receiver's total yards among receivers clearing the leaderboard minimum of 1.875 targets per team game, where 1 is best. |
| `passing_td_rank` | Float64 | Ordinal rank of the player's passing touchdowns among qualifying players that season; ties share a fractional rank. |
| `fumbles_rank` | Float64 | Ordinal rank of the player's fumbles among qualifying players that season; ties share a fractional rank. |
| `yardsplay_rank` | Float64 | Rank of the receiver's yards per play among receivers clearing the leaderboard minimum of 1.875 targets per team game, where 1 is best. |
| `yardsgame_rank` | Float64 | Rank of the receiver's yards per game among receivers clearing the leaderboard minimum of 1.875 targets per team game, where 1 is best. |
| `boom_rate_rank` | Float64 | Rank of the receiver's boom rate (share of games more than one standard deviation above their own per-game EPA per play) among receivers clearing the leaderboard minimum of 1.875 targets per team game, where 1 is best. |
| `TEPA_pct` | Float64 | Percentile position (0-100) of the player's total EPA among qualifying players that season. |
| `EPAgame_pct` | Float64 | Percentile position (0-100) of the player's EPA per game among qualifying players that season. |
| `EPAplay_pct` | Float64 | Percentile position (0-100) of the player's EPA per play among qualifying players that season. |
| `success_pct` | Float64 | Percentile position (0-100) of the player's success rate among qualifying players that season. |
| `comp_pct` | Float64 | Percentile position (0-100) of the player's completions among qualifying players that season. |
| `targets_pct` | Float64 | Percentile position (0-100) of the player's targets among qualifying players that season. |
| `catchpct_pct` | Float64 | Percentile position (0-100) of the player's catch rate among qualifying players that season. |
| `yards_pct` | Float64 | Percentile position (0-100) of the player's yards among qualifying players that season. |
| `passing_td_pct` | Float64 | Percentile position (0-100) of the player's passing touchdowns among qualifying players that season. |
| `fumbles_pct` | Float64 | Percentile position (0-100) of the player's fumbles among qualifying players that season. |
| `yardsplay_pct` | Float64 | Percentile position (0-100) of the player's yards per play among qualifying players that season. |
| `yardsgame_pct` | Float64 | Percentile position (0-100) of the player's yards per game among qualifying players that season. |
| `boom_rate_pct` | Float64 | Percentile (0-100) of boom rate (share of games more than one standard deviation above their own per-game EPA per play) among receivers clearing the leaderboard minimum of 1.875 targets per team game, where 100 is best. Direction is already encoded in the matching rank, so a lower-is-better metric still scores 100 at its best. Null when the metric is null, and null rows are excluded from the denominator. |
| `EPAplay_n` | Int64 | Sample size behind EPAplay: the number of targets the receiver's value is computed over. 0 where EPAplay is null. |
| `success_n` | Int64 | Sample size behind success: the number of targets the receiver's value is computed over. 0 where success is null. |
| `yardsplay_n` | Int64 | Sample size behind yardsplay: the number of targets the receiver's value is computed over. 0 where yardsplay is null. |
| `catchpct_n` | Int64 | Sample size behind catchpct: the number of targets the receiver's value is computed over. 0 where catchpct is null. |
| `EPAgame_n` | Int64 | Sample size behind EPAgame: the number of games the receiver's value is computed over. 0 where EPAgame is null. |
| `yardsgame_n` | Int64 | Sample size behind yardsgame: the number of games the receiver's value is computed over. 0 where yardsgame is null. |
| `playsgame_n` | Int64 | Sample size behind playsgame: the number of games the receiver's value is computed over. 0 where playsgame is null. |
| `fbs_class` | String | Power/Group classification for the season: P4 or G6 from 2024 on, P5 or G5 through 2023, derived from conference membership. Null for teams outside FBS. |
| `position_group` | String | Position group from the season's ESPN roster (espn_cfb_rosters position_abbreviation): QB; RB (RB and FB); WR; TE; other for any other listed position. Null when the player is not on the season roster, is listed without a position ('-'), or is listed under two different groups. The cohort of the _pos_pct columns. |
| `TEPA_pos_pct` | Float64 | Percentile (0-100) of total EPA summed over every play among receivers clearing the leaderboard minimum of 1.875 targets per team game at the same position group (position_group), where 100 is best. Direction is already encoded in the matching rank, so a lower-is-better metric still scores 100 at its best. Null when the metric is null, when position_group is null, or when fewer than 10 qualifiers in the group have the metric. Rows with a null metric are excluded from the denominator. |
| `EPAgame_pos_pct` | Float64 | Percentile (0-100) of EPA generated per game among receivers clearing the leaderboard minimum of 1.875 targets per team game at the same position group (position_group), where 100 is best. Direction is already encoded in the matching rank, so a lower-is-better metric still scores 100 at its best. Null when the metric is null, when position_group is null, or when fewer than 10 qualifiers in the group have the metric. Rows with a null metric are excluded from the denominator. |
| `EPAplay_pos_pct` | Float64 | Percentile (0-100) of EPA generated per play among receivers clearing the leaderboard minimum of 1.875 targets per team game at the same position group (position_group), where 100 is best. Direction is already encoded in the matching rank, so a lower-is-better metric still scores 100 at its best. Null when the metric is null, when position_group is null, or when fewer than 10 qualifiers in the group have the metric. Rows with a null metric are excluded from the denominator. |
| `success_pos_pct` | Float64 | Percentile (0-100) of success rate across their plays among receivers clearing the leaderboard minimum of 1.875 targets per team game at the same position group (position_group), where 100 is best. Direction is already encoded in the matching rank, so a lower-is-better metric still scores 100 at its best. Null when the metric is null, when position_group is null, or when fewer than 10 qualifiers in the group have the metric. Rows with a null metric are excluded from the denominator. |
| `comp_pos_pct` | Float64 | Percentile (0-100) of completions among receivers clearing the leaderboard minimum of 1.875 targets per team game at the same position group (position_group), where 100 is best. Direction is already encoded in the matching rank, so a lower-is-better metric still scores 100 at its best. Null when the metric is null, when position_group is null, or when fewer than 10 qualifiers in the group have the metric. Rows with a null metric are excluded from the denominator. |
| `targets_pos_pct` | Float64 | Percentile (0-100) of targets among receivers clearing the leaderboard minimum of 1.875 targets per team game at the same position group (position_group), where 100 is best. Direction is already encoded in the matching rank, so a lower-is-better metric still scores 100 at its best. Null when the metric is null, when position_group is null, or when fewer than 10 qualifiers in the group have the metric. Rows with a null metric are excluded from the denominator. |
| `catchpct_pos_pct` | Float64 | Percentile (0-100) of catch rate among receivers clearing the leaderboard minimum of 1.875 targets per team game at the same position group (position_group), where 100 is best. Direction is already encoded in the matching rank, so a lower-is-better metric still scores 100 at its best. Null when the metric is null, when position_group is null, or when fewer than 10 qualifiers in the group have the metric. Rows with a null metric are excluded from the denominator. |
| `yards_pos_pct` | Float64 | Percentile (0-100) of total yards among receivers clearing the leaderboard minimum of 1.875 targets per team game at the same position group (position_group), where 100 is best. Direction is already encoded in the matching rank, so a lower-is-better metric still scores 100 at its best. Null when the metric is null, when position_group is null, or when fewer than 10 qualifiers in the group have the metric. Rows with a null metric are excluded from the denominator. |
| `passing_td_pos_pct` | Float64 | Percentile (0-100) of passing touchdowns among receivers clearing the leaderboard minimum of 1.875 targets per team game at the same position group (position_group), where 100 is best. Direction is already encoded in the matching rank, so a lower-is-better metric still scores 100 at its best. Null when the metric is null, when position_group is null, or when fewer than 10 qualifiers in the group have the metric. Rows with a null metric are excluded from the denominator. |
| `fumbles_pos_pct` | Float64 | Percentile (0-100) of fumbles among receivers clearing the leaderboard minimum of 1.875 targets per team game at the same position group (position_group), where 100 is best. Direction is already encoded in the matching rank, so a lower-is-better metric still scores 100 at its best. Null when the metric is null, when position_group is null, or when fewer than 10 qualifiers in the group have the metric. Rows with a null metric are excluded from the denominator. |
| `yardsplay_pos_pct` | Float64 | Percentile (0-100) of yards per play among receivers clearing the leaderboard minimum of 1.875 targets per team game at the same position group (position_group), where 100 is best. Direction is already encoded in the matching rank, so a lower-is-better metric still scores 100 at its best. Null when the metric is null, when position_group is null, or when fewer than 10 qualifiers in the group have the metric. Rows with a null metric are excluded from the denominator. |
| `yardsgame_pos_pct` | Float64 | Percentile (0-100) of yards per game among receivers clearing the leaderboard minimum of 1.875 targets per team game at the same position group (position_group), where 100 is best. Direction is already encoded in the matching rank, so a lower-is-better metric still scores 100 at its best. Null when the metric is null, when position_group is null, or when fewer than 10 qualifiers in the group have the metric. Rows with a null metric are excluded from the denominator. |
| `boom_rate_pos_pct` | Float64 | Percentile (0-100) of boom rate (share of games more than one standard deviation above their own per-game EPA per play) among receivers clearing the leaderboard minimum of 1.875 targets per team game at the same position group (position_group), where 100 is best. Direction is already encoded in the matching rank, so a lower-is-better metric still scores 100 at its best. Null when the metric is null, when position_group is null, or when fewer than 10 qualifiers in the group have the metric. Rows with a null metric are excluded from the denominator. |

```python
load_cfb_receiving(seasons=2024)
```

## load_cfb_rushing

Release: [espn_cfb_rushing](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_rushing) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_rushing/cfb_rushing_{season}.parquet`
### Returns {#load_cfb_rushing-returns}

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | ESPN team id. |
| `pos_team` | String | Team name in possession at the start of the play (offense, kickoff-aware). |
| `division` | String | Division in the conference for the team. |
| `conference` | String | Conference of the team. |
| `season` | Int64 | Season (4-digit year). |
| `player_id` | Int64 | ESPN player id from the roster entry. |
| `rusher_player_name` | String | Display name of the ball carrier on a rush -- the FIRST participant in that role on the play. |
| `plays` | UInt32 | Total qualifying passing plays included in the WEPA calculation. |
| `games` | UInt32 | Number of games included in the ATS summary. |
| `team_games` | UInt32 | Games the team played, used as the per-game denominator. |
| `TEPA` | Float64 | Total EPA summed over every play. |
| `EPAplay` | Float64 | EPA generated per play. |
| `yards` | Int64 | Total yards gained on the drive. |
| `success` | Float64 | Success rate across the team plays. |
| `rushing_td` | Float64 | Rushing touchdowns. |
| `fumbles` | Float64 | Count of the ball carrier's rush attempts across the season whose play text mentions a fumble by either team. |
| `playsgame` | Float64 | Plays per game. |
| `EPAgame` | Float64 | EPA generated per game. |
| `yardsplay` | Float64 | Yards per play. |
| `yardsgame` | Float64 | Yards per game. |
| `dispersion_games` | Int64 | Games with at least one carry by the rusher: the sample the dispersion columns (EPAplay_sd, EPAplay_p10, EPAplay_p90, boom_rate, bust_rate) are computed over. Those five are null below 3 games. |
| `EPAplay_sd` | Float64 | Population standard deviation (ddof=0) of the rusher's per-game EPA per play (a game's EPA summed over the rusher's carrys, divided by those carrys), across dispersion_games. Null when dispersion_games is below 3. |
| `EPAplay_p10` | Float64 | 10th percentile (the floor) of the rusher's per-game EPA per play (a game's EPA summed over the rusher's carrys, divided by those carrys), across dispersion_games; linear interpolation (R type 7). Null when dispersion_games is below 3. |
| `EPAplay_p90` | Float64 | 90th percentile (the ceiling) of the rusher's per-game EPA per play (a game's EPA summed over the rusher's carrys, divided by those carrys), across dispersion_games; linear interpolation (R type 7). Null when dispersion_games is below 3. |
| `boom_rate` | Float64 | Share of dispersion_games in which the rusher's EPA per play was more than one EPAplay_sd ABOVE their own unweighted per-game mean (not the play-weighted EPAplay). Strict, so a player whose games are all equal is neither a boom nor a bust. Null when dispersion_games is below 3. |
| `bust_rate` | Float64 | Share of dispersion_games in which the rusher's EPA per play was more than one EPAplay_sd BELOW their own unweighted per-game mean (not the play-weighted EPAplay). Strict, so a player whose games are all equal is neither a boom nor a bust. Null when dispersion_games is below 3. |
| `line_yards_share` | Float64 | Share of the rusher's carries that gained 4 or fewer yards, losses included (the line-yards tier of Football Outsiders' cut-points), on the carry's own yds_rushed (penalty yardage excluded), over carries with a recorded yds_rushed; line_yards_share + second_level_share + open_field_share = 1. |
| `second_level_share` | Float64 | Share of the rusher's carries that gained 5 to 10 yards (the second-level tier), on the carry's own yds_rushed (penalty yardage excluded), over carries with a recorded yds_rushed; line_yards_share + second_level_share + open_field_share = 1. |
| `open_field_share` | Float64 | Share of the rusher's carries that gained 11 or more yards (the open-field tier), on the carry's own yds_rushed (penalty yardage excluded), over carries with a recorded yds_rushed; line_yards_share + second_level_share + open_field_share = 1. |
| `stuff_rate` | Float64 | Share of the rusher's carries for 0 or fewer yards, over carries with a recorded yds_rushed. A stuff is also a line-yards-tier carry. |
| `EPAplay_one_score` | Float64 | EPA per carry on the rusher's carries with the offense within 8 points either way at the snap (pos_score_diff_start, the margin before the play). Null when there were none. |
| `EPAplay_one_score_n` | Int64 | Sample size behind EPAplay_one_score: the number of carries with the offense within 8 points either way at the snap the rusher's value is computed over. 0 where EPAplay_one_score is null. |
| `EPAplay_not_one_score` | Float64 | EPA per carry on the rusher's carries with the offense more than 8 points ahead or behind at the snap (pos_score_diff_start). Null when there were none. |
| `EPAplay_not_one_score_n` | Int64 | Sample size behind EPAplay_not_one_score: the number of carries with the offense more than 8 points ahead or behind at the snap the rusher's value is computed over. 0 where EPAplay_not_one_score is null. |
| `TEPA_rank` | Float64 | Rank of the rusher's total EPA summed over every play among rushers clearing the leaderboard minimum of 6.25 plays per team game, where 1 is best. |
| `EPAgame_rank` | Float64 | Rank of the rusher's EPA generated per game among rushers clearing the leaderboard minimum of 6.25 plays per team game, where 1 is best. |
| `EPAplay_rank` | Float64 | Rank of the rusher's EPA generated per play among rushers clearing the leaderboard minimum of 6.25 plays per team game, where 1 is best. |
| `success_rank` | Float64 | Rank of the rusher's success rate across their plays among rushers clearing the leaderboard minimum of 6.25 plays per team game, where 1 is best. |
| `plays_rank` | Float64 | Ordinal rank of the player's plays among qualifying players that season; ties share a fractional rank. |
| `yards_rank` | Float64 | Rank of the rusher's total yards among rushers clearing the leaderboard minimum of 6.25 plays per team game, where 1 is best. |
| `rushing_td_rank` | Float64 | Ordinal rank of the player's rushing touchdowns among qualifying players that season; ties share a fractional rank. |
| `fumbles_rank` | Float64 | Ordinal rank of the player's fumbles among qualifying players that season; ties share a fractional rank. |
| `yardsplay_rank` | Float64 | Rank of the rusher's yards per play among rushers clearing the leaderboard minimum of 6.25 plays per team game, where 1 is best. |
| `yardsgame_rank` | Float64 | Rank of the rusher's yards per game among rushers clearing the leaderboard minimum of 6.25 plays per team game, where 1 is best. |
| `boom_rate_rank` | Float64 | Rank of the rusher's boom rate (share of games more than one standard deviation above their own per-game EPA per play) among rushers clearing the leaderboard minimum of 6.25 plays per team game, where 1 is best. |
| `stuff_rate_rank` | Float64 | Rank of the rusher's stuff rate (share of carries for 0 or fewer yards) among rushers clearing the leaderboard minimum of 6.25 plays per team game, where 1 is best. |
| `TEPA_pct` | Float64 | Percentile position (0-100) of the player's total EPA among qualifying players that season. |
| `EPAgame_pct` | Float64 | Percentile position (0-100) of the player's EPA per game among qualifying players that season. |
| `EPAplay_pct` | Float64 | Percentile position (0-100) of the player's EPA per play among qualifying players that season. |
| `success_pct` | Float64 | Percentile position (0-100) of the player's success rate among qualifying players that season. |
| `plays_pct` | Float64 | Percentile position (0-100) of the player's plays among qualifying players that season. |
| `yards_pct` | Float64 | Percentile position (0-100) of the player's yards among qualifying players that season. |
| `rushing_td_pct` | Float64 | Percentile position (0-100) of the player's rushing touchdowns among qualifying players that season. |
| `fumbles_pct` | Float64 | Percentile position (0-100) of the player's fumbles among qualifying players that season. |
| `yardsplay_pct` | Float64 | Percentile position (0-100) of the player's yards per play among qualifying players that season. |
| `yardsgame_pct` | Float64 | Percentile position (0-100) of the player's yards per game among qualifying players that season. |
| `boom_rate_pct` | Float64 | Percentile (0-100) of boom rate (share of games more than one standard deviation above their own per-game EPA per play) among rushers clearing the leaderboard minimum of 6.25 plays per team game, where 100 is best. Direction is already encoded in the matching rank, so a lower-is-better metric still scores 100 at its best. Null when the metric is null, and null rows are excluded from the denominator. |
| `stuff_rate_pct` | Float64 | Percentile (0-100) of stuff rate (share of carries for 0 or fewer yards) among rushers clearing the leaderboard minimum of 6.25 plays per team game, where 100 is best. Direction is already encoded in the matching rank, so a lower-is-better metric still scores 100 at its best. Null when the metric is null, and null rows are excluded from the denominator. |
| `EPAplay_n` | Int64 | Sample size behind EPAplay: the number of carries the rusher's value is computed over. 0 where EPAplay is null. |
| `success_n` | Int64 | Sample size behind success: the number of carries the rusher's value is computed over. 0 where success is null. |
| `yardsplay_n` | Int64 | Sample size behind yardsplay: the number of carries the rusher's value is computed over. 0 where yardsplay is null. |
| `EPAgame_n` | Int64 | Sample size behind EPAgame: the number of games the rusher's value is computed over. 0 where EPAgame is null. |
| `yardsgame_n` | Int64 | Sample size behind yardsgame: the number of games the rusher's value is computed over. 0 where yardsgame is null. |
| `playsgame_n` | Int64 | Sample size behind playsgame: the number of games the rusher's value is computed over. 0 where playsgame is null. |
| `fbs_class` | String | Power/Group classification for the season: P4 or G6 from 2024 on, P5 or G5 through 2023, derived from conference membership. Null for teams outside FBS. |
| `position_group` | String | Position group from the season's ESPN roster (espn_cfb_rosters position_abbreviation): QB; RB (RB and FB); WR; TE; other for any other listed position. Null when the player is not on the season roster, is listed without a position ('-'), or is listed under two different groups. The cohort of the _pos_pct columns. |
| `TEPA_pos_pct` | Float64 | Percentile (0-100) of total EPA summed over every play among rushers clearing the leaderboard minimum of 6.25 plays per team game at the same position group (position_group), where 100 is best. Direction is already encoded in the matching rank, so a lower-is-better metric still scores 100 at its best. Null when the metric is null, when position_group is null, or when fewer than 10 qualifiers in the group have the metric. Rows with a null metric are excluded from the denominator. |
| `EPAgame_pos_pct` | Float64 | Percentile (0-100) of EPA generated per game among rushers clearing the leaderboard minimum of 6.25 plays per team game at the same position group (position_group), where 100 is best. Direction is already encoded in the matching rank, so a lower-is-better metric still scores 100 at its best. Null when the metric is null, when position_group is null, or when fewer than 10 qualifiers in the group have the metric. Rows with a null metric are excluded from the denominator. |
| `EPAplay_pos_pct` | Float64 | Percentile (0-100) of EPA generated per play among rushers clearing the leaderboard minimum of 6.25 plays per team game at the same position group (position_group), where 100 is best. Direction is already encoded in the matching rank, so a lower-is-better metric still scores 100 at its best. Null when the metric is null, when position_group is null, or when fewer than 10 qualifiers in the group have the metric. Rows with a null metric are excluded from the denominator. |
| `success_pos_pct` | Float64 | Percentile (0-100) of success rate across their plays among rushers clearing the leaderboard minimum of 6.25 plays per team game at the same position group (position_group), where 100 is best. Direction is already encoded in the matching rank, so a lower-is-better metric still scores 100 at its best. Null when the metric is null, when position_group is null, or when fewer than 10 qualifiers in the group have the metric. Rows with a null metric are excluded from the denominator. |
| `plays_pos_pct` | Float64 | Percentile (0-100) of plays among rushers clearing the leaderboard minimum of 6.25 plays per team game at the same position group (position_group), where 100 is best. Direction is already encoded in the matching rank, so a lower-is-better metric still scores 100 at its best. Null when the metric is null, when position_group is null, or when fewer than 10 qualifiers in the group have the metric. Rows with a null metric are excluded from the denominator. |
| `yards_pos_pct` | Float64 | Percentile (0-100) of total yards among rushers clearing the leaderboard minimum of 6.25 plays per team game at the same position group (position_group), where 100 is best. Direction is already encoded in the matching rank, so a lower-is-better metric still scores 100 at its best. Null when the metric is null, when position_group is null, or when fewer than 10 qualifiers in the group have the metric. Rows with a null metric are excluded from the denominator. |
| `rushing_td_pos_pct` | Float64 | Percentile (0-100) of rushing touchdowns among rushers clearing the leaderboard minimum of 6.25 plays per team game at the same position group (position_group), where 100 is best. Direction is already encoded in the matching rank, so a lower-is-better metric still scores 100 at its best. Null when the metric is null, when position_group is null, or when fewer than 10 qualifiers in the group have the metric. Rows with a null metric are excluded from the denominator. |
| `fumbles_pos_pct` | Float64 | Percentile (0-100) of fumbles among rushers clearing the leaderboard minimum of 6.25 plays per team game at the same position group (position_group), where 100 is best. Direction is already encoded in the matching rank, so a lower-is-better metric still scores 100 at its best. Null when the metric is null, when position_group is null, or when fewer than 10 qualifiers in the group have the metric. Rows with a null metric are excluded from the denominator. |
| `yardsplay_pos_pct` | Float64 | Percentile (0-100) of yards per play among rushers clearing the leaderboard minimum of 6.25 plays per team game at the same position group (position_group), where 100 is best. Direction is already encoded in the matching rank, so a lower-is-better metric still scores 100 at its best. Null when the metric is null, when position_group is null, or when fewer than 10 qualifiers in the group have the metric. Rows with a null metric are excluded from the denominator. |
| `yardsgame_pos_pct` | Float64 | Percentile (0-100) of yards per game among rushers clearing the leaderboard minimum of 6.25 plays per team game at the same position group (position_group), where 100 is best. Direction is already encoded in the matching rank, so a lower-is-better metric still scores 100 at its best. Null when the metric is null, when position_group is null, or when fewer than 10 qualifiers in the group have the metric. Rows with a null metric are excluded from the denominator. |
| `boom_rate_pos_pct` | Float64 | Percentile (0-100) of boom rate (share of games more than one standard deviation above their own per-game EPA per play) among rushers clearing the leaderboard minimum of 6.25 plays per team game at the same position group (position_group), where 100 is best. Direction is already encoded in the matching rank, so a lower-is-better metric still scores 100 at its best. Null when the metric is null, when position_group is null, or when fewer than 10 qualifiers in the group have the metric. Rows with a null metric are excluded from the denominator. |
| `stuff_rate_pos_pct` | Float64 | Percentile (0-100) of stuff rate (share of carries for 0 or fewer yards) among rushers clearing the leaderboard minimum of 6.25 plays per team game at the same position group (position_group), where 100 is best. Direction is already encoded in the matching rank, so a lower-is-better metric still scores 100 at its best. Null when the metric is null, when position_group is null, or when fewer than 10 qualifiers in the group have the metric. Rows with a null metric are excluded from the denominator. |

```python
load_cfb_rushing(seasons=2024)
```

## load_cfb_ratings_weekly

Release: [cfb_ratings_weekly](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfb_ratings_weekly) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/cfb_ratings_weekly/cfb_ratings_weekly_{season}.parquet`
### Returns {#load_cfb_ratings_weekly-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season (4-digit year). |
| `team_id` | Int64 | ESPN team id. |
| `adj_off_epa` | Float64 | Opponent-adjusted offensive EPA per play as of the snapshot week: raw per-game EPA on pass and rush plays net of each opponent's ridge-fitted defensive strength. |
| `adj_def_epa` | Float64 | Opponent-adjusted EPA per play allowed as of the snapshot week, netted the same way as the offensive rating, so lower is better. |
| `adj_st_epa` | Float64 | Special-teams composite in EPA units as of the snapshot week, summing the league-centered per-play EPA of the field goal, punt, and kick-return units. |
| `adj_net` | Float64 | adj_off_epa minus adj_def_epa at the snapshot week, the team's overall efficiency rating in EPA per play with special teams excluded. |
| `fei_off` | Float64 | Drive-level offensive rating at the snapshot week, from a ridge fit on per-drive EPA. |
| `fei_def` | Float64 | Drive-level defensive rating at the snapshot week, from the same per-drive ridge fit and on the same scale as fei_off. |
| `fei_net` | Float64 | fei_off minus fei_def at the snapshot week, the team's overall drive-efficiency rating. |
| `games` | Int64 | Number of games included in the ATS summary. |
| `off_pace` | Float64 | Scrimmage plays per game through the snapshot week, the tempo input consumed by the totals model. |
| `off_rank` | Int64 | Dense rank of adj_off_epa in descending order within the snapshot week, so rank 1 is the most efficient offense at that point. |
| `def_rank` | Int64 | Dense rank of adj_def_epa in ascending order within the snapshot week, so rank 1 is the stingiest defense at that point. |
| `net_rank` | Int64 | Dense rank of adj_net in descending order within the snapshot week, so rank 1 is the strongest overall team at that point. |
| `net_z` | Float64 | adj_net restated as a z-score against the mean and standard deviation of adj_net across the teams rated in that snapshot week. |
| `fei_off_rank` | Int64 | National rank of fei_off at the snapshot week: dense rank, descending, 1 = highest offensive drive-efficiency rating. |
| `fei_def_rank` | Int64 | National rank of fei_def at the snapshot week: dense rank, ascending -- fewer drive EPA allowed ranks better, 1 = best defense. |
| `fei_net_rank` | Int64 | National rank of fei_net at the snapshot week: dense rank, descending, 1 = highest net drive-efficiency rating. |
| `through_week` | Int32 | Regular-season week the snapshot runs through; the ratings were refit using only games kicking off on or before that week's final kickoff date. |

```python
load_cfb_ratings_weekly(seasons=2024)
```

## load_cfb_groups

Release: [cfb_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfb_groups) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/cfb_groups/cfb_groups.parquet`

:::caution[Coverage]
One season-less file: one row per group lineage (the league, subdivisions, conferences, divisions) with the first and last season it had members. group_id is SDV's own id (e.g. cfb:big-ten) and names a lineage: a rename that keeps continuity keeps the id, a new body gets a new one, and notes records each call. Seasons are the STARTING year (2025 = the fall 2025 season).
:::

### Returns {#load_cfb_groups-returns}

| col_name | type | description |
|---|---|---|
| `league` | String | League code of the table ("cfb"); the prefix of every group_id in it. |
| `group_id` | String | SDV group id, {league}:{slug}. It names a lineage: renames that keep continuity keep the id, and a new body (a new conference, or a merger the sources treat as new) gets a new one. |
| `level` | String | Hierarchy level of the group: "league", "subdivision", "conference" or "division". |
| `first_season` | Int32 | First season in which the group had at least one member (STARTING year: 2025 = the fall 2025 season). |
| `last_season` | Int32 | Last season in which the group had at least one member (STARTING year: 2025 = the fall 2025 season). |
| `notes` | String | Builder notes on the group: the lineage decisions behind its group_id and any source caveats. |

```python
load_cfb_groups()
```

## load_cfb_group_seasons

Release: [cfb_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfb_groups) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/cfb_groups/cfb_group_seasons.parquet`

:::caution[Coverage]
One season-less file: one row per group per season it existed, with its name, short name, abbreviation and parent group AS OF that season (never today's label applied to the past) and its member count. season is the STARTING year (2025 = the fall 2025 season).
:::

### Returns {#load_cfb_group_seasons-returns}

| col_name | type | description |
|---|---|---|
| `league` | String | League code of the table ("cfb"); the prefix of every group_id in it. |
| `group_id` | String | SDV group id, {league}:{slug}. It names a lineage: renames that keep continuity keep the id, and a new body (a new conference, or a merger the sources treat as new) gets a new one. |
| `season` | Int32 | Season the row describes (STARTING year: 2025 = the fall 2025 season). |
| `level` | String | Hierarchy level of the group: "league", "subdivision", "conference" or "division". |
| `name` | String | Full name of the group as of that season -- the label in use then, not today's name. |
| `short_name` | String | Short display name of the group as of that season. |
| `abbreviation` | String | Abbreviation of the group as of that season. |
| `parent_group_id` | String | group_id one level up as of that season (division -> conference -> subdivision -> league); null at the top level or where no higher group applied that season. |
| `n_teams` | Int32 | Number of member teams in the group that season. |

```python
load_cfb_group_seasons()
```

## load_cfb_group_aliases

Release: [cfb_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfb_groups) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/cfb_groups/cfb_group_aliases.parquet`

:::caution[Coverage]
One season-less file: every name, abbreviation, slug and source id that a source (cfbd, espn, sdv) uses for a group, each with the seasons it is valid for (valid_from / valid_to, inclusive; null = unbounded). Match a source's conference or division label here to reach group_id.
:::

### Returns {#load_cfb_group_aliases-returns}

| col_name | type | description |
|---|---|---|
| `league` | String | League code of the table ("cfb"); the prefix of every group_id in it. |
| `group_id` | String | SDV group id, {league}:{slug}. It names a lineage: renames that keep continuity keep the id, and a new body (a new conference, or a merger the sources treat as new) gets a new one. |
| `source` | String | Source that uses this label or id (in this table: cfbd, espn, sdv); "sdv" marks SDV's own labels. |
| `source_id` | String | The source's own id for the group (ESPN group id, NCAA conf_id, CFBD id, MLB division id) when it has one; null otherwise. |
| `name_kind` | String | Kind of label in value: "name", "short_name", "abbreviation", "slug" or "code". |
| `value` | String | The label exactly as the source writes it; match a source's conference or division label against it to reach group_id. |
| `valid_from` | Int32 | First season the alias is valid for, inclusive (STARTING year: 2025 = the fall 2025 season); null = unbounded. |
| `valid_to` | Int32 | Last season the alias is valid for, inclusive (STARTING year: 2025 = the fall 2025 season); null = unbounded (still in use). |

```python
load_cfb_group_aliases()
```
