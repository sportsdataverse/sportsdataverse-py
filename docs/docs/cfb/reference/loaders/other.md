---
title: "CFB dataset loaders — Other"
sidebar_label: "Other"
sidebar_position: 15
description: "CFB dataset loaders — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# CFB dataset loaders — Other

## load_cfb_ratings

Release: [cfb_ratings](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfb_ratings) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/cfb_ratings/cfb_ratings_{season}.parquet`
### Returns {#load_cfb_ratings-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season (4-digit year). |
| `team_id` | Int64 | ESPN team id. |
| `adj_off_epa` | Float64 | Opponent-adjusted offensive EPA per play: the team's raw per-game EPA on pass and rush plays net of each opponent's ridge-fitted defensive strength, averaged over its games. |
| `adj_def_epa` | Float64 | Opponent-adjusted EPA per play allowed, netted the same way as the offensive rating, so lower is better because it measures EPA surrendered. |
| `adj_st_epa` | Float64 | Special-teams composite in EPA units: per-play mean EPA on field goals, punts, and kick returns, each centered on that unit's league mean and summed across the three units. |
| `adj_net` | Float64 | adj_off_epa minus adj_def_epa, the team's overall efficiency rating in EPA per play; special teams is deliberately excluded. |
| `fei_off` | Float64 | Drive-level offensive rating from a ridge fit on per-drive EPA, the Fremeau-style drive-efficiency counterpart to adj_off_epa. |
| `fei_def` | Float64 | Drive-level defensive rating from the same per-drive ridge fit, on the same scale as fei_off. |
| `fei_net` | Float64 | fei_off minus fei_def, the team's overall drive-efficiency rating, with the ridge's dropped reference team pinned at zero. |
| `games` | Int64 | Number of games included in the ATS summary. |
| `off_pace` | Float64 | Tempo measure: scrimmage plays (pass plus rush) per game, centering near 65 and used as the pace input to the totals model. |
| `off_rank` | Int64 | Dense rank of adj_off_epa in descending order, so rank 1 is the season's most efficient offense. |
| `def_rank` | Int64 | Dense rank of adj_def_epa in ascending order, so rank 1 is the season's stingiest defense. |
| `net_rank` | Int64 | Dense rank of adj_net in descending order, so rank 1 is the season's strongest overall team. |
| `net_z` | Float64 | adj_net restated as a z-score against the mean and standard deviation of adj_net across the rated teams that season. |
| `fei_off_rank` | Int64 | Dense rank of fei_off in descending order, so rank 1 is the season's most efficient drive offense. |
| `fei_def_rank` | Int64 | Dense rank of fei_def in ascending order, so rank 1 is the season's stingiest drive defense. |
| `fei_net_rank` | Int64 | Dense rank of fei_net in descending order, so rank 1 is the season's strongest overall drive-efficiency team. |

```python
load_cfb_ratings(seasons=2024)
```

## load_cfb_recruiting_proj

Release: [cfb_recruiting_proj](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfb_recruiting_proj) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/cfb_recruiting_proj/cfb_recruiting_proj_{season}.parquet`
### Returns {#load_cfb_recruiting_proj-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season (4-digit year). |
| `team_id` | Int64 | ESPN team id. |
| `pred_wins` | Float64 | Ridge projection of the team's season win total, fit strictly on prior seasons from talent composite, blue-chip ratio, offensive and defensive returning production, and prior wins. |
| `pred_margin` | Float64 | Ridge projection of the team's average per-game scoring margin, from the same preseason-known feature set as pred_wins. |
| `pred_net_epa` | Float64 | Reserved slot for a projected adjusted net EPA; it ships all-null because the adjusted-EPA training target is not currently loadable. |

```python
load_cfb_recruiting_proj(seasons=2024)
```

## load_cfb_recruits

Release: [cfb_recruits](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfb_recruits) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/cfb_recruits/cfb_recruits_{season}.parquet`
### Returns {#load_cfb_recruits-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season (4-digit year). |
| `team_id` | Int64 | ESPN team id. |
| `team_id_247` | String | 247Sports' own team key for the recruit's committed or signed school; not interchangeable with the ESPN/CFBD team id. |
| `team` | String | Team name. |
| `recruit_id` | String | ESPN recruit id. |
| `player_name` | String | Full name of player |
| `stars` | Int64 | Recruit star rating on the 247Sports scale (2-5). |
| `grade` | Float64 | ESPN recruit grade (0-100; `0` = not rated). |
| `position` | String | Athlete position. |

```python
load_cfb_recruits(seasons=2024)
```

## load_cfb_returning_production

Release: [cfb_returning_production](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfb_returning_production) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/cfb_returning_production/cfb_returning_production_{season}.parquet`
### Returns {#load_cfb_returning_production-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season (4-digit year). |
| `team_id` | Int64 | ESPN team id. |
| `off_returning` | Float64 | Share of the team's prior-season offensive production returning for the season. |
| `def_returning` | Float64 | Share of the team's prior-season defensive production returning for the season. |
| `overall_returning` | Float64 | Usage-weighted blend of the offensive and defensive returning-production shares. |
| `n_returning` | Int64 | Number of returning players counted in the returning-production calculation. |
| `def_basis` | String | Defensive measure behind def_returning: "participants" when the production season is 2014+ (tackle volume -- tackles, assists, tackles for loss, shared sacks, passes defended -- from load_cfb_play_participants), "pbp_splash" for 2004-2013 (sacks, interceptions, pass breakups and forced fumbles from play-by-play ids; no tackle volume, so not on the same scale), "box" when that source's release is missing and the ESPN player box stood in; null when def_returning is null. |
| `overall_basis` | String | "offense+defense" when overall_returning is the usage-weighted blend of off_returning and def_returning; "offense" when the team has no defensive value (e.g. production season 2003), so overall_returning equals off_returning. |
| `is_estimated` | Boolean | Whether the returning-production figures for the row were estimated rather than derived from complete data. |

```python
load_cfb_returning_production(seasons=2024)
```

## load_cfb_player_box

Release: [espn_cfb_player_box](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_player_box) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_player_box/player_box_{season}.parquet`
### Returns {#load_cfb_player_box-returns}

| col_name | type | description |
|---|---|---|
| `stat_1` | String | First value of ESPN's raw athlete stats array, written only when the category's key list does not line up with the stats list; on those rows the named per-category columns are all null. |
| `stat_2` | String | Second value of ESPN's raw athlete stats array, written only on rows where the category keys did not line up and the named columns could not be filled. |
| `stat_3` | String | Third value of ESPN's raw athlete stats array, written only on rows where the category keys did not line up and the named columns could not be filled. |
| `stat_4` | String | Fourth value of ESPN's raw athlete stats array, written only on rows where the category keys did not line up and the named columns could not be filled. |
| `stat_5` | String | Fifth value of ESPN's raw athlete stats array, written only on rows where the category keys did not line up and the named columns could not be filled. |
| `category` | String | CFBD stats category name (e.g. passing, rushing, defensive). |
| `athlete_id` | Int64 | ESPN athlete id. |
| `athlete_name` | String | Player full name. |
| `jersey` | String | Jersey number. |
| `team_id` | Int64 | ESPN team id. |
| `rushingAttempts` | String | Rushing attempts. |
| `rushingYards` | String | Net rushing yards gained. |
| `yardsPerRushAttempt` | String | Yards gained per rushing attempt. |
| `rushingTouchdowns` | String | Rushing touchdowns. |
| `longRushing` | String | Longest rush of the game, in yards. |
| `receptions` | String | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `receivingYards` | String | Receiving yards gained. |
| `yardsPerReception` | String | Yards gained per reception. |
| `receivingTouchdowns` | String | Receiving touchdowns. |
| `longReception` | String | Longest reception of the game, in yards. |
| `totalTackles` | String | Player's total tackles, solo plus assisted (ESPN box score). |
| `soloTackles` | String | Player's solo (unassisted) tackles (ESPN box score). |
| `sacks` | String | Sacks credited to the player (ESPN box score). |
| `tacklesForLoss` | String | Player's tackles made behind the line of scrimmage (ESPN box score). |
| `passesDefended` | String | Passes the player broke up or defended (ESPN box score). |
| `hurries` | String | Quarterback hurries credited to the player (ESPN box score). |
| `defensiveTouchdowns` | String | Touchdowns the player scored on defense (ESPN box score). |
| `interceptions` | String | Passing interceptions. |
| `interceptionYards` | String | Yards returned on interceptions. |
| `interceptionTouchdowns` | String | Touchdowns scored on interception returns. |
| `kickReturns` | String | Number of kickoff returns by the player (ESPN box score). |
| `kickReturnYards` | String | Total kickoff-return yards by the player (ESPN box score). |
| `yardsPerKickReturn` | String | Average yards per kickoff return for the player (ESPN box score). |
| `longKickReturn` | String | Player's longest kickoff return of the game, in yards (ESPN box score). |
| `kickReturnTouchdowns` | String | Kickoff returns the player took for touchdowns (ESPN box score). |
| `fieldGoalsMade/fieldGoalAttempts` | String | Field goals made and attempted, as ESPN's combined string. |
| `fieldGoalPct` | String | Field-goal percentage. |
| `longFieldGoalMade` | String | Longest field goal made, in yards. |
| `extraPointsMade/extraPointAttempts` | String | Extra points made and attempted, as ESPN's combined string. |
| `totalKickingPoints` | String | Total points scored by kicking. |
| `punts` | String | Punts attempted. |
| `puntYards` | String | Total punt yards. |
| `grossAvgPuntYards` | String | Gross average yards per punt, before return yardage. |
| `touchbacks` | String | Punts or kickoffs that resulted in a touchback. |
| `puntsInside20` | String | Punts downed inside the opponent 20-yard line. |
| `longPunt` | String | Longest punt of the game, in yards. |
| `puntReturns` | String | Punt returns attempted. |
| `puntReturnYards` | String | Yards gained on punt returns. |
| `yardsPerPuntReturn` | String | Yards gained per punt return. |
| `longPuntReturn` | String | Longest punt return of the game, in yards. |
| `puntReturnTouchdowns` | String | Touchdowns scored on punt returns. |
| `game_id` | Int64 | ESPN game identifier. |
| `season` | Int64 | Season (4-digit year). |
| `fumbles` | String | Number of times the player fumbled in the game (ESPN box score). |
| `fumblesLost` | String | Number of the player's fumbles lost to the opponent (ESPN box score). |
| `fumblesRecovered` | String | Number of fumbles the player recovered (ESPN box score). |
| `completions/passingAttempts` | String | Completions and pass attempts, as ESPN's combined string. |
| `passingYards` | String | Net passing yards gained. |
| `yardsPerPassAttempt` | String | Yards gained per pass attempt. |
| `passingTouchdowns` | String | Passing touchdowns. |
| `adjQBR` | String | Adjusted Total QBR for the quarterback. |

```python
load_cfb_player_box(seasons=2024)
```

## load_cfb_drives

Release: [espn_cfb_drives](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_drives) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_drives/drives_{season}.parquet`
### Returns {#load_cfb_drives-returns}

| col_name | type | description |
|---|---|---|
| `drive_id` | String | CFBD drive identifier the play belongs to. |
| `team_id` | Int64 | ESPN team id. |
| `result` | String | Drive result code (e.g. `PUNT`, `TD`). |
| `display_result` | String | Drive-result label (e.g. `Punt`, `Touchdown`). |
| `short_display_result` | String | Short drive-result label. |
| `description` | String | ESPN's description of the stat. |
| `yards` | Int64 | Total yards gained on the drive. |
| `offensive_plays` | Int64 | Number of offensive plays on the drive. |
| `is_score` | Boolean | `TRUE` if the drive resulted in a score. |
| `start_period` | Int64 | Period (quarter) in which the drive starts. |
| `start_yard_line` | Int64 | Yard line at the start of the play. |
| `start_clock` | String | Game clock display value at the start of the drive. |
| `start_text` | String | Field-position text at the start of the drive. |
| `end_period` | Int64 | Period (quarter) in which the drive ends. |
| `end_yard_line` | Int64 | Yard line at the end of the play. |
| `end_clock` | String | Game clock display value at the end of the drive. |
| `time_elapsed` | String | Elapsed game time for the drive (`MM:SS`). |
| `n_plays` | Int64 | Number of entries in ESPN's raw plays array for the drive, which is generally at least offensive_plays because it also counts penalties and other non-offensive snaps. |
| `game_id` | Int64 | ESPN game identifier. |
| `season` | Int64 | Season (4-digit year). |

```python
load_cfb_drives(seasons=2024)
```

## load_cfb_play_participants

Release: [espn_cfb_play_participants](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_play_participants) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_play_participants/play_participants_{season}.parquet`
### Returns {#load_cfb_play_participants-returns}

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | ESPN game identifier. |
| `play_id` | Int64 | ESPN play id. |
| `kicker_player_name` | String | Display name of the kicker -- the FIRST participant in that role on the play. |
| `returner_player_name` | String | Display name of the player returning the kick or punt -- the FIRST participant in that role on the play. |
| `tackler_player_name` | String | Display name of a defender credited with the tackle -- the FIRST participant in that role on the play. |
| `rusher_player_name` | String | Display name of the ball carrier on a rush -- the FIRST participant in that role on the play. |
| `passer_player_name` | String | Display name of the passer -- the FIRST participant in that role on the play. |
| `receiver_player_name` | String | Display name of the targeted receiver -- the FIRST participant in that role on the play. |
| `assisted_by_player_name` | String | Display name of a defender credited with an assisted tackle -- the FIRST participant in that role on the play. |
| `scorer_player_name` | String | Display name of the player credited with the score -- the FIRST participant in that role on the play. |
| `pat_scorer_player_name` | String | Display name of the player credited with the point-after score -- the FIRST participant in that role on the play. |
| `punter_player_name` | String | Display name of the punter -- the FIRST participant in that role on the play. |
| `pass_defender_player_name` | String | Display name of the defender credited with defending the pass -- the FIRST participant in that role on the play. |
| `sacked_by_player_name` | String | Display name of a defender credited with the sack -- the FIRST participant in that role on the play. |
| `penalized_player_name` | String | Display name of the penalized player -- the FIRST participant in that role on the play. |
| `pat_passer_player_name` | String | Display name of the passer on the point-after attempt -- the FIRST participant in that role on the play. |
| `kicker_player_id` | String | ESPN athlete id of the kicker -- the FIRST participant in that role on the play. |
| `returner_player_id` | String | ESPN athlete id of the player returning the kick or punt -- the FIRST participant in that role on the play. |
| `tackler_player_id` | String | ESPN athlete id of a defender credited with the tackle -- the FIRST participant in that role on the play. |
| `rusher_player_id` | String | ESPN athlete id of the ball carrier on a rush -- the FIRST participant in that role on the play. |
| `passer_player_id` | String | ESPN athlete id of the passer -- the FIRST participant in that role on the play. |
| `receiver_player_id` | String | ESPN athlete id of the targeted receiver -- the FIRST participant in that role on the play. |
| `assisted_by_player_id` | String | ESPN athlete id of a defender credited with an assisted tackle -- the FIRST participant in that role on the play. |
| `scorer_player_id` | String | ESPN athlete id of the player credited with the score -- the FIRST participant in that role on the play. |
| `pat_scorer_player_id` | String | ESPN athlete id of the player credited with the point-after score -- the FIRST participant in that role on the play. |
| `punter_player_id` | String | ESPN athlete id of the punter -- the FIRST participant in that role on the play. |
| `pass_defender_player_id` | String | ESPN athlete id of the defender credited with defending the pass -- the FIRST participant in that role on the play. |
| `sacked_by_player_id` | String | ESPN athlete id of a defender credited with the sack -- the FIRST participant in that role on the play. |
| `penalized_player_id` | String | ESPN athlete id of the penalized player -- the FIRST participant in that role on the play. |
| `pat_passer_player_id` | String | ESPN athlete id of the passer on the point-after attempt -- the FIRST participant in that role on the play. |
| `kicker_player_names` | String | List of the display names of EVERY participant credited as the kicker on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `returner_player_names` | String | List of the display names of EVERY participant credited as the player returning the kick or punt on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `tackler_player_names` | String | List of the display names of EVERY participant credited as a defender credited with the tackle on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `rusher_player_names` | String | List of the display names of EVERY participant credited as the ball carrier on a rush on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `passer_player_names` | String | List of the display names of EVERY participant credited as the passer on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `receiver_player_names` | String | List of the display names of EVERY participant credited as the targeted receiver on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `assisted_by_player_names` | String | List of the display names of EVERY participant credited as a defender credited with an assisted tackle on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `scorer_player_names` | String | List of the display names of EVERY participant credited as the player credited with the score on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `pat_scorer_player_names` | String | List of the display names of EVERY participant credited as the player credited with the point-after score on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `punter_player_names` | String | List of the display names of EVERY participant credited as the punter on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `pass_defender_player_names` | String | List of the display names of EVERY participant credited as the defender credited with defending the pass on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `sacked_by_player_names` | String | List of the display names of EVERY participant credited as a defender credited with the sack on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `penalized_player_names` | String | List of the display names of EVERY participant credited as the penalized player on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `pat_passer_player_names` | String | List of the display names of EVERY participant credited as the passer on the point-after attempt on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `kicker_player_ids` | String | List of the athlete ids of EVERY participant credited as the kicker on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `returner_player_ids` | String | List of the athlete ids of EVERY participant credited as the player returning the kick or punt on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `tackler_player_ids` | String | List of the athlete ids of EVERY participant credited as a defender credited with the tackle on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `rusher_player_ids` | String | List of the athlete ids of EVERY participant credited as the ball carrier on a rush on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `passer_player_ids` | String | List of the athlete ids of EVERY participant credited as the passer on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `receiver_player_ids` | String | List of the athlete ids of EVERY participant credited as the targeted receiver on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `assisted_by_player_ids` | String | List of the athlete ids of EVERY participant credited as a defender credited with an assisted tackle on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `scorer_player_ids` | String | List of the athlete ids of EVERY participant credited as the player credited with the score on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `pat_scorer_player_ids` | String | List of the athlete ids of EVERY participant credited as the player credited with the point-after score on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `punter_player_ids` | String | List of the athlete ids of EVERY participant credited as the punter on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `pass_defender_player_ids` | String | List of the athlete ids of EVERY participant credited as the defender credited with defending the pass on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `sacked_by_player_ids` | String | List of the athlete ids of EVERY participant credited as a defender credited with the sack on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `penalized_player_ids` | String | List of the athlete ids of EVERY participant credited as the penalized player on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `pat_passer_player_ids` | String | List of the athlete ids of EVERY participant credited as the passer on the point-after attempt on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `season` | Int64 | Season (4-digit year). |
| `week` | Int64 | Game week of the season. |
| `recoverer_player_name` | String | Display name of the player who recovered the fumble -- the FIRST participant in that role on the play. |
| `fumbler_player_name` | String | Display name of the first (primary) player who fumbled on the play, from ESPN's per-play participants. |
| `forced_by_player_name` | String | Display name of the defender who forced the fumble -- the FIRST participant in that role on the play. |
| `recoverer_player_id` | String | ESPN athlete id of the player who recovered the fumble -- the FIRST participant in that role on the play. |
| `fumbler_player_id` | String | ESPN athlete id of the first (primary) player who fumbled on the play. |
| `forced_by_player_id` | String | ESPN athlete id of the defender who forced the fumble -- the FIRST participant in that role on the play. |
| `recoverer_player_names` | String | List of the display names of EVERY participant credited as the player who recovered the fumble on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `fumbler_player_names` | String | Display names of every player who fumbled on the play, as a list. |
| `forced_by_player_names` | String | List of the display names of EVERY participant credited as the defender who forced the fumble on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `recoverer_player_ids` | String | List of the athlete ids of EVERY participant credited as the player who recovered the fumble on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `fumbler_player_ids` | String | ESPN athlete ids of every player who fumbled on the play, as a list. |
| `forced_by_player_ids` | String | List of the athlete ids of EVERY participant credited as the defender who forced the fumble on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `kicker_position_id` | String | ESPN position id of the kicker -- the FIRST participant in that role on the play, as listed in the game's participants. |
| `returner_position_id` | String | ESPN position id of the player returning the kick or punt -- the FIRST participant in that role on the play, as listed in the game's participants. |
| `penalized_position_id` | String | ESPN position id of the penalized player -- the FIRST participant in that role on the play, as listed in the game's participants. |
| `passer_position_id` | String | ESPN position id of the passer -- the FIRST participant in that role on the play, as listed in the game's participants. |
| `receiver_position_id` | String | ESPN position id of the targeted receiver -- the FIRST participant in that role on the play, as listed in the game's participants. |
| `assisted_by_position_id` | String | ESPN position id of a defender credited with an assisted tackle -- the FIRST participant in that role on the play, as listed in the game's participants. |
| `tackler_position_id` | String | ESPN position id of a defender credited with the tackle -- the FIRST participant in that role on the play, as listed in the game's participants. |
| `punter_position_id` | String | ESPN position id of the punter -- the FIRST participant in that role on the play, as listed in the game's participants. |
| `rusher_position_id` | String | ESPN position id of the ball carrier on a rush -- the FIRST participant in that role on the play, as listed in the game's participants. |
| `pass_defender_position_id` | String | ESPN position id of the defender credited with defending the pass -- the FIRST participant in that role on the play, as listed in the game's participants. |
| `scorer_position_id` | String | ESPN position id of the player credited with the score -- the FIRST participant in that role on the play, as listed in the game's participants. |
| `pat_scorer_position_id` | String | ESPN position id of the player credited with the point-after score -- the FIRST participant in that role on the play, as listed in the game's participants. |
| `sacked_by_position_id` | String | ESPN position id of a defender credited with the sack -- the FIRST participant in that role on the play, as listed in the game's participants. |
| `recoverer_position_id` | String | ESPN position id of the player who recovered the fumble -- the FIRST participant in that role on the play, as listed in the game's participants. |
| `fumbler_position_id` | String | ESPN position id of the fumbler -- the FIRST participant in that role on the play, as listed in the game's participants. |
| `forced_by_position_id` | String | ESPN position id of the defender who forced the fumble -- the FIRST participant in that role on the play, as listed in the game's participants. |
| `pat_passer_position_id` | String | ESPN position id of the passer on the point-after attempt -- the FIRST participant in that role on the play, as listed in the game's participants. |

```python
load_cfb_play_participants(seasons=2024)
```

## load_cfb_game_rosters

Release: [espn_cfb_game_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_game_rosters) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_game_rosters/game_rosters_{season}.parquet`
### Returns {#load_cfb_game_rosters-returns}

| col_name | type | description |
|---|---|---|
| `athlete_id` | Int64 | ESPN athlete id. |
| `athlete_uid` | String | ESPN athlete UID (universal identifier). |
| `athlete_guid` | String | ESPN athlete GUID. |
| `athlete_type` | String | Athlete type / class. |
| `first_name` | String | Athlete first name. |
| `last_name` | String | Athlete last name. |
| `full_name` | String | Venue full name (e.g. `Tenney Stadium`). |
| `athlete_display_name` | String | Player display name; `athlete_detail = TRUE` only. |
| `short_name` | String | Ranking source short name (e.g. `AP Poll`). |
| `weight` | Float64 | Listed weight (lbs). |
| `display_weight` | String | Human-readable weight (e.g. `205 lbs`). |
| `height` | Float64 | Listed height (inches). |
| `display_height` | String | Human-readable height (e.g. `6' 1"`). |
| `slug` | String | URL slug for the team. |
| `jersey` | String | Jersey number. |
| `linked` | Boolean | TRUE if the record is linked to a related entity. |
| `active` | Boolean | `TRUE` if the player was active for the game. |
| `alternate_ids_sdr` | String | Alternate ids sdr. |
| `birth_place_city` | String | Birth place city. |
| `birth_place_state` | String | Birth place state. |
| `birth_place_country` | String | Birth place country. |
| `birth_country_alternate_id` | String | ESPN's internal alternate identifier for the athlete's birth country, paired with birth_place_country and the flag fields. |
| `birth_country_abbreviation` | String | Birth country abbreviation. |
| `headshot_href` | String | URL of the athlete headshot image. |
| `headshot_alt` | String | Alternative-text label for the headshot. |
| `flag_href` | String | URL of the birth-country flag image hosted on ESPN's CDN under teamlogos/countries. |
| `flag_alt` | String | Alt text ESPN attaches to the birth-country flag image, which is the country's name spelled out. |
| `flag_rel` | String | Stringified relationship list ESPN ships with the flag image; the only non-null value observed is a single country-flag entry. |
| `experience_years` | Float64 | Years of experience. |
| `experience_display_value` | String | Experience display value. |
| `experience_abbreviation` | String | Experience abbreviation. |
| `status_id` | String | ESPN commitment status id. |
| `status_name` | String | Status-type key (e.g. `STATUS_FINAL`). |
| `status_type` | String | Status type. |
| `status_abbreviation` | String | Status abbreviation. |
| `hand_type` | String | Hand type. |
| `hand_abbreviation` | String | Hand abbreviation. |
| `hand_display_value` | String | Hand display value. |
| `starter` | Boolean | `TRUE` if the athlete started the game. |
| `jersey_right` | String | Secondary or alternate jersey number display string from ESPN's roster record, distinct from the primary jersey number. |
| `valid` | Boolean | `TRUE` if the roster entry is flagged valid by ESPN. |
| `did_not_play` | Boolean | `TRUE` if the athlete did not play. |
| `display_name` | String | Human-readable metric name. |
| `athlete_href` | String | ESPN Core v2 API reference URL for the athlete's season record, ending in the athlete id. |
| `position_href` | String | ESPN Core v2 API reference URL for the position resource ESPN lists the athlete at. |
| `statistics_href` | String | ESPN Core v2 API reference URL for this athlete's stat line in this game, null for the roughly 71 percent of listed players who recorded no stats. |
| `team_id` | Int64 | ESPN team id. |
| `order` | Int64 | Team order within the competition (0 = first). |
| `home_away` | String | `home` or `away`. |
| `winner` | Boolean | `TRUE` if this team won the game. |
| `team_guid` | String | ESPN team GUID. |
| `team_uid` | String | ESPN universal team identifier (UID format 's:40~l:...~t:...'). |
| `team_slug` | String | Team slug for the stat row. |
| `team_location` | String | Team location / school name; `team_detail = TRUE` only. |
| `team_name` | String | Team nickname; `team_detail = TRUE` only. |
| `team_nickname` | String | Team nickname label; `team_detail = TRUE` only. |
| `team_abbreviation` | String | Team abbreviation; `team_detail = TRUE` only. |
| `team_display_name` | String | Full team display name; `team_detail = TRUE` only. |
| `team_short_display_name` | String | Short team display name; `team_detail = TRUE` only. |
| `team_color` | String | Primary team color; `team_detail = TRUE` only. |
| `team_alternate_color` | String | Alternate team color; `team_detail = TRUE` only. |
| `is_active` | Boolean | Whether the team is currently active. |
| `is_all_star` | Boolean | Whether the team is an all-star team. |
| `team_alternate_ids_sdr` | String | The team's Sportradar alternate identifier, which maps one-to-one with team_id. |
| `logo_href` | String | URL of the default team logo. |
| `logo_dark_href` | String | URL of the dark-variant team logo. |
| `game_id` | Int64 | ESPN game identifier. |
| `season` | Int64 | Season (4-digit year). |
| `week` | Int64 | Game week of the season. |
| `citizenship` | String | Citizenship. |
| `middle_name` | String | Middle name of the player. |
| `age` | Float64 | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |
| `date_of_birth` | String | Player date of birth (if published). |
| `draft_display_text` | String | Draft display text. |
| `draft_round` | Float64 | Round that player was drafted in |
| `draft_year` | Float64 | Year that player was drafted |
| `draft_selection` | Float64 | Draft selection. |
| `nickname` | String | Team nickname / location label. |

```python
load_cfb_game_rosters(seasons=2024)
```

## load_cfb_linescores

Release: [espn_cfb_linescores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_linescores) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_linescores/linescores_{season}.parquet`
### Returns {#load_cfb_linescores-returns}

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | ESPN team id. |
| `period` | Int64 | Period (quarter) number. |
| `value` | String | Metric value. |
| `game_id` | Int64 | ESPN game identifier. |
| `season` | Int64 | Season (4-digit year). |

```python
load_cfb_linescores(seasons=2024)
```

## load_cfb_betting

Release: [espn_cfb_betting](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_betting) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_betting/betting_{season}.parquet`
### Returns {#load_cfb_betting-returns}

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | ESPN game identifier. |
| `season` | Int64 | Season (4-digit year). |
| `week` | Int64 | Game week of the season. |
| `game_spread` | Float64 | Game spread in (-X Team) format. There are almost none, I would recommend not trusting any of these three columns |
| `over_under` | Float64 | Pre-game over/under total from the selected provider. |
| `home_favorite` | Boolean | `TRUE` if the home team is the favorite. |
| `home_team_spread` | Float64 | The game spread with respect to the home team |
| `game_spread_available` | Boolean | Logical (TRUE/FALSE) indicating whether the spread was available from ESPN. Basically, I would just not recommend using any of the spread information, I think I defaulted a lot of them to -2.5 for the home team. Most games probably do not have spread information. This column should really be listed first |
| `odds_source` | String | Provenance of the spread and over/under used for the game: summary_pickcenter when ESPN's own pickcenter carried them, core_odds_api when they came from the live odds endpoint, default when neither resolved, injected when supplied by an offline rebuild. |

```python
load_cfb_betting(seasons=2024)
```

## load_cfb_fpi_weekly

Release: [cfb_fpi_weekly](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfb_fpi_weekly) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/cfb_fpi_weekly/cfb_fpi_weekly_{season}.parquet`
### Returns {#load_cfb_fpi_weekly-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season (4-digit year). |
| `season_type` | Int64 | ESPN season type (2 = regular, 3 = postseason). |
| `week` | Int64 | Game week of the season. |
| `team_id` | Int64 | ESPN team id. |
| `last_updated` | String | Timestamp ESPN last refreshed the power index. |
| `run_date_time_key` | Int64 | ESPN's run key for the snapshot, as an integer timestamp (e.g. 20241021040000). This is the AS-OF date the snapshot represents, which is not the same as last_updated (when ESPN computed it); the gap between the two is what snapshot_is_contemporaneous flags. |
| `snapshot_out_of_sequence` | Boolean | True when this snapshot was computed AFTER one belonging to a later week of the same season type -- so it cannot be read as an as-of-that-week rating. Almost always the week-1 slot, which ESPN overwrites with a late-season computation (2024 week 1 is stamped 2024-12-15). Filter these out for any point-in-time or backtest use. |
| `fpi` | Float64 | Football Power Index that measures team's true strength on net points scale; expected point margin vs average opponent on neutral field. |
| `fpirank` | Float64 | ESPN's FPI rank field. Agrees with rank on 99.4% of rows; on the ~0.6% where they differ it is stale -- it never matches the rank implied by the published fpi, while rank always does. Prefer rank. |
| `projectedw` | Float64 | Projected overall W-L, accounting for results to date and FPI-based projections for remaining scheduled games (and potential conference championship games). May not sum to a whole number because of differing number of games played in each simulation. |
| `projectedl` | Float64 | Projected overall Losses, accounting for results to date and FPI-based projections for remaining scheduled games (including potential conference championship games). May not sum to a whole number because of differing number of games played in each simulation. |
| `projectedt` | Null | Projected ties. Always null -- college football abolished ties in 1996, and ESPN emits the key beside projectedw/projectedl without ever populating it. Retained so the column set matches the upstream payload. |
| `projectedwpctrank` | Float64 | Rank among FBS teams by projected win percentage. ESPN publishes the rank without the underlying percentage; derive it from projectedw and projectedl. |
| `probwinout` | Float64 | Percent of season simulations in which team won all remaining scheduled games as well as conference championship game (if applicable). |
| `probwinconf` | Float64 | Percent of season simulations in which team won its conference, incorporating chance of getting to and winning conference championship game (if applicable). Accounts for shared conference titles in conferences that allow them. |
| `sosremainingrank` | Float64 | Rank among all FBS teams of remaining schedule strength, from perspective of an average FBS team. |
| `accomplishment` | Float64 | Reflects chance that an average Top 25 team would have team's record or better, given the schedule. On a 0 to 100 scale, where 100 is best. |
| `accomplishmentrank` | Float64 | Strength of Record rank. Reflects chance that an average Top 25 team would have team's record or better, given the schedule. |
| `adjwins` | Float64 | Team's Wins adjusted for chance an average FBS team would have team's record or better, given the schedule. |
| `adjlosses` | Float64 | Team's Losses adjusted for chance an average FBS team would have team's record or better, given the schedule. |
| `adjwinpctrank` | Float64 | Rank among FBS teams by adjusted win percentage. ESPN publishes the rank without the underlying percentage; derive it from adjwins and adjlosses. 0 is an unranked placeholder, not a rank -- it appears where the underlying value is null. |
| `gamecontrol` | Float64 | Reflects chance that an average Top 25 team would control games from start to end the way this team did, given the schedule. On a 0 to 100 scale, where 100 is best. |
| `gamecontrolrank` | Float64 | Game Control rank. Reflects chance that an average Top 25 team would control games from start to end the way this team did, given the schedule. |
| `adjavgingamewp` | Float64 | Team's average in-game win probability adjusted for chance that an average FBS team would control games from start to end the way this team did, given the schedule. |
| `adjavgingamewprank` | Float64 | Rank among FBS teams by adjavgingamewp (average in-game win probability adjusted for opponent). Null for most pre-2019 snapshots. 0 is an unranked placeholder, not a rank. |
| `avgingamewp` | Float64 | Team's average in-game win probability across all plays of all games played, not adjusted for site or opponent. |
| `avgingamewprank` | Float64 | Team's average in-game win probability rank adjusted for chance that an average FBS team would control games from start to end the way this team did, given the schedule. |
| `avgsosrank` | Float64 | Rank among all FBS teams of games already played schedule strength, from perspective of an average Top 25 team. |
| `topsosrank` | Float64 | Rank among all FBS teams of games already played schedule strength, from perspective of an top FBS team. |
| `epaoffense` | Float64 | Offensive component of FPI. Offensive contribution to expected point margin vs average opponent on neutral field. |
| `epadefense` | Float64 | Defensive component of FPI. Defensive contribution to expected point margin vs average opponent on neutral field. |
| `epaspecialteams` | Float64 | Special teams component of FPI. Special teams contribution to expected point margin vs average opponent on neutral field. |
| `probwindiv` | Float64 | Percent of season simulations in which team won its conference division, for those conferences that have divisions. |
| `probmakeplayoffs` | Float64 | Chance to make the CFB Playoff, according to the Playoff Predictor. |
| `probmaketitlegame` | Float64 | Chance to make the CFB Playoff National Championship game, according to the Playoff Predictor. |
| `numwins` | Float64 | Actual wins to date at the time of the snapshot. Distinct from projectedw (full-season projection) and adjwins (opponent-adjusted). |
| `numlosses` | Float64 | Actual losses to date at the time of the snapshot. Distinct from projectedl (full-season projection) and adjlosses (opponent-adjusted). |
| `numties` | Float64 | Actual ties to date. Never nonzero -- college football abolished ties in 1996; the column is null or 0 in every published row. |
| `probwintitle` | Float64 | Chance to win the CFB Playoff National Championship, according to the Playoff Predictor. |
| `rankchange7days` | Float64 | FPI Rank change from previous week. |
| `prob6wins` | Float64 | Percent of season simulations in which a team won at least 6 games (typically bowl-eligible). |
| `rank` | Float64 | FPI rank among FBS teams for this snapshot (1 = best). Prefer this over fpirank: the two agree on 99.4% of rows, and on the ~0.6% where they differ, rank is always the one consistent with the published fpi value. |
| `offefficiency` | Float64 | Offensive efficiency on 0-100 scale; based on offense's contribution to scoring margin on per-play basis, adjusted for strength of opposing defenses faced. |
| `offefficiencyrank` | Float64 | Team's offensive efficiency rank among all FBS teams. |
| `defefficiency` | Float64 | Defensive efficiency on 0-100 scale; based on defense's contribution to scoring margin on per-play basis, adjusted for strength of opposing offenses faced. |
| `defefficiencyrank` | Float64 | Team's defensive efficiency rank among all FBS teams. |
| `stefficiency` | Float64 | Special teams efficiency on 0-100 scale; based on special teams' contribution to scoring margin on per-play basis, adjusted for strength of opposing special teams faced. |
| `stefficiencyrank` | Float64 | Team's special teams efficiency rank among all FBS teams. |
| `totefficiency` | Float64 | Net efficiency on 0-100 scale; incorporates offense, defense and special teams efficiencies into a single schedule-adjusted measure of per-play efficiency. |
| `totefficiencyrank` | Float64 | Team's overall efficiency rank among all FBS teams. |
| `snapshot_is_contemporaneous` | Boolean | True when the snapshot was computed inside its own season's window (August of the season year through February of the next), i.e. it is a live weekly run rather than a retrospective backfill. False for every row before 2015, which ESPN computed in one pass afterwards. A retrospective row is a reconstruction, not an as-of-week rating. |

```python
load_cfb_fpi_weekly(seasons=2024)
```

## load_cfb_power_index

Release: [espn_cfb_power_index](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_power_index) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_power_index/power_index_{season}.parquet`
### Returns {#load_cfb_power_index-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season (4-digit year). |
| `game_id` | Int64 | ESPN game identifier. |
| `team_id` | Int64 | ESPN team id. |
| `teampredptdiff` | Float64 | Expected margin of victory for the FPI favorite. |
| `gameprojection` | Float64 | Team's predicted win percentage in this game at time of given BPI run. |
| `matchupquality` | Float64 | A measure of projected competitiveness and excitement in the game, using a 0 to 100 scale, with 100 as the most exciting. |
| `teamadjgamescore` | Null | A measure of how well a team performed compared to their expected performance and the expected performance of a typical top 25 team. |

```python
load_cfb_power_index(seasons=2024)
```

## load_cfb_model_pbp

Release: [espn_cfb_model_pbp](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_model_pbp) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_model_pbp/model_pbp_{season}.parquet`
### Returns {#load_cfb_model_pbp-returns}

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | ESPN game identifier. |
| `id` | String | 247Sports referencing id for the recruit. |
| `sequenceNumber` | String | Broadcast sequence order number. |
| `game_play_number` | Int64 | Sequential play number within the game (excludes timeouts/end markers). |
| `drive.id` | String | ESPN's drive identifier, formed as the game id followed by the drive's sequence number within that game. |
| `season` | Int64 | Season (4-digit year). |
| `week` | Int64 | Game week of the season. |
| `period` | Int64 | Period (quarter) number. |
| `pos_team` | Int64 | Team name in possession at the start of the play (offense, kickoff-aware). |
| `def_pos_team` | Int64 | Team name on defense at the start of the play. |
| `start.pos_team.name` | String | School name of the team with possession at the snap, taken from ESPN's team location field so it carries no mascot. |
| `homeTeamId` | Int64 | ESPN team id of the home team, read off the game header and stamped on every play. |
| `awayTeamId` | Int64 | ESPN team id of the away team, read off the game header and stamped on every play. |
| `homeTeamName` | String | Home team's school name from ESPN's team location field, without the mascot. |
| `awayTeamName` | String | Away team's school name from ESPN's team location field, without the mascot. |
| `type.text` | String | ESPN's play-type label, for example Rush, Pass Reception, Sack, Punt, Penalty, or Timeout. |
| `text` | String | Full play description. |
| `start.down` | Int64 | Down at the snap as ESPN reports it; 0 marks the small share of rows ESPN leaves without a down, overwhelmingly timeouts and penalty administrations. |
| `start.distance` | Int64 | Yards the offense needs for a first down at the snap, carried through from ESPN without correction. |
| `start.yardsToEndzone` | Int64 | Distance in yards from the offense's spot at the snap to the opponent's end zone, ranging 0 to 100. |
| `pos_score_diff_start` | Int64 | Score differential for the possession team at the start of the play. |
| `start.TimeSecsRem` | Int64 | Seconds remaining in the half from ESPN's clock stamp for this play, which is the end-of-play time in 2005 and 2007+ (the snap time in 2004 and most of 2006); tops out at 1800. |
| `start.is_home` | Boolean | True when the team holding possession at the snap is the home team. |
| `passing_down` | Boolean | True on second and eight or longer, third and five or longer, or fourth and five or longer, the standard obvious-passing-situation flag. |
| `pass` | Boolean | Binary flag for a passing play (includes sacks). |
| `rush` | Boolean | Binary flag for a rushing play. |
| `completion` | Boolean | Binary flag for a completed pass. |
| `scoring_play` | Boolean | `TRUE` if the play resulted in a score. |
| `statYardage` | Int64 | Yards gained on the play as ESPN reports it, negative on plays that lost yardage. |
| `passer_player_name` | String | Display name of the passer -- the FIRST participant in that role on the play. |
| `ep_before` | Float64 | Expected points value before the play (cfbfastR EPA model). |
| `ep_after` | Float64 | Expected points value after the play (cfbfastR EPA model). |
| `epa` | Float64 | Expected points added (EPA) by the posteam for the given play. |
| `wp_before` | Float64 | Win probability for the possession team before the play (0-1). |
| `wp_after` | Float64 | Win probability for the possession team after the play (0-1). |
| `wpa` | Float64 | Win Probability Added on the play (cfbfastR WP model output). |
| `completion_prob` | Float64 | Modelled probability the pass is completed. |
| `cpoe` | Float64 | For a single pass play this is 1 - cp when the pass was completed or 0 - cp when the pass was incomplete. Analyzed for a whole game or season an indicator for the passer how much over or under expectation his completion percentage was. |
| `model_pbp_version` | String | Version of the model-scored play-by-play build. |
| `cp_model_version` | String | Version of the completion-probability model that scored the play. |
| `ep_model_version` | String | Version of the expected-points model that scored the play. |
| `wp_model_version` | String | Version of the win-probability model that scored the play. |
| `scored_date` | String | Date on which the play was scored by the models. |

```python
load_cfb_model_pbp(seasons=2024)
```

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

```python
load_cfb_passing(seasons=2024)
```
