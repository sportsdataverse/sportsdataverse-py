---
title: "CFB dataset loaders — Coach: careers"
sidebar_label: "Coach: careers"
sidebar_position: 3
description: "CFB dataset loaders — Coach: careers — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# CFB dataset loaders — Coach: careers

## load_cfb_coach_careers

Release: [espn_cfb_coach_careers](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_coach_careers) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_coach_careers/coach_careers.parquet`

:::caution[Coverage]
One season-less file: every published coach_tendencies season summed per head coach with the rates recomputed (play-weighted, never averaged averages). Careers therefore cover exactly the seasons published under the coach_tendencies tag.
:::

### Returns {#load_cfb_coach_careers-returns}

| col_name | type | description |
|---|---|---|
| `coach` | String | Head coach the plays were attributed to -- per game from the nflverse schedule (home_coach / away_coach) for the NFL, per team-season from the producer's CFBD coach roster for CFB. |
| `role` | String | Coaching role the row is keyed on; always "HC" (head coach) in the published assets, reserved for later coordinator rows. |
| `teams` | String | Comma-separated teams the coach's attributed seasons were with, in first-to-last season order. |
| `seasons` | UInt32 | Number of distinct seasons summed into the career row. |
| `first_season` | Int64 | Earliest season summed into the career row. |
| `last_season` | Int64 | Latest season summed into the career row. |
| `games` | UInt32 | Distinct games in which the offense ran at least one standing scrimmage play. Summed over the coach's published seasons, like every count here. |
| `plays` | UInt32 | Standing scrimmage plays run by the offense (scrimmage_play rows not nullified by penalty). |
| `rushes` | UInt32 | Rushing plays among the standing scrimmage plays. |
| `passes` | UInt32 | Pass plays among the standing scrimmage plays. |
| `epa` | Float64 | Play EPA summed over the standing scrimmage plays. |
| `epa_rush` | Float64 | Play EPA summed over the rushing plays. |
| `epa_pass` | Float64 | Play EPA summed over the pass plays. |
| `successes` | UInt32 | Plays flagged EPA_success (positive EPA) on the play-by-play. |
| `successes_rush` | UInt32 | Rushing plays flagged EPA_success. |
| `successes_pass` | UInt32 | Pass plays flagged EPA_success. |
| `yards` | Float64 | statYardage summed over the standing scrimmage plays. |
| `yards_rush` | Float64 | statYardage summed over the rushing plays. |
| `yards_pass` | Float64 | statYardage summed over the pass plays. |
| `explosives` | UInt32 | Plays flagged EPA_explosive on the play-by-play. |
| `explosives_rush` | UInt32 | Rushing plays flagged EPA_explosive. |
| `explosives_pass` | UInt32 | Pass plays flagged EPA_explosive. |
| `third_down_opportunities` | UInt32 | Third-down scrimmage plays. |
| `third_down_conversions` | UInt32 | Third-down plays that produced a first down or an offensive touchdown. |
| `third_down_expected` | Float64 | Expected third-down conversions: the league's bundled third-down yards-to-go conversion curve summed over the third-down plays; null when no curve was available. |
| `plays_neutral` | UInt32 | Plays in situation-neutral plays: score-and-clock win probability (wp_before_naive, no pregame line) between 20% and 80%, in the first four quarters, outside the final two minutes of a half. |
| `passes_neutral` | UInt32 | Pass plays in situation-neutral situations (see plays_neutral). |
| `epa_neutral` | Float64 | Play EPA summed over situation-neutral plays: score-and-clock win probability (wp_before_naive, no pregame line) between 20% and 80%, in the first four quarters, outside the final two minutes of a half. |
| `successes_neutral` | UInt32 | Plays flagged EPA_success (positive EPA) in situation-neutral situations (score-and-clock win probability (wp_before_naive, no pregame line) between 20% and 80%, in the first four quarters, outside the final two minutes of a half). |
| `plays_d1` | UInt32 | Plays on first down. |
| `passes_d1` | UInt32 | Pass plays on first down. |
| `epa_d1` | Float64 | Play EPA summed over the plays on first down. |
| `successes_d1` | UInt32 | Plays flagged EPA_success (positive EPA) on first down. |
| `plays_d2` | UInt32 | Plays on second down. |
| `passes_d2` | UInt32 | Pass plays on second down. |
| `epa_d2` | Float64 | Play EPA summed over the plays on second down. |
| `successes_d2` | UInt32 | Plays flagged EPA_success (positive EPA) on second down. |
| `plays_d3` | UInt32 | Plays on third down. |
| `passes_d3` | UInt32 | Pass plays on third down. |
| `epa_d3` | Float64 | Play EPA summed over the plays on third down. |
| `successes_d3` | UInt32 | Plays flagged EPA_success (positive EPA) on third down. |
| `plays_d4` | UInt32 | Plays on fourth down. |
| `passes_d4` | UInt32 | Pass plays on fourth down. |
| `epa_d4` | Float64 | Play EPA summed over the plays on fourth down. |
| `successes_d4` | UInt32 | Plays flagged EPA_success (positive EPA) on fourth down. |
| `plays_early_down` | UInt32 | Plays on first or second down. |
| `passes_early_down` | UInt32 | Pass plays on first or second down. |
| `epa_early_down` | Float64 | Play EPA summed over the first- and second-down plays. |
| `successes_early_down` | UInt32 | Plays flagged EPA_success (positive EPA) on first or second down. |
| `plays_standard_down` | UInt32 | Plays flagged standard_down on the play-by-play: first down, second down with fewer than 8 to go, or third / fourth down with fewer than 5 to go. |
| `passes_standard_down` | UInt32 | Pass plays on standard downs (see plays_standard_down). |
| `epa_standard_down` | Float64 | Play EPA summed over the plays on standard downs (the play-by-play standard_down flag). |
| `successes_standard_down` | UInt32 | Plays flagged EPA_success (positive EPA) on standard downs (the play-by-play standard_down flag). |
| `plays_passing_down` | UInt32 | Plays flagged passing_down on the play-by-play: second down with 8 or more to go, or third / fourth down with 5 or more to go. |
| `passes_passing_down` | UInt32 | Pass plays on passing downs (see plays_passing_down). |
| `epa_passing_down` | Float64 | Play EPA summed over the plays on passing downs (the play-by-play passing_down flag). |
| `successes_passing_down` | UInt32 | Plays flagged EPA_success (positive EPA) on passing downs (the play-by-play passing_down flag). |
| `plays_leading` | UInt32 | Plays snapped with the offense ahead on the scoreboard (pos_score_diff_start > 0, the score at the snap, before the play; pos_score_diff where the start score is missing). |
| `passes_leading` | UInt32 | Pass plays snapped with the offense ahead on the scoreboard (pos_score_diff_start > 0, the score at the snap, before the play; pos_score_diff where the start score is missing). |
| `epa_leading` | Float64 | Play EPA summed over the plays snapped with the offense ahead on the scoreboard (pos_score_diff_start > 0, the score at the snap, before the play; pos_score_diff where the start score is missing). |
| `successes_leading` | UInt32 | Plays flagged EPA_success (positive EPA) snapped with the offense ahead on the scoreboard (pos_score_diff_start > 0, the score at the snap, before the play; pos_score_diff where the start score is missing). |
| `plays_tied` | UInt32 | Plays snapped with the score tied (pos_score_diff_start == 0, the score at the snap, before the play; pos_score_diff where the start score is missing). |
| `passes_tied` | UInt32 | Pass plays snapped with the score tied (pos_score_diff_start == 0, the score at the snap, before the play; pos_score_diff where the start score is missing). |
| `epa_tied` | Float64 | Play EPA summed over the plays snapped with the score tied (pos_score_diff_start == 0, the score at the snap, before the play; pos_score_diff where the start score is missing). |
| `successes_tied` | UInt32 | Plays flagged EPA_success (positive EPA) snapped with the score tied (pos_score_diff_start == 0, the score at the snap, before the play; pos_score_diff where the start score is missing). |
| `plays_trailing` | UInt32 | Plays snapped with the offense behind on the scoreboard (pos_score_diff_start < 0, the score at the snap, before the play; pos_score_diff where the start score is missing). |
| `passes_trailing` | UInt32 | Pass plays snapped with the offense behind on the scoreboard (pos_score_diff_start < 0, the score at the snap, before the play; pos_score_diff where the start score is missing). |
| `epa_trailing` | Float64 | Play EPA summed over the plays snapped with the offense behind on the scoreboard (pos_score_diff_start < 0, the score at the snap, before the play; pos_score_diff where the start score is missing). |
| `successes_trailing` | UInt32 | Plays flagged EPA_success (positive EPA) snapped with the offense behind on the scoreboard (pos_score_diff_start < 0, the score at the snap, before the play; pos_score_diff where the start score is missing). |
| `plays_first_half` | UInt32 | Plays in the first two quarters. |
| `passes_first_half` | UInt32 | Pass plays in the first two quarters. |
| `epa_first_half` | Float64 | Play EPA summed over the plays in the first two quarters. |
| `successes_first_half` | UInt32 | Plays flagged EPA_success (positive EPA) in the first two quarters. |
| `plays_second_half` | UInt32 | Plays in the third and fourth quarters (overtime belongs to neither half). |
| `passes_second_half` | UInt32 | Pass plays in the third and fourth quarters. |
| `epa_second_half` | Float64 | Play EPA summed over the plays in the third and fourth quarters (overtime belongs to neither half). |
| `successes_second_half` | UInt32 | Plays flagged EPA_success (positive EPA) in the third and fourth quarters (overtime belongs to neither half). |
| `plays_d3_short` | UInt32 | Plays on third down with 3 or fewer yards to go. |
| `passes_d3_short` | UInt32 | Pass plays on third down with 3 or fewer yards to go. |
| `epa_d3_short` | Float64 | Play EPA summed over the plays on third down with 3 or fewer yards to go. |
| `successes_d3_short` | UInt32 | Plays flagged EPA_success (positive EPA) on third down with 3 or fewer yards to go. |
| `plays_d3_medium` | UInt32 | Plays on third down with 4 to 6 yards to go. |
| `passes_d3_medium` | UInt32 | Pass plays on third down with 4 to 6 yards to go. |
| `epa_d3_medium` | Float64 | Play EPA summed over the plays on third down with 4 to 6 yards to go. |
| `successes_d3_medium` | UInt32 | Plays flagged EPA_success (positive EPA) on third down with 4 to 6 yards to go. |
| `plays_d3_long` | UInt32 | Plays on third down with 7 or more yards to go. |
| `passes_d3_long` | UInt32 | Pass plays on third down with 7 or more yards to go. |
| `epa_d3_long` | Float64 | Play EPA summed over the plays on third down with 7 or more yards to go. |
| `successes_d3_long` | UInt32 | Plays flagged EPA_success (positive EPA) on third down with 7 or more yards to go. |
| `plays_red_zone` | UInt32 | Plays in the red zone (20 or fewer yards from the opponent end zone, read from yards to goal rather than the absolute yard line; the play-by-play rz_play flag only when that distance is missing). |
| `passes_red_zone` | UInt32 | Pass plays in the red zone (20 or fewer yards from the opponent end zone, read from yards to goal rather than the absolute yard line; the play-by-play rz_play flag only when that distance is missing). |
| `epa_red_zone` | Float64 | Play EPA summed over the plays in the red zone (20 or fewer yards from the opponent end zone, read from yards to goal rather than the absolute yard line; the play-by-play rz_play flag only when that distance is missing). |
| `successes_red_zone` | UInt32 | Plays flagged EPA_success (positive EPA) in the red zone (20 or fewer yards from the opponent end zone, read from yards to goal rather than the absolute yard line; the play-by-play rz_play flag only when that distance is missing). |
| `plays_own_half` | UInt32 | Plays snapped in the offense's own half (50 or more yards from the opponent end zone). |
| `passes_own_half` | UInt32 | Pass plays snapped in the offense's own half (50 or more yards from the opponent end zone). |
| `epa_own_half` | Float64 | Play EPA summed over the plays snapped in the offense's own half (50 or more yards from the opponent end zone). |
| `successes_own_half` | UInt32 | Plays flagged EPA_success (positive EPA) snapped in the offense's own half (50 or more yards from the opponent end zone). |
| `plays_opp_half` | UInt32 | Plays snapped in the opponent's half (fewer than 50 yards from the opponent end zone). |
| `passes_opp_half` | UInt32 | Pass plays snapped in the opponent's half (fewer than 50 yards from the opponent end zone). |
| `epa_opp_half` | Float64 | Play EPA summed over the plays snapped in the opponent's half (fewer than 50 yards from the opponent end zone). |
| `successes_opp_half` | UInt32 | Plays flagged EPA_success (positive EPA) snapped in the opponent's half (fewer than 50 yards from the opponent end zone). |
| `plays_one_score` | UInt32 | Plays snapped with the offense within 8 points either way (\|pos_score_diff_start\| <= 8, the score at the snap, before the play; pos_score_diff where the start score is missing). |
| `passes_one_score` | UInt32 | Pass plays snapped with the offense within 8 points either way (\|pos_score_diff_start\| <= 8, the score at the snap, before the play; pos_score_diff where the start score is missing). |
| `epa_one_score` | Float64 | Play EPA summed over the plays snapped with the offense within 8 points either way (\|pos_score_diff_start\| <= 8, the score at the snap, before the play; pos_score_diff where the start score is missing). |
| `successes_one_score` | UInt32 | Plays flagged EPA_success (positive EPA) snapped with the offense within 8 points either way (\|pos_score_diff_start\| <= 8, the score at the snap, before the play; pos_score_diff where the start score is missing). |
| `plays_home` | UInt32 | Plays in the team's home games. |
| `passes_home` | UInt32 | Pass plays in the team's home games. |
| `epa_home` | Float64 | Play EPA summed over the plays in the team's home games. |
| `successes_home` | UInt32 | Plays flagged EPA_success (positive EPA) in the team's home games. |
| `games_home` | UInt32 | Distinct home games in which the offense ran at least one standing scrimmage play. |
| `wins_home` | UInt32 | Of games_home, the home games the team won (points for above points against, so a tie is not a win). |
| `plays_away` | UInt32 | Plays in the team's away games. |
| `passes_away` | UInt32 | Pass plays in the team's away games. |
| `epa_away` | Float64 | Play EPA summed over the plays in the team's away games. |
| `successes_away` | UInt32 | Plays flagged EPA_success (positive EPA) in the team's away games. |
| `games_away` | UInt32 | Distinct away games in which the offense ran at least one standing scrimmage play. |
| `wins_away` | UInt32 | Of games_away, the away games the team won (points for above points against, so a tie is not a win). |
| `plays_neutral_site` | UInt32 | Plays in the team's neutral-site games. |
| `passes_neutral_site` | UInt32 | Pass plays in the team's neutral-site games. |
| `epa_neutral_site` | Float64 | Play EPA summed over the plays in the team's neutral-site games. |
| `successes_neutral_site` | UInt32 | Plays flagged EPA_success (positive EPA) in the team's neutral-site games. |
| `games_neutral_site` | UInt32 | Distinct neutral-site games in which the offense ran at least one standing scrimmage play. |
| `wins_neutral_site` | UInt32 | Of games_neutral_site, the neutral-site games the team won (points for above points against, so a tie is not a win). |
| `plays_vs_ranked` | UInt32 | Plays in the team's games against an opponent ranked at kickoff (ESPN's displayed rank). |
| `passes_vs_ranked` | UInt32 | Pass plays in the team's games against an opponent ranked at kickoff (ESPN's displayed rank). |
| `epa_vs_ranked` | Float64 | Play EPA summed over the plays in the team's games against an opponent ranked at kickoff (ESPN's displayed rank). |
| `successes_vs_ranked` | UInt32 | Plays flagged EPA_success (positive EPA) in the team's games against an opponent ranked at kickoff (ESPN's displayed rank). |
| `games_vs_ranked` | UInt32 | Distinct games against an opponent ranked at kickoff (ESPN's displayed rank) in which the offense ran at least one standing scrimmage play. |
| `wins_vs_ranked` | UInt32 | Of games_vs_ranked, the games against an opponent ranked at kickoff (ESPN's displayed rank) the team won (points for above points against, so a tie is not a win). |
| `plays_after_bye` | UInt32 | Plays in the team's games played 13 or more days after the team's previous game. |
| `passes_after_bye` | UInt32 | Pass plays in the team's games played 13 or more days after the team's previous game. |
| `epa_after_bye` | Float64 | Play EPA summed over the plays in the team's games played 13 or more days after the team's previous game. |
| `successes_after_bye` | UInt32 | Plays flagged EPA_success (positive EPA) in the team's games played 13 or more days after the team's previous game. |
| `games_after_bye` | UInt32 | Distinct games played 13 or more days after the team's previous game in which the offense ran at least one standing scrimmage play. |
| `wins_after_bye` | UInt32 | Of games_after_bye, the games played 13 or more days after the team's previous game the team won (points for above points against, so a tie is not a win). |
| `plays_opener` | UInt32 | Plays in the team's regular-season openers. |
| `passes_opener` | UInt32 | Pass plays in the team's regular-season openers. |
| `epa_opener` | Float64 | Play EPA summed over the plays in the team's regular-season openers. |
| `successes_opener` | UInt32 | Plays flagged EPA_success (positive EPA) in the team's regular-season openers. |
| `games_opener` | UInt32 | Distinct regular-season openers in which the offense ran at least one standing scrimmage play. |
| `wins_opener` | UInt32 | Of games_opener, the regular-season openers the team won (points for above points against, so a tie is not a win). |
| `plays_one_score_game` | UInt32 | Plays in the team's games decided by 8 points or fewer. |
| `passes_one_score_game` | UInt32 | Pass plays in the team's games decided by 8 points or fewer. |
| `epa_one_score_game` | Float64 | Play EPA summed over the plays in the team's games decided by 8 points or fewer. |
| `successes_one_score_game` | UInt32 | Plays flagged EPA_success (positive EPA) in the team's games decided by 8 points or fewer. |
| `games_one_score_game` | UInt32 | Distinct games decided by 8 points or fewer in which the offense ran at least one standing scrimmage play. |
| `wins_one_score_game` | UInt32 | Of games_one_score_game, the games decided by 8 points or fewer the team won (points for above points against, so a tie is not a win). |
| `fourth_decisions` | UInt32 | Fourth-down plays on which the offense ran, passed, punted or attempted a field goal and the play stood (timeouts and nullified plays are not decisions). |
| `fourth_went` | UInt32 | Fourth-down decisions that were a rush or a pass (the offense went for it). |
| `fourth_converted` | UInt32 | Fourth-down go attempts that produced a first down or an offensive touchdown. |
| `fourth_model_go` | UInt32 | Decisions on which the fourth-down model recommended going for it (fourth_down_recommendation == "go"). |
| `fourth_model_kick` | UInt32 | Decisions on which the fourth-down model recommended a punt or a field goal. |
| `fourth_went_when_go` | UInt32 | Decisions on which the offense went for it when the model also said go. |
| `fourth_went_when_kick` | UInt32 | Decisions on which the offense went for it when the model said kick. |
| `fourth_agreed` | UInt32 | Decisions that matched the model's recommendation (went when it said go, kicked when it said kick). |
| `fourth_wp_left` | Float64 | Win probability left on the table, summed over the decisions that went against the model: go_boost when the offense kicked against a go recommendation, minus go_boost when it went against a kick recommendation, floored at zero. |
| `drives` | UInt32 | Offensive drives: distinct drive.id values with at least one standing scrimmage play. |
| `drives_with_clock` | UInt32 | Drives with a usable ESPN drive clock (a parseable drive.timeElapsed and a positive drive.offensivePlays), in regulation (overtime has no game clock) and owned by this offense (an ESPN drive id that holds snaps by both offenses counts once, for ESPN's drive team when it matches an offense in the drive, else the offense with the most standing snaps, the first snap breaking a tie). |
| `drive_seconds` | Float64 | ESPN elapsed drive time in seconds, summed over the drives with a usable clock. |
| `drive_plays` | Float64 | ESPN drive.offensivePlays summed over the drives with a usable clock -- the pace denominator. |
| `drive_seconds_neutral` | Float64 | ESPN elapsed drive seconds summed over the clocked drives whose first play was situation-neutral. |
| `drive_plays_neutral` | Float64 | ESPN drive.offensivePlays summed over the clocked drives whose first play was situation-neutral. |
| `drive_points` | Float64 | Drive points (touchdown 7, field goal 3, from drive.result) summed over the drives. |
| `rz_trips` | UInt32 | Drives with at least one play snapped in the red zone (20 or fewer yards to the end zone). |
| `rz_tds` | UInt32 | Red-zone drives that included an offensive touchdown play. |
| `rz_scores` | UInt32 | Red-zone drives that scored (a touchdown or a field goal). |
| `rz_points` | Float64 | Drive points (touchdown 7, field goal 3) summed over the red-zone drives. |
| `so_trips` | UInt32 | Drives with at least one play snapped in scoring-opportunity territory (40 or fewer yards to the end zone). |
| `so_tds` | UInt32 | Scoring-opportunity drives that included an offensive touchdown play. |
| `so_scores` | UInt32 | Scoring-opportunity drives that scored (a touchdown or a field goal). |
| `so_points` | Float64 | Drive points (touchdown 7, field goal 3) summed over the scoring-opportunity drives. |
| `scripted_drives` | UInt32 | The offense's first two drives of each half. |
| `scripted_plays` | UInt32 | Standing scrimmage plays on the scripted drives. |
| `scripted_epa` | Float64 | Play EPA summed over the scripted drives. |
| `scripted_successes` | UInt32 | Plays flagged EPA_success on the scripted drives. |
| `scripted_points` | Float64 | Drive points (touchdown 7, field goal 3) summed over the scripted drives. |
| `non_scripted_drives` | UInt32 | Every drive after the offense's first two of each half. |
| `non_scripted_plays` | UInt32 | Standing scrimmage plays on the non-scripted drives. |
| `non_scripted_epa` | Float64 | Play EPA summed over the non-scripted drives. |
| `non_scripted_successes` | UInt32 | Plays flagged EPA_success on the non-scripted drives. |
| `non_scripted_points` | Float64 | Drive points (touchdown 7, field goal 3) summed over the non-scripted drives. |
| `def_games` | UInt32 | Defense-allowed twin of games -- the same measure over the opposing offenses' plays while this coach's defense was on the field: distinct games in which the offense ran at least one standing scrimmage play. |
| `def_plays` | UInt32 | Defense-allowed twin of plays -- the same measure over the opposing offenses' plays while this coach's defense was on the field: standing scrimmage plays run by the offense (scrimmage_play rows not nullified by penalty). |
| `def_rushes` | UInt32 | Defense-allowed twin of rushes -- the same measure over the opposing offenses' plays while this coach's defense was on the field: rushing plays among the standing scrimmage plays. |
| `def_passes` | UInt32 | Defense-allowed twin of passes -- the same measure over the opposing offenses' plays while this coach's defense was on the field: pass plays among the standing scrimmage plays. |
| `def_epa` | Float64 | Defense-allowed twin of epa -- the same measure over the opposing offenses' plays while this coach's defense was on the field: play EPA summed over the standing scrimmage plays. |
| `def_epa_rush` | Float64 | Defense-allowed twin of epa_rush -- the same measure over the opposing offenses' plays while this coach's defense was on the field: play EPA summed over the rushing plays. |
| `def_epa_pass` | Float64 | Defense-allowed twin of epa_pass -- the same measure over the opposing offenses' plays while this coach's defense was on the field: play EPA summed over the pass plays. |
| `def_successes` | UInt32 | Defense-allowed twin of successes -- the same measure over the opposing offenses' plays while this coach's defense was on the field: plays flagged EPA_success (positive EPA) on the play-by-play. |
| `def_successes_rush` | UInt32 | Defense-allowed twin of successes_rush -- the same measure over the opposing offenses' plays while this coach's defense was on the field: rushing plays flagged EPA_success. |
| `def_successes_pass` | UInt32 | Defense-allowed twin of successes_pass -- the same measure over the opposing offenses' plays while this coach's defense was on the field: pass plays flagged EPA_success. |
| `def_yards` | Float64 | Defense-allowed twin of yards -- the same measure over the opposing offenses' plays while this coach's defense was on the field: statYardage summed over the standing scrimmage plays. |
| `def_yards_rush` | Float64 | Defense-allowed twin of yards_rush -- the same measure over the opposing offenses' plays while this coach's defense was on the field: statYardage summed over the rushing plays. |
| `def_yards_pass` | Float64 | Defense-allowed twin of yards_pass -- the same measure over the opposing offenses' plays while this coach's defense was on the field: statYardage summed over the pass plays. |
| `def_explosives` | UInt32 | Defense-allowed twin of explosives -- the same measure over the opposing offenses' plays while this coach's defense was on the field: plays flagged EPA_explosive on the play-by-play. |
| `def_explosives_rush` | UInt32 | Defense-allowed twin of explosives_rush -- the same measure over the opposing offenses' plays while this coach's defense was on the field: rushing plays flagged EPA_explosive. |
| `def_explosives_pass` | UInt32 | Defense-allowed twin of explosives_pass -- the same measure over the opposing offenses' plays while this coach's defense was on the field: pass plays flagged EPA_explosive. |
| `def_third_down_opportunities` | UInt32 | Defense-allowed twin of third_down_opportunities -- the same measure over the opposing offenses' plays while this coach's defense was on the field: third-down scrimmage plays. |
| `def_third_down_conversions` | UInt32 | Defense-allowed twin of third_down_conversions -- the same measure over the opposing offenses' plays while this coach's defense was on the field: third-down plays that produced a first down or an offensive touchdown. |
| `def_third_down_expected` | Float64 | Defense-allowed twin of third_down_expected -- the same measure over the opposing offenses' plays while this coach's defense was on the field: expected third-down conversions: the league's bundled third-down yards-to-go conversion curve summed over the third-down plays; null when no curve was available. |
| `def_plays_neutral` | UInt32 | Defense-allowed twin of plays_neutral -- the same measure over the opposing offenses' plays while this coach's defense was on the field: plays in situation-neutral plays: score-and-clock win probability (wp_before_naive, no pregame line) between 20% and 80%, in the first four quarters, outside the final two minutes of a half. |
| `def_passes_neutral` | UInt32 | Defense-allowed twin of passes_neutral -- the same measure over the opposing offenses' plays while this coach's defense was on the field: pass plays in situation-neutral situations (see plays_neutral). |
| `def_epa_neutral` | Float64 | Defense-allowed twin of epa_neutral -- the same measure over the opposing offenses' plays while this coach's defense was on the field: play EPA summed over situation-neutral plays: score-and-clock win probability (wp_before_naive, no pregame line) between 20% and 80%, in the first four quarters, outside the final two minutes of a half. |
| `def_successes_neutral` | UInt32 | Defense-allowed twin of successes_neutral -- the same measure over the opposing offenses' plays while this coach's defense was on the field: plays flagged EPA_success (positive EPA) in situation-neutral situations (score-and-clock win probability (wp_before_naive, no pregame line) between 20% and 80%, in the first four quarters, outside the final two minutes of a half). |
| `def_plays_d1` | UInt32 | Defense-allowed twin of plays_d1 -- the same measure over the opposing offenses' plays while this coach's defense was on the field: plays on first down. |
| `def_passes_d1` | UInt32 | Defense-allowed twin of passes_d1 -- the same measure over the opposing offenses' plays while this coach's defense was on the field: pass plays on first down. |
| `def_epa_d1` | Float64 | Defense-allowed twin of epa_d1 -- the same measure over the opposing offenses' plays while this coach's defense was on the field: play EPA summed over the plays on first down. |
| `def_successes_d1` | UInt32 | Defense-allowed twin of successes_d1 -- the same measure over the opposing offenses' plays while this coach's defense was on the field: plays flagged EPA_success (positive EPA) on first down. |
| `def_plays_d2` | UInt32 | Defense-allowed twin of plays_d2 -- the same measure over the opposing offenses' plays while this coach's defense was on the field: plays on second down. |
| `def_passes_d2` | UInt32 | Defense-allowed twin of passes_d2 -- the same measure over the opposing offenses' plays while this coach's defense was on the field: pass plays on second down. |
| `def_epa_d2` | Float64 | Defense-allowed twin of epa_d2 -- the same measure over the opposing offenses' plays while this coach's defense was on the field: play EPA summed over the plays on second down. |
| `def_successes_d2` | UInt32 | Defense-allowed twin of successes_d2 -- the same measure over the opposing offenses' plays while this coach's defense was on the field: plays flagged EPA_success (positive EPA) on second down. |
| `def_plays_d3` | UInt32 | Defense-allowed twin of plays_d3 -- the same measure over the opposing offenses' plays while this coach's defense was on the field: plays on third down. |
| `def_passes_d3` | UInt32 | Defense-allowed twin of passes_d3 -- the same measure over the opposing offenses' plays while this coach's defense was on the field: pass plays on third down. |
| `def_epa_d3` | Float64 | Defense-allowed twin of epa_d3 -- the same measure over the opposing offenses' plays while this coach's defense was on the field: play EPA summed over the plays on third down. |
| `def_successes_d3` | UInt32 | Defense-allowed twin of successes_d3 -- the same measure over the opposing offenses' plays while this coach's defense was on the field: plays flagged EPA_success (positive EPA) on third down. |
| `def_plays_d4` | UInt32 | Defense-allowed twin of plays_d4 -- the same measure over the opposing offenses' plays while this coach's defense was on the field: plays on fourth down. |
| `def_passes_d4` | UInt32 | Defense-allowed twin of passes_d4 -- the same measure over the opposing offenses' plays while this coach's defense was on the field: pass plays on fourth down. |
| `def_epa_d4` | Float64 | Defense-allowed twin of epa_d4 -- the same measure over the opposing offenses' plays while this coach's defense was on the field: play EPA summed over the plays on fourth down. |
| `def_successes_d4` | UInt32 | Defense-allowed twin of successes_d4 -- the same measure over the opposing offenses' plays while this coach's defense was on the field: plays flagged EPA_success (positive EPA) on fourth down. |
| `def_plays_early_down` | UInt32 | Defense-allowed twin of plays_early_down -- the same measure over the opposing offenses' plays while this coach's defense was on the field: plays on first or second down. |
| `def_passes_early_down` | UInt32 | Defense-allowed twin of passes_early_down -- the same measure over the opposing offenses' plays while this coach's defense was on the field: pass plays on first or second down. |
| `def_epa_early_down` | Float64 | Defense-allowed twin of epa_early_down -- the same measure over the opposing offenses' plays while this coach's defense was on the field: play EPA summed over the first- and second-down plays. |
| `def_successes_early_down` | UInt32 | Defense-allowed twin of successes_early_down -- the same measure over the opposing offenses' plays while this coach's defense was on the field: plays flagged EPA_success (positive EPA) on first or second down. |
| `def_plays_standard_down` | UInt32 | Defense-allowed twin of plays_standard_down -- the same measure over the opposing offenses' plays while this coach's defense was on the field: plays flagged standard_down on the play-by-play: first down, second down with fewer than 8 to go, or third / fourth down with fewer than 5 to go. |
| `def_passes_standard_down` | UInt32 | Defense-allowed twin of passes_standard_down -- the same measure over the opposing offenses' plays while this coach's defense was on the field: pass plays on standard downs (see plays_standard_down). |
| `def_epa_standard_down` | Float64 | Defense-allowed twin of epa_standard_down -- the same measure over the opposing offenses' plays while this coach's defense was on the field: play EPA summed over the plays on standard downs (the play-by-play standard_down flag). |
| `def_successes_standard_down` | UInt32 | Defense-allowed twin of successes_standard_down -- the same measure over the opposing offenses' plays while this coach's defense was on the field: plays flagged EPA_success (positive EPA) on standard downs (the play-by-play standard_down flag). |
| `def_plays_passing_down` | UInt32 | Defense-allowed twin of plays_passing_down -- the same measure over the opposing offenses' plays while this coach's defense was on the field: plays flagged passing_down on the play-by-play: second down with 8 or more to go, or third / fourth down with 5 or more to go. |
| `def_passes_passing_down` | UInt32 | Defense-allowed twin of passes_passing_down -- the same measure over the opposing offenses' plays while this coach's defense was on the field: pass plays on passing downs (see plays_passing_down). |
| `def_epa_passing_down` | Float64 | Defense-allowed twin of epa_passing_down -- the same measure over the opposing offenses' plays while this coach's defense was on the field: play EPA summed over the plays on passing downs (the play-by-play passing_down flag). |
| `def_successes_passing_down` | UInt32 | Defense-allowed twin of successes_passing_down -- the same measure over the opposing offenses' plays while this coach's defense was on the field: plays flagged EPA_success (positive EPA) on passing downs (the play-by-play passing_down flag). |
| `def_plays_leading` | UInt32 | Defense-allowed twin of plays_leading -- the same measure over the opposing offenses' plays while this coach's defense was on the field: plays snapped with the offense ahead on the scoreboard (pos_score_diff_start > 0, the score at the snap, before the play; pos_score_diff where the start score is missing). |
| `def_passes_leading` | UInt32 | Defense-allowed twin of passes_leading -- the same measure over the opposing offenses' plays while this coach's defense was on the field: pass plays snapped with the offense ahead on the scoreboard (pos_score_diff_start > 0, the score at the snap, before the play; pos_score_diff where the start score is missing). |
| `def_epa_leading` | Float64 | Defense-allowed twin of epa_leading -- the same measure over the opposing offenses' plays while this coach's defense was on the field: play EPA summed over the plays snapped with the offense ahead on the scoreboard (pos_score_diff_start > 0, the score at the snap, before the play; pos_score_diff where the start score is missing). |
| `def_successes_leading` | UInt32 | Defense-allowed twin of successes_leading -- the same measure over the opposing offenses' plays while this coach's defense was on the field: plays flagged EPA_success (positive EPA) snapped with the offense ahead on the scoreboard (pos_score_diff_start > 0, the score at the snap, before the play; pos_score_diff where the start score is missing). |
| `def_plays_tied` | UInt32 | Defense-allowed twin of plays_tied -- the same measure over the opposing offenses' plays while this coach's defense was on the field: plays snapped with the score tied (pos_score_diff_start == 0, the score at the snap, before the play; pos_score_diff where the start score is missing). |
| `def_passes_tied` | UInt32 | Defense-allowed twin of passes_tied -- the same measure over the opposing offenses' plays while this coach's defense was on the field: pass plays snapped with the score tied (pos_score_diff_start == 0, the score at the snap, before the play; pos_score_diff where the start score is missing). |
| `def_epa_tied` | Float64 | Defense-allowed twin of epa_tied -- the same measure over the opposing offenses' plays while this coach's defense was on the field: play EPA summed over the plays snapped with the score tied (pos_score_diff_start == 0, the score at the snap, before the play; pos_score_diff where the start score is missing). |
| `def_successes_tied` | UInt32 | Defense-allowed twin of successes_tied -- the same measure over the opposing offenses' plays while this coach's defense was on the field: plays flagged EPA_success (positive EPA) snapped with the score tied (pos_score_diff_start == 0, the score at the snap, before the play; pos_score_diff where the start score is missing). |
| `def_plays_trailing` | UInt32 | Defense-allowed twin of plays_trailing -- the same measure over the opposing offenses' plays while this coach's defense was on the field: plays snapped with the offense behind on the scoreboard (pos_score_diff_start < 0, the score at the snap, before the play; pos_score_diff where the start score is missing). |
| `def_passes_trailing` | UInt32 | Defense-allowed twin of passes_trailing -- the same measure over the opposing offenses' plays while this coach's defense was on the field: pass plays snapped with the offense behind on the scoreboard (pos_score_diff_start < 0, the score at the snap, before the play; pos_score_diff where the start score is missing). |
| `def_epa_trailing` | Float64 | Defense-allowed twin of epa_trailing -- the same measure over the opposing offenses' plays while this coach's defense was on the field: play EPA summed over the plays snapped with the offense behind on the scoreboard (pos_score_diff_start < 0, the score at the snap, before the play; pos_score_diff where the start score is missing). |
| `def_successes_trailing` | UInt32 | Defense-allowed twin of successes_trailing -- the same measure over the opposing offenses' plays while this coach's defense was on the field: plays flagged EPA_success (positive EPA) snapped with the offense behind on the scoreboard (pos_score_diff_start < 0, the score at the snap, before the play; pos_score_diff where the start score is missing). |
| `def_plays_first_half` | UInt32 | Defense-allowed twin of plays_first_half -- the same measure over the opposing offenses' plays while this coach's defense was on the field: plays in the first two quarters. |
| `def_passes_first_half` | UInt32 | Defense-allowed twin of passes_first_half -- the same measure over the opposing offenses' plays while this coach's defense was on the field: pass plays in the first two quarters. |
| `def_epa_first_half` | Float64 | Defense-allowed twin of epa_first_half -- the same measure over the opposing offenses' plays while this coach's defense was on the field: play EPA summed over the plays in the first two quarters. |
| `def_successes_first_half` | UInt32 | Defense-allowed twin of successes_first_half -- the same measure over the opposing offenses' plays while this coach's defense was on the field: plays flagged EPA_success (positive EPA) in the first two quarters. |
| `def_plays_second_half` | UInt32 | Defense-allowed twin of plays_second_half -- the same measure over the opposing offenses' plays while this coach's defense was on the field: plays in the third and fourth quarters (overtime belongs to neither half). |
| `def_passes_second_half` | UInt32 | Defense-allowed twin of passes_second_half -- the same measure over the opposing offenses' plays while this coach's defense was on the field: pass plays in the third and fourth quarters. |
| `def_epa_second_half` | Float64 | Defense-allowed twin of epa_second_half -- the same measure over the opposing offenses' plays while this coach's defense was on the field: play EPA summed over the plays in the third and fourth quarters (overtime belongs to neither half). |
| `def_successes_second_half` | UInt32 | Defense-allowed twin of successes_second_half -- the same measure over the opposing offenses' plays while this coach's defense was on the field: plays flagged EPA_success (positive EPA) in the third and fourth quarters (overtime belongs to neither half). |
| `def_plays_d3_short` | UInt32 | Defense-allowed twin of plays_d3_short -- the same measure over the opposing offenses' plays while this coach's defense was on the field: plays on third down with 3 or fewer yards to go. |
| `def_passes_d3_short` | UInt32 | Defense-allowed twin of passes_d3_short -- the same measure over the opposing offenses' plays while this coach's defense was on the field: pass plays on third down with 3 or fewer yards to go. |
| `def_epa_d3_short` | Float64 | Defense-allowed twin of epa_d3_short -- the same measure over the opposing offenses' plays while this coach's defense was on the field: play EPA summed over the plays on third down with 3 or fewer yards to go. |
| `def_successes_d3_short` | UInt32 | Defense-allowed twin of successes_d3_short -- the same measure over the opposing offenses' plays while this coach's defense was on the field: plays flagged EPA_success (positive EPA) on third down with 3 or fewer yards to go. |
| `def_plays_d3_medium` | UInt32 | Defense-allowed twin of plays_d3_medium -- the same measure over the opposing offenses' plays while this coach's defense was on the field: plays on third down with 4 to 6 yards to go. |
| `def_passes_d3_medium` | UInt32 | Defense-allowed twin of passes_d3_medium -- the same measure over the opposing offenses' plays while this coach's defense was on the field: pass plays on third down with 4 to 6 yards to go. |
| `def_epa_d3_medium` | Float64 | Defense-allowed twin of epa_d3_medium -- the same measure over the opposing offenses' plays while this coach's defense was on the field: play EPA summed over the plays on third down with 4 to 6 yards to go. |
| `def_successes_d3_medium` | UInt32 | Defense-allowed twin of successes_d3_medium -- the same measure over the opposing offenses' plays while this coach's defense was on the field: plays flagged EPA_success (positive EPA) on third down with 4 to 6 yards to go. |
| `def_plays_d3_long` | UInt32 | Defense-allowed twin of plays_d3_long -- the same measure over the opposing offenses' plays while this coach's defense was on the field: plays on third down with 7 or more yards to go. |
| `def_passes_d3_long` | UInt32 | Defense-allowed twin of passes_d3_long -- the same measure over the opposing offenses' plays while this coach's defense was on the field: pass plays on third down with 7 or more yards to go. |
| `def_epa_d3_long` | Float64 | Defense-allowed twin of epa_d3_long -- the same measure over the opposing offenses' plays while this coach's defense was on the field: play EPA summed over the plays on third down with 7 or more yards to go. |
| `def_successes_d3_long` | UInt32 | Defense-allowed twin of successes_d3_long -- the same measure over the opposing offenses' plays while this coach's defense was on the field: plays flagged EPA_success (positive EPA) on third down with 7 or more yards to go. |
| `def_plays_red_zone` | UInt32 | Defense-allowed twin of plays_red_zone -- the same measure over the opposing offenses' plays while this coach's defense was on the field: plays in the red zone (20 or fewer yards from the opponent end zone, read from yards to goal rather than the absolute yard line; the play-by-play rz_play flag only when that distance is missing). |
| `def_passes_red_zone` | UInt32 | Defense-allowed twin of passes_red_zone -- the same measure over the opposing offenses' plays while this coach's defense was on the field: pass plays in the red zone (20 or fewer yards from the opponent end zone, read from yards to goal rather than the absolute yard line; the play-by-play rz_play flag only when that distance is missing). |
| `def_epa_red_zone` | Float64 | Defense-allowed twin of epa_red_zone -- the same measure over the opposing offenses' plays while this coach's defense was on the field: play EPA summed over the plays in the red zone (20 or fewer yards from the opponent end zone, read from yards to goal rather than the absolute yard line; the play-by-play rz_play flag only when that distance is missing). |
| `def_successes_red_zone` | UInt32 | Defense-allowed twin of successes_red_zone -- the same measure over the opposing offenses' plays while this coach's defense was on the field: plays flagged EPA_success (positive EPA) in the red zone (20 or fewer yards from the opponent end zone, read from yards to goal rather than the absolute yard line; the play-by-play rz_play flag only when that distance is missing). |
| `def_plays_own_half` | UInt32 | Defense-allowed twin of plays_own_half -- the same measure over the opposing offenses' plays while this coach's defense was on the field: plays snapped in the offense's own half (50 or more yards from the opponent end zone). |
| `def_passes_own_half` | UInt32 | Defense-allowed twin of passes_own_half -- the same measure over the opposing offenses' plays while this coach's defense was on the field: pass plays snapped in the offense's own half (50 or more yards from the opponent end zone). |
| `def_epa_own_half` | Float64 | Defense-allowed twin of epa_own_half -- the same measure over the opposing offenses' plays while this coach's defense was on the field: play EPA summed over the plays snapped in the offense's own half (50 or more yards from the opponent end zone). |
| `def_successes_own_half` | UInt32 | Defense-allowed twin of successes_own_half -- the same measure over the opposing offenses' plays while this coach's defense was on the field: plays flagged EPA_success (positive EPA) snapped in the offense's own half (50 or more yards from the opponent end zone). |
| `def_plays_opp_half` | UInt32 | Defense-allowed twin of plays_opp_half -- the same measure over the opposing offenses' plays while this coach's defense was on the field: plays snapped in the opponent's half (fewer than 50 yards from the opponent end zone). |
| `def_passes_opp_half` | UInt32 | Defense-allowed twin of passes_opp_half -- the same measure over the opposing offenses' plays while this coach's defense was on the field: pass plays snapped in the opponent's half (fewer than 50 yards from the opponent end zone). |
| `def_epa_opp_half` | Float64 | Defense-allowed twin of epa_opp_half -- the same measure over the opposing offenses' plays while this coach's defense was on the field: play EPA summed over the plays snapped in the opponent's half (fewer than 50 yards from the opponent end zone). |
| `def_successes_opp_half` | UInt32 | Defense-allowed twin of successes_opp_half -- the same measure over the opposing offenses' plays while this coach's defense was on the field: plays flagged EPA_success (positive EPA) snapped in the opponent's half (fewer than 50 yards from the opponent end zone). |
| `def_plays_one_score` | UInt32 | Defense-allowed twin of plays_one_score -- the same measure over the opposing offenses' plays while this coach's defense was on the field: plays snapped with the offense within 8 points either way (\|pos_score_diff_start\| <= 8, the score at the snap, before the play; pos_score_diff where the start score is missing). |
| `def_passes_one_score` | UInt32 | Defense-allowed twin of passes_one_score -- the same measure over the opposing offenses' plays while this coach's defense was on the field: pass plays snapped with the offense within 8 points either way (\|pos_score_diff_start\| <= 8, the score at the snap, before the play; pos_score_diff where the start score is missing). |
| `def_epa_one_score` | Float64 | Defense-allowed twin of epa_one_score -- the same measure over the opposing offenses' plays while this coach's defense was on the field: play EPA summed over the plays snapped with the offense within 8 points either way (\|pos_score_diff_start\| <= 8, the score at the snap, before the play; pos_score_diff where the start score is missing). |
| `def_successes_one_score` | UInt32 | Defense-allowed twin of successes_one_score -- the same measure over the opposing offenses' plays while this coach's defense was on the field: plays flagged EPA_success (positive EPA) snapped with the offense within 8 points either way (\|pos_score_diff_start\| <= 8, the score at the snap, before the play; pos_score_diff where the start score is missing). |
| `def_plays_home` | UInt32 | Defense-allowed twin of plays_home -- the same measure over the opposing offenses' plays while this coach's defense was on the field, with the game context read from the defending team's side (def_ctx_*): plays in the team's home games. |
| `def_passes_home` | UInt32 | Defense-allowed twin of passes_home -- the same measure over the opposing offenses' plays while this coach's defense was on the field, with the game context read from the defending team's side (def_ctx_*): pass plays in the team's home games. |
| `def_epa_home` | Float64 | Defense-allowed twin of epa_home -- the same measure over the opposing offenses' plays while this coach's defense was on the field, with the game context read from the defending team's side (def_ctx_*): play EPA summed over the plays in the team's home games. |
| `def_successes_home` | UInt32 | Defense-allowed twin of successes_home -- the same measure over the opposing offenses' plays while this coach's defense was on the field, with the game context read from the defending team's side (def_ctx_*): plays flagged EPA_success (positive EPA) in the team's home games. |
| `def_games_home` | UInt32 | Defense-allowed twin of games_home -- the same measure over the opposing offenses' plays while this coach's defense was on the field, with the game context read from the defending team's side (def_ctx_*): distinct home games in which the offense ran at least one standing scrimmage play. |
| `def_wins_home` | UInt32 | Defense-allowed twin of wins_home -- the same measure over the opposing offenses' plays while this coach's defense was on the field, with the game context read from the defending team's side (def_ctx_*): of games_home, the home games the team won (points for above points against, so a tie is not a win). |
| `def_plays_away` | UInt32 | Defense-allowed twin of plays_away -- the same measure over the opposing offenses' plays while this coach's defense was on the field, with the game context read from the defending team's side (def_ctx_*): plays in the team's away games. |
| `def_passes_away` | UInt32 | Defense-allowed twin of passes_away -- the same measure over the opposing offenses' plays while this coach's defense was on the field, with the game context read from the defending team's side (def_ctx_*): pass plays in the team's away games. |
| `def_epa_away` | Float64 | Defense-allowed twin of epa_away -- the same measure over the opposing offenses' plays while this coach's defense was on the field, with the game context read from the defending team's side (def_ctx_*): play EPA summed over the plays in the team's away games. |
| `def_successes_away` | UInt32 | Defense-allowed twin of successes_away -- the same measure over the opposing offenses' plays while this coach's defense was on the field, with the game context read from the defending team's side (def_ctx_*): plays flagged EPA_success (positive EPA) in the team's away games. |
| `def_games_away` | UInt32 | Defense-allowed twin of games_away -- the same measure over the opposing offenses' plays while this coach's defense was on the field, with the game context read from the defending team's side (def_ctx_*): distinct away games in which the offense ran at least one standing scrimmage play. |
| `def_wins_away` | UInt32 | Defense-allowed twin of wins_away -- the same measure over the opposing offenses' plays while this coach's defense was on the field, with the game context read from the defending team's side (def_ctx_*): of games_away, the away games the team won (points for above points against, so a tie is not a win). |
| `def_plays_neutral_site` | UInt32 | Defense-allowed twin of plays_neutral_site -- the same measure over the opposing offenses' plays while this coach's defense was on the field, with the game context read from the defending team's side (def_ctx_*): plays in the team's neutral-site games. |
| `def_passes_neutral_site` | UInt32 | Defense-allowed twin of passes_neutral_site -- the same measure over the opposing offenses' plays while this coach's defense was on the field, with the game context read from the defending team's side (def_ctx_*): pass plays in the team's neutral-site games. |
| `def_epa_neutral_site` | Float64 | Defense-allowed twin of epa_neutral_site -- the same measure over the opposing offenses' plays while this coach's defense was on the field, with the game context read from the defending team's side (def_ctx_*): play EPA summed over the plays in the team's neutral-site games. |
| `def_successes_neutral_site` | UInt32 | Defense-allowed twin of successes_neutral_site -- the same measure over the opposing offenses' plays while this coach's defense was on the field, with the game context read from the defending team's side (def_ctx_*): plays flagged EPA_success (positive EPA) in the team's neutral-site games. |
| `def_games_neutral_site` | UInt32 | Defense-allowed twin of games_neutral_site -- the same measure over the opposing offenses' plays while this coach's defense was on the field, with the game context read from the defending team's side (def_ctx_*): distinct neutral-site games in which the offense ran at least one standing scrimmage play. |
| `def_wins_neutral_site` | UInt32 | Defense-allowed twin of wins_neutral_site -- the same measure over the opposing offenses' plays while this coach's defense was on the field, with the game context read from the defending team's side (def_ctx_*): of games_neutral_site, the neutral-site games the team won (points for above points against, so a tie is not a win). |
| `def_plays_vs_ranked` | UInt32 | Defense-allowed twin of plays_vs_ranked -- the same measure over the opposing offenses' plays while this coach's defense was on the field, with the game context read from the defending team's side (def_ctx_*): plays in the team's games against an opponent ranked at kickoff (ESPN's displayed rank). |
| `def_passes_vs_ranked` | UInt32 | Defense-allowed twin of passes_vs_ranked -- the same measure over the opposing offenses' plays while this coach's defense was on the field, with the game context read from the defending team's side (def_ctx_*): pass plays in the team's games against an opponent ranked at kickoff (ESPN's displayed rank). |
| `def_epa_vs_ranked` | Float64 | Defense-allowed twin of epa_vs_ranked -- the same measure over the opposing offenses' plays while this coach's defense was on the field, with the game context read from the defending team's side (def_ctx_*): play EPA summed over the plays in the team's games against an opponent ranked at kickoff (ESPN's displayed rank). |
| `def_successes_vs_ranked` | UInt32 | Defense-allowed twin of successes_vs_ranked -- the same measure over the opposing offenses' plays while this coach's defense was on the field, with the game context read from the defending team's side (def_ctx_*): plays flagged EPA_success (positive EPA) in the team's games against an opponent ranked at kickoff (ESPN's displayed rank). |
| `def_games_vs_ranked` | UInt32 | Defense-allowed twin of games_vs_ranked -- the same measure over the opposing offenses' plays while this coach's defense was on the field, with the game context read from the defending team's side (def_ctx_*): distinct games against an opponent ranked at kickoff (ESPN's displayed rank) in which the offense ran at least one standing scrimmage play. |
| `def_wins_vs_ranked` | UInt32 | Defense-allowed twin of wins_vs_ranked -- the same measure over the opposing offenses' plays while this coach's defense was on the field, with the game context read from the defending team's side (def_ctx_*): of games_vs_ranked, the games against an opponent ranked at kickoff (ESPN's displayed rank) the team won (points for above points against, so a tie is not a win). |
| `def_plays_after_bye` | UInt32 | Defense-allowed twin of plays_after_bye -- the same measure over the opposing offenses' plays while this coach's defense was on the field, with the game context read from the defending team's side (def_ctx_*): plays in the team's games played 13 or more days after the team's previous game. |
| `def_passes_after_bye` | UInt32 | Defense-allowed twin of passes_after_bye -- the same measure over the opposing offenses' plays while this coach's defense was on the field, with the game context read from the defending team's side (def_ctx_*): pass plays in the team's games played 13 or more days after the team's previous game. |
| `def_epa_after_bye` | Float64 | Defense-allowed twin of epa_after_bye -- the same measure over the opposing offenses' plays while this coach's defense was on the field, with the game context read from the defending team's side (def_ctx_*): play EPA summed over the plays in the team's games played 13 or more days after the team's previous game. |
| `def_successes_after_bye` | UInt32 | Defense-allowed twin of successes_after_bye -- the same measure over the opposing offenses' plays while this coach's defense was on the field, with the game context read from the defending team's side (def_ctx_*): plays flagged EPA_success (positive EPA) in the team's games played 13 or more days after the team's previous game. |
| `def_games_after_bye` | UInt32 | Defense-allowed twin of games_after_bye -- the same measure over the opposing offenses' plays while this coach's defense was on the field, with the game context read from the defending team's side (def_ctx_*): distinct games played 13 or more days after the team's previous game in which the offense ran at least one standing scrimmage play. |
| `def_wins_after_bye` | UInt32 | Defense-allowed twin of wins_after_bye -- the same measure over the opposing offenses' plays while this coach's defense was on the field, with the game context read from the defending team's side (def_ctx_*): of games_after_bye, the games played 13 or more days after the team's previous game the team won (points for above points against, so a tie is not a win). |
| `def_plays_opener` | UInt32 | Defense-allowed twin of plays_opener -- the same measure over the opposing offenses' plays while this coach's defense was on the field, with the game context read from the defending team's side (def_ctx_*): plays in the team's regular-season openers. |
| `def_passes_opener` | UInt32 | Defense-allowed twin of passes_opener -- the same measure over the opposing offenses' plays while this coach's defense was on the field, with the game context read from the defending team's side (def_ctx_*): pass plays in the team's regular-season openers. |
| `def_epa_opener` | Float64 | Defense-allowed twin of epa_opener -- the same measure over the opposing offenses' plays while this coach's defense was on the field, with the game context read from the defending team's side (def_ctx_*): play EPA summed over the plays in the team's regular-season openers. |
| `def_successes_opener` | UInt32 | Defense-allowed twin of successes_opener -- the same measure over the opposing offenses' plays while this coach's defense was on the field, with the game context read from the defending team's side (def_ctx_*): plays flagged EPA_success (positive EPA) in the team's regular-season openers. |
| `def_games_opener` | UInt32 | Defense-allowed twin of games_opener -- the same measure over the opposing offenses' plays while this coach's defense was on the field, with the game context read from the defending team's side (def_ctx_*): distinct regular-season openers in which the offense ran at least one standing scrimmage play. |
| `def_wins_opener` | UInt32 | Defense-allowed twin of wins_opener -- the same measure over the opposing offenses' plays while this coach's defense was on the field, with the game context read from the defending team's side (def_ctx_*): of games_opener, the regular-season openers the team won (points for above points against, so a tie is not a win). |
| `def_plays_one_score_game` | UInt32 | Defense-allowed twin of plays_one_score_game -- the same measure over the opposing offenses' plays while this coach's defense was on the field, with the game context read from the defending team's side (def_ctx_*): plays in the team's games decided by 8 points or fewer. |
| `def_passes_one_score_game` | UInt32 | Defense-allowed twin of passes_one_score_game -- the same measure over the opposing offenses' plays while this coach's defense was on the field, with the game context read from the defending team's side (def_ctx_*): pass plays in the team's games decided by 8 points or fewer. |
| `def_epa_one_score_game` | Float64 | Defense-allowed twin of epa_one_score_game -- the same measure over the opposing offenses' plays while this coach's defense was on the field, with the game context read from the defending team's side (def_ctx_*): play EPA summed over the plays in the team's games decided by 8 points or fewer. |
| `def_successes_one_score_game` | UInt32 | Defense-allowed twin of successes_one_score_game -- the same measure over the opposing offenses' plays while this coach's defense was on the field, with the game context read from the defending team's side (def_ctx_*): plays flagged EPA_success (positive EPA) in the team's games decided by 8 points or fewer. |
| `def_games_one_score_game` | UInt32 | Defense-allowed twin of games_one_score_game -- the same measure over the opposing offenses' plays while this coach's defense was on the field, with the game context read from the defending team's side (def_ctx_*): distinct games decided by 8 points or fewer in which the offense ran at least one standing scrimmage play. |
| `def_wins_one_score_game` | UInt32 | Defense-allowed twin of wins_one_score_game -- the same measure over the opposing offenses' plays while this coach's defense was on the field, with the game context read from the defending team's side (def_ctx_*): of games_one_score_game, the games decided by 8 points or fewer the team won (points for above points against, so a tie is not a win). |
| `def_drives` | UInt32 | Defense-allowed twin of drives -- the same measure over the opposing offenses' plays while this coach's defense was on the field: offensive drives: distinct drive.id values with at least one standing scrimmage play. |
| `def_drives_with_clock` | UInt32 | Defense-allowed twin of drives_with_clock -- the same measure over the opposing offenses' plays while this coach's defense was on the field: drives with a usable ESPN drive clock (a parseable drive.timeElapsed and a positive drive.offensivePlays). |
| `def_drive_seconds` | Float64 | Defense-allowed twin of drive_seconds -- the same measure over the opposing offenses' plays while this coach's defense was on the field: eSPN elapsed drive time in seconds, summed over the drives with a usable clock. |
| `def_drive_plays` | Float64 | Defense-allowed twin of drive_plays -- the same measure over the opposing offenses' plays while this coach's defense was on the field: eSPN drive.offensivePlays summed over the drives with a usable clock -- the pace denominator. |
| `def_drive_seconds_neutral` | Float64 | Defense-allowed twin of drive_seconds_neutral -- the same measure over the opposing offenses' plays while this coach's defense was on the field: eSPN elapsed drive seconds summed over the clocked drives whose first play was situation-neutral. |
| `def_drive_plays_neutral` | Float64 | Defense-allowed twin of drive_plays_neutral -- the same measure over the opposing offenses' plays while this coach's defense was on the field: eSPN drive.offensivePlays summed over the clocked drives whose first play was situation-neutral. |
| `def_drive_points` | Float64 | Defense-allowed twin of drive_points -- the same measure over the opposing offenses' plays while this coach's defense was on the field: drive points (touchdown 7, field goal 3, from drive.result) summed over the drives. |
| `def_rz_trips` | UInt32 | Defense-allowed twin of rz_trips -- the same measure over the opposing offenses' plays while this coach's defense was on the field: drives with at least one play snapped in the red zone (20 or fewer yards to the end zone). |
| `def_rz_tds` | UInt32 | Defense-allowed twin of rz_tds -- the same measure over the opposing offenses' plays while this coach's defense was on the field: red-zone drives that included an offensive touchdown play. |
| `def_rz_scores` | UInt32 | Defense-allowed twin of rz_scores -- the same measure over the opposing offenses' plays while this coach's defense was on the field: red-zone drives that scored (a touchdown or a field goal). |
| `def_rz_points` | Float64 | Defense-allowed twin of rz_points -- the same measure over the opposing offenses' plays while this coach's defense was on the field: drive points (touchdown 7, field goal 3) summed over the red-zone drives. |
| `def_so_trips` | UInt32 | Defense-allowed twin of so_trips -- the same measure over the opposing offenses' plays while this coach's defense was on the field: drives with at least one play snapped in scoring-opportunity territory (40 or fewer yards to the end zone). |
| `def_so_tds` | UInt32 | Defense-allowed twin of so_tds -- the same measure over the opposing offenses' plays while this coach's defense was on the field: scoring-opportunity drives that included an offensive touchdown play. |
| `def_so_scores` | UInt32 | Defense-allowed twin of so_scores -- the same measure over the opposing offenses' plays while this coach's defense was on the field: scoring-opportunity drives that scored (a touchdown or a field goal). |
| `def_so_points` | Float64 | Defense-allowed twin of so_points -- the same measure over the opposing offenses' plays while this coach's defense was on the field: drive points (touchdown 7, field goal 3) summed over the scoring-opportunity drives. |
| `def_scripted_drives` | UInt32 | Defense-allowed twin of scripted_drives -- the same measure over the opposing offenses' plays while this coach's defense was on the field: the offense's first two drives of each half. |
| `def_scripted_plays` | UInt32 | Defense-allowed twin of scripted_plays -- the same measure over the opposing offenses' plays while this coach's defense was on the field: standing scrimmage plays on the scripted drives. |
| `def_scripted_epa` | Float64 | Defense-allowed twin of scripted_epa -- the same measure over the opposing offenses' plays while this coach's defense was on the field: play EPA summed over the scripted drives. |
| `def_scripted_successes` | UInt32 | Defense-allowed twin of scripted_successes -- the same measure over the opposing offenses' plays while this coach's defense was on the field: plays flagged EPA_success on the scripted drives. |
| `def_scripted_points` | Float64 | Defense-allowed twin of scripted_points -- the same measure over the opposing offenses' plays while this coach's defense was on the field: drive points (touchdown 7, field goal 3) summed over the scripted drives. |
| `def_non_scripted_drives` | UInt32 | Defense-allowed twin of non_scripted_drives -- the same measure over the opposing offenses' plays while this coach's defense was on the field: every drive after the offense's first two of each half. |
| `def_non_scripted_plays` | UInt32 | Defense-allowed twin of non_scripted_plays -- the same measure over the opposing offenses' plays while this coach's defense was on the field: standing scrimmage plays on the non-scripted drives. |
| `def_non_scripted_epa` | Float64 | Defense-allowed twin of non_scripted_epa -- the same measure over the opposing offenses' plays while this coach's defense was on the field: play EPA summed over the non-scripted drives. |
| `def_non_scripted_successes` | UInt32 | Defense-allowed twin of non_scripted_successes -- the same measure over the opposing offenses' plays while this coach's defense was on the field: plays flagged EPA_success on the non-scripted drives. |
| `def_non_scripted_points` | Float64 | Defense-allowed twin of non_scripted_points -- the same measure over the opposing offenses' plays while this coach's defense was on the field: drive points (touchdown 7, field goal 3) summed over the non-scripted drives. |
| `plays_per_game` | Float64 | plays / games. Null when the denominator is 0. |
| `plays_per_drive` | Float64 | plays / drives. Null when the denominator is 0. |
| `drives_per_game` | Float64 | drives / games. Null when the denominator is 0. |
| `sec_per_play` | Float64 | drive_seconds / drive_plays: seconds of game clock per offensive play from ESPN's own drive clock (pace; lower is faster). Null when the denominator is 0. |
| `sec_per_play_neutral` | Float64 | drive_seconds_neutral / drive_plays_neutral: the same pace measure on drives that started situation-neutral. Null when the denominator is 0. |
| `pace_coverage` | Float64 | drives_with_clock / drives: the share of drives with a usable clock; treat sec_per_play with caution when this is low. Null when the denominator is 0. |
| `pass_rate` | Float64 | passes / plays. Null when the denominator is 0. |
| `epa_per_play` | Float64 | epa / plays. Null when the denominator is 0. |
| `epa_per_rush` | Float64 | epa_rush / rushes. Null when the denominator is 0. |
| `epa_per_pass` | Float64 | epa_pass / passes. Null when the denominator is 0. |
| `success_rate` | Float64 | successes / plays. Null when the denominator is 0. |
| `success_rate_rush` | Float64 | successes_rush / rushes. Null when the denominator is 0. |
| `success_rate_pass` | Float64 | successes_pass / passes. Null when the denominator is 0. |
| `pass_rate_neutral` | Float64 | passes_neutral / plays_neutral: pass rate in situation-neutral situations. Null when the denominator is 0. |
| `epa_per_play_neutral` | Float64 | epa_neutral / plays_neutral. Null when the denominator is 0. |
| `success_rate_neutral` | Float64 | successes_neutral / plays_neutral: success rate in situation-neutral situations (score-and-clock win probability (wp_before_naive, no pregame line) between 20% and 80%, in the first four quarters, outside the final two minutes of a half). Null when the denominator is 0. |
| `pass_rate_d1` | Float64 | passes_d1 / plays_d1: pass rate on first down. Null when the denominator is 0. |
| `epa_per_play_d1` | Float64 | epa_d1 / plays_d1: EPA per play on first down. Null when the denominator is 0. |
| `success_rate_d1` | Float64 | successes_d1 / plays_d1: success rate on first down. Null when the denominator is 0. |
| `pass_rate_d2` | Float64 | passes_d2 / plays_d2: pass rate on second down. Null when the denominator is 0. |
| `epa_per_play_d2` | Float64 | epa_d2 / plays_d2: EPA per play on second down. Null when the denominator is 0. |
| `success_rate_d2` | Float64 | successes_d2 / plays_d2: success rate on second down. Null when the denominator is 0. |
| `pass_rate_d3` | Float64 | passes_d3 / plays_d3: pass rate on third down. Null when the denominator is 0. |
| `epa_per_play_d3` | Float64 | epa_d3 / plays_d3: EPA per play on third down. Null when the denominator is 0. |
| `success_rate_d3` | Float64 | successes_d3 / plays_d3: success rate on third down. Null when the denominator is 0. |
| `pass_rate_d4` | Float64 | passes_d4 / plays_d4: pass rate on fourth down. Null when the denominator is 0. |
| `epa_per_play_d4` | Float64 | epa_d4 / plays_d4: EPA per play on fourth down. Null when the denominator is 0. |
| `success_rate_d4` | Float64 | successes_d4 / plays_d4: success rate on fourth down. Null when the denominator is 0. |
| `pass_rate_early_down` | Float64 | passes_early_down / plays_early_down. Null when the denominator is 0. |
| `epa_per_play_early_down` | Float64 | epa_early_down / plays_early_down. Null when the denominator is 0. |
| `success_rate_early_down` | Float64 | successes_early_down / plays_early_down: success rate on first or second down. Null when the denominator is 0. |
| `pass_rate_standard_down` | Float64 | passes_standard_down / plays_standard_down. Null when the denominator is 0. |
| `epa_per_play_standard_down` | Float64 | epa_standard_down / plays_standard_down: EPA per play on standard downs (the play-by-play standard_down flag). Null when the denominator is 0. |
| `success_rate_standard_down` | Float64 | successes_standard_down / plays_standard_down: success rate on standard downs (the play-by-play standard_down flag). Null when the denominator is 0. |
| `pass_rate_passing_down` | Float64 | passes_passing_down / plays_passing_down. Null when the denominator is 0. |
| `epa_per_play_passing_down` | Float64 | epa_passing_down / plays_passing_down: EPA per play on passing downs (the play-by-play passing_down flag). Null when the denominator is 0. |
| `success_rate_passing_down` | Float64 | successes_passing_down / plays_passing_down: success rate on passing downs (the play-by-play passing_down flag). Null when the denominator is 0. |
| `pass_rate_leading` | Float64 | passes_leading / plays_leading: pass rate snapped with the offense ahead on the scoreboard (pos_score_diff_start > 0, the score at the snap, before the play; pos_score_diff where the start score is missing). Null when the denominator is 0. |
| `epa_per_play_leading` | Float64 | epa_leading / plays_leading: EPA per play snapped with the offense ahead on the scoreboard (pos_score_diff_start > 0, the score at the snap, before the play; pos_score_diff where the start score is missing). Null when the denominator is 0. |
| `success_rate_leading` | Float64 | successes_leading / plays_leading: success rate snapped with the offense ahead on the scoreboard (pos_score_diff_start > 0, the score at the snap, before the play; pos_score_diff where the start score is missing). Null when the denominator is 0. |
| `pass_rate_tied` | Float64 | passes_tied / plays_tied: pass rate snapped with the score tied (pos_score_diff_start == 0, the score at the snap, before the play; pos_score_diff where the start score is missing). Null when the denominator is 0. |
| `epa_per_play_tied` | Float64 | epa_tied / plays_tied: EPA per play snapped with the score tied (pos_score_diff_start == 0, the score at the snap, before the play; pos_score_diff where the start score is missing). Null when the denominator is 0. |
| `success_rate_tied` | Float64 | successes_tied / plays_tied: success rate snapped with the score tied (pos_score_diff_start == 0, the score at the snap, before the play; pos_score_diff where the start score is missing). Null when the denominator is 0. |
| `pass_rate_trailing` | Float64 | passes_trailing / plays_trailing: pass rate snapped with the offense behind on the scoreboard (pos_score_diff_start < 0, the score at the snap, before the play; pos_score_diff where the start score is missing). Null when the denominator is 0. |
| `epa_per_play_trailing` | Float64 | epa_trailing / plays_trailing: EPA per play snapped with the offense behind on the scoreboard (pos_score_diff_start < 0, the score at the snap, before the play; pos_score_diff where the start score is missing). Null when the denominator is 0. |
| `success_rate_trailing` | Float64 | successes_trailing / plays_trailing: success rate snapped with the offense behind on the scoreboard (pos_score_diff_start < 0, the score at the snap, before the play; pos_score_diff where the start score is missing). Null when the denominator is 0. |
| `pass_rate_first_half` | Float64 | passes_first_half / plays_first_half. Null when the denominator is 0. |
| `epa_per_play_first_half` | Float64 | epa_first_half / plays_first_half: EPA per play in the first two quarters. Null when the denominator is 0. |
| `success_rate_first_half` | Float64 | successes_first_half / plays_first_half: success rate in the first two quarters. Null when the denominator is 0. |
| `pass_rate_second_half` | Float64 | passes_second_half / plays_second_half. Null when the denominator is 0. |
| `epa_per_play_second_half` | Float64 | epa_second_half / plays_second_half: EPA per play in the third and fourth quarters (overtime belongs to neither half). Null when the denominator is 0. |
| `success_rate_second_half` | Float64 | successes_second_half / plays_second_half: success rate in the third and fourth quarters (overtime belongs to neither half). Null when the denominator is 0. |
| `pass_rate_d3_short` | Float64 | passes_d3_short / plays_d3_short: pass rate on third down with 3 or fewer yards to go. Null when the denominator is 0. |
| `epa_per_play_d3_short` | Float64 | epa_d3_short / plays_d3_short: EPA per play on third down with 3 or fewer yards to go. Null when the denominator is 0. |
| `success_rate_d3_short` | Float64 | successes_d3_short / plays_d3_short: success rate on third down with 3 or fewer yards to go. Null when the denominator is 0. |
| `pass_rate_d3_medium` | Float64 | passes_d3_medium / plays_d3_medium: pass rate on third down with 4 to 6 yards to go. Null when the denominator is 0. |
| `epa_per_play_d3_medium` | Float64 | epa_d3_medium / plays_d3_medium: EPA per play on third down with 4 to 6 yards to go. Null when the denominator is 0. |
| `success_rate_d3_medium` | Float64 | successes_d3_medium / plays_d3_medium: success rate on third down with 4 to 6 yards to go. Null when the denominator is 0. |
| `pass_rate_d3_long` | Float64 | passes_d3_long / plays_d3_long: pass rate on third down with 7 or more yards to go. Null when the denominator is 0. |
| `epa_per_play_d3_long` | Float64 | epa_d3_long / plays_d3_long: EPA per play on third down with 7 or more yards to go. Null when the denominator is 0. |
| `success_rate_d3_long` | Float64 | successes_d3_long / plays_d3_long: success rate on third down with 7 or more yards to go. Null when the denominator is 0. |
| `pass_rate_red_zone` | Float64 | passes_red_zone / plays_red_zone: pass rate in the red zone (20 or fewer yards from the opponent end zone, read from yards to goal rather than the absolute yard line; the play-by-play rz_play flag only when that distance is missing). Null when the denominator is 0. |
| `epa_per_play_red_zone` | Float64 | epa_red_zone / plays_red_zone: EPA per play in the red zone (20 or fewer yards from the opponent end zone, read from yards to goal rather than the absolute yard line; the play-by-play rz_play flag only when that distance is missing). Null when the denominator is 0. |
| `success_rate_red_zone` | Float64 | successes_red_zone / plays_red_zone: success rate in the red zone (20 or fewer yards from the opponent end zone, read from yards to goal rather than the absolute yard line; the play-by-play rz_play flag only when that distance is missing). Null when the denominator is 0. |
| `pass_rate_own_half` | Float64 | passes_own_half / plays_own_half: pass rate snapped in the offense's own half (50 or more yards from the opponent end zone). Null when the denominator is 0. |
| `epa_per_play_own_half` | Float64 | epa_own_half / plays_own_half: EPA per play snapped in the offense's own half (50 or more yards from the opponent end zone). Null when the denominator is 0. |
| `success_rate_own_half` | Float64 | successes_own_half / plays_own_half: success rate snapped in the offense's own half (50 or more yards from the opponent end zone). Null when the denominator is 0. |
| `pass_rate_opp_half` | Float64 | passes_opp_half / plays_opp_half: pass rate snapped in the opponent's half (fewer than 50 yards from the opponent end zone). Null when the denominator is 0. |
| `epa_per_play_opp_half` | Float64 | epa_opp_half / plays_opp_half: EPA per play snapped in the opponent's half (fewer than 50 yards from the opponent end zone). Null when the denominator is 0. |
| `success_rate_opp_half` | Float64 | successes_opp_half / plays_opp_half: success rate snapped in the opponent's half (fewer than 50 yards from the opponent end zone). Null when the denominator is 0. |
| `pass_rate_one_score` | Float64 | passes_one_score / plays_one_score: pass rate snapped with the offense within 8 points either way (\|pos_score_diff_start\| <= 8, the score at the snap, before the play; pos_score_diff where the start score is missing). Null when the denominator is 0. |
| `epa_per_play_one_score` | Float64 | epa_one_score / plays_one_score: EPA per play snapped with the offense within 8 points either way (\|pos_score_diff_start\| <= 8, the score at the snap, before the play; pos_score_diff where the start score is missing). Null when the denominator is 0. |
| `success_rate_one_score` | Float64 | successes_one_score / plays_one_score: success rate snapped with the offense within 8 points either way (\|pos_score_diff_start\| <= 8, the score at the snap, before the play; pos_score_diff where the start score is missing). Null when the denominator is 0. |
| `pass_rate_home` | Float64 | passes_home / plays_home: pass rate in the team's home games. Null when the denominator is 0. |
| `epa_per_play_home` | Float64 | epa_home / plays_home: EPA per play in the team's home games. Null when the denominator is 0. |
| `success_rate_home` | Float64 | successes_home / plays_home: success rate in the team's home games. Null when the denominator is 0. |
| `win_rate_home` | Float64 | wins_home / games_home: win rate in the team's home games. Null when the denominator is 0. |
| `pass_rate_away` | Float64 | passes_away / plays_away: pass rate in the team's away games. Null when the denominator is 0. |
| `epa_per_play_away` | Float64 | epa_away / plays_away: EPA per play in the team's away games. Null when the denominator is 0. |
| `success_rate_away` | Float64 | successes_away / plays_away: success rate in the team's away games. Null when the denominator is 0. |
| `win_rate_away` | Float64 | wins_away / games_away: win rate in the team's away games. Null when the denominator is 0. |
| `pass_rate_neutral_site` | Float64 | passes_neutral_site / plays_neutral_site: pass rate in the team's neutral-site games. Null when the denominator is 0. |
| `epa_per_play_neutral_site` | Float64 | epa_neutral_site / plays_neutral_site: EPA per play in the team's neutral-site games. Null when the denominator is 0. |
| `success_rate_neutral_site` | Float64 | successes_neutral_site / plays_neutral_site: success rate in the team's neutral-site games. Null when the denominator is 0. |
| `win_rate_neutral_site` | Float64 | wins_neutral_site / games_neutral_site: win rate in the team's neutral-site games. Null when the denominator is 0. |
| `pass_rate_vs_ranked` | Float64 | passes_vs_ranked / plays_vs_ranked: pass rate in the team's games against an opponent ranked at kickoff (ESPN's displayed rank). Null when the denominator is 0. |
| `epa_per_play_vs_ranked` | Float64 | epa_vs_ranked / plays_vs_ranked: EPA per play in the team's games against an opponent ranked at kickoff (ESPN's displayed rank). Null when the denominator is 0. |
| `success_rate_vs_ranked` | Float64 | successes_vs_ranked / plays_vs_ranked: success rate in the team's games against an opponent ranked at kickoff (ESPN's displayed rank). Null when the denominator is 0. |
| `win_rate_vs_ranked` | Float64 | wins_vs_ranked / games_vs_ranked: win rate in the team's games against an opponent ranked at kickoff (ESPN's displayed rank). Null when the denominator is 0. |
| `pass_rate_after_bye` | Float64 | passes_after_bye / plays_after_bye: pass rate in the team's games played 13 or more days after the team's previous game. Null when the denominator is 0. |
| `epa_per_play_after_bye` | Float64 | epa_after_bye / plays_after_bye: EPA per play in the team's games played 13 or more days after the team's previous game. Null when the denominator is 0. |
| `success_rate_after_bye` | Float64 | successes_after_bye / plays_after_bye: success rate in the team's games played 13 or more days after the team's previous game. Null when the denominator is 0. |
| `win_rate_after_bye` | Float64 | wins_after_bye / games_after_bye: win rate in the team's games played 13 or more days after the team's previous game. Null when the denominator is 0. |
| `pass_rate_opener` | Float64 | passes_opener / plays_opener: pass rate in the team's regular-season openers. Null when the denominator is 0. |
| `epa_per_play_opener` | Float64 | epa_opener / plays_opener: EPA per play in the team's regular-season openers. Null when the denominator is 0. |
| `success_rate_opener` | Float64 | successes_opener / plays_opener: success rate in the team's regular-season openers. Null when the denominator is 0. |
| `win_rate_opener` | Float64 | wins_opener / games_opener: win rate in the team's regular-season openers. Null when the denominator is 0. |
| `pass_rate_one_score_game` | Float64 | passes_one_score_game / plays_one_score_game: pass rate in the team's games decided by 8 points or fewer. Null when the denominator is 0. |
| `epa_per_play_one_score_game` | Float64 | epa_one_score_game / plays_one_score_game: EPA per play in the team's games decided by 8 points or fewer. Null when the denominator is 0. |
| `success_rate_one_score_game` | Float64 | successes_one_score_game / plays_one_score_game: success rate in the team's games decided by 8 points or fewer. Null when the denominator is 0. |
| `win_rate_one_score_game` | Float64 | wins_one_score_game / games_one_score_game: win rate in the team's games decided by 8 points or fewer. Null when the denominator is 0. |
| `ypp` | Float64 | yards / plays: yards per play. Null when the denominator is 0. |
| `ypp_rush` | Float64 | yards_rush / rushes: yards per rush. Null when the denominator is 0. |
| `ypp_pass` | Float64 | yards_pass / passes: yards per pass play. Null when the denominator is 0. |
| `explosive_rate` | Float64 | explosives / plays. Null when the denominator is 0. |
| `explosive_rate_rush` | Float64 | explosives_rush / rushes. Null when the denominator is 0. |
| `explosive_rate_pass` | Float64 | explosives_pass / passes. Null when the denominator is 0. |
| `third_down_rate` | Float64 | third_down_conversions / third_down_opportunities. Null when the denominator is 0. |
| `rz_trip_rate` | Float64 | rz_trips / drives: the share of drives that reached the red zone. Null when the denominator is 0. |
| `rz_td_rate` | Float64 | rz_tds / rz_trips: touchdowns per red-zone trip. Null when the denominator is 0. |
| `rz_conversion_rate` | Float64 | rz_scores / rz_trips: the share of red-zone trips that scored (touchdown or field goal). Null when the denominator is 0. |
| `rz_pts_per_trip` | Float64 | rz_points / rz_trips: points per red-zone trip. Null when the denominator is 0. |
| `so_trip_rate` | Float64 | so_trips / drives: the share of drives that reached the opponent's 40. Null when the denominator is 0. |
| `so_td_rate` | Float64 | so_tds / so_trips: touchdowns per scoring-opportunity trip. Null when the denominator is 0. |
| `so_conversion_rate` | Float64 | so_scores / so_trips: the share of scoring-opportunity trips that scored. Null when the denominator is 0. |
| `so_pts_per_trip` | Float64 | so_points / so_trips: points per scoring-opportunity trip. Null when the denominator is 0. |
| `pts_per_drive` | Float64 | drive_points / drives: points per drive. Null when the denominator is 0. |
| `scripted_epa_per_play` | Float64 | scripted_epa / scripted_plays. Null when the denominator is 0. |
| `scripted_success_rate` | Float64 | scripted_successes / scripted_plays. Null when the denominator is 0. |
| `scripted_pts_per_drive` | Float64 | scripted_points / scripted_drives. Null when the denominator is 0. |
| `non_scripted_epa_per_play` | Float64 | non_scripted_epa / non_scripted_plays. Null when the denominator is 0. |
| `non_scripted_success_rate` | Float64 | non_scripted_successes / non_scripted_plays. Null when the denominator is 0. |
| `non_scripted_pts_per_drive` | Float64 | non_scripted_points / non_scripted_drives. Null when the denominator is 0. |
| `go_rate` | Float64 | fourth_went / fourth_decisions: the share of fourth-down decisions on which the offense went for it. Null when the denominator is 0. |
| `go_rate_when_model_says_go` | Float64 | fourth_went_when_go / fourth_model_go: go rate on the decisions where the fourth-down model said go. Null when the denominator is 0. |
| `go_rate_when_model_says_kick` | Float64 | fourth_went_when_kick / fourth_model_kick: go rate on the decisions where the model said punt or kick. Null when the denominator is 0. |
| `fourth_agreement_rate` | Float64 | fourth_agreed / fourth_decisions: the share of decisions that matched the model. Null when the denominator is 0. |
| `fourth_wp_left_per_decision` | Float64 | fourth_wp_left / fourth_decisions: win probability left on the table per fourth-down decision. Null when the denominator is 0. |
| `fourth_conversion_rate` | Float64 | fourth_converted / fourth_went: conversion rate when going for it. Null when the denominator is 0. |
| `third_down_over_expected` | Float64 | third_down_conversions minus third_down_expected: conversions above the distance-adjusted expectation; null when no curve was available. |
| `def_plays_per_game` | Float64 | Defense-allowed twin of plays_per_game: plays / games. Computed from the def_ counts; null when the denominator is 0. |
| `def_plays_per_drive` | Float64 | Defense-allowed twin of plays_per_drive: plays / drives. Computed from the def_ counts; null when the denominator is 0. |
| `def_drives_per_game` | Float64 | Defense-allowed twin of drives_per_game: drives / games. Computed from the def_ counts; null when the denominator is 0. |
| `def_sec_per_play` | Float64 | Defense-allowed twin of sec_per_play: drive_seconds / drive_plays: seconds of game clock per offensive play from ESPN's own drive clock (pace; lower is faster). Computed from the def_ counts; null when the denominator is 0. |
| `def_sec_per_play_neutral` | Float64 | Defense-allowed twin of sec_per_play_neutral: drive_seconds_neutral / drive_plays_neutral: the same pace measure on drives that started situation-neutral. Computed from the def_ counts; null when the denominator is 0. |
| `def_pace_coverage` | Float64 | Defense-allowed twin of pace_coverage: drives_with_clock / drives: the share of drives with a usable clock; treat sec_per_play with caution when this is low. Computed from the def_ counts; null when the denominator is 0. |
| `def_pass_rate` | Float64 | Defense-allowed twin of pass_rate: passes / plays. Computed from the def_ counts; null when the denominator is 0. |
| `def_epa_per_play` | Float64 | Defense-allowed twin of epa_per_play: epa / plays. Computed from the def_ counts; null when the denominator is 0. |
| `def_epa_per_rush` | Float64 | Defense-allowed twin of epa_per_rush: epa_rush / rushes. Computed from the def_ counts; null when the denominator is 0. |
| `def_epa_per_pass` | Float64 | Defense-allowed twin of epa_per_pass: epa_pass / passes. Computed from the def_ counts; null when the denominator is 0. |
| `def_success_rate` | Float64 | Defense-allowed twin of success_rate: successes / plays. Computed from the def_ counts; null when the denominator is 0. |
| `def_success_rate_rush` | Float64 | Defense-allowed twin of success_rate_rush: successes_rush / rushes. Computed from the def_ counts; null when the denominator is 0. |
| `def_success_rate_pass` | Float64 | Defense-allowed twin of success_rate_pass: successes_pass / passes. Computed from the def_ counts; null when the denominator is 0. |
| `def_pass_rate_neutral` | Float64 | Defense-allowed twin of pass_rate_neutral: passes_neutral / plays_neutral: pass rate in situation-neutral situations. Computed from the def_ counts; null when the denominator is 0. |
| `def_epa_per_play_neutral` | Float64 | Defense-allowed twin of epa_per_play_neutral: epa_neutral / plays_neutral. Computed from the def_ counts; null when the denominator is 0. |
| `def_success_rate_neutral` | Float64 | Defense-allowed twin of success_rate_neutral: successes_neutral / plays_neutral: success rate in situation-neutral situations (score-and-clock win probability (wp_before_naive, no pregame line) between 20% and 80%, in the first four quarters, outside the final two minutes of a half). Computed from the def_ counts; null when the denominator is 0. |
| `def_pass_rate_d1` | Float64 | Defense-allowed twin of pass_rate_d1: passes_d1 / plays_d1: pass rate on first down. Computed from the def_ counts; null when the denominator is 0. |
| `def_epa_per_play_d1` | Float64 | Defense-allowed twin of epa_per_play_d1: epa_d1 / plays_d1: EPA per play on first down. Computed from the def_ counts; null when the denominator is 0. |
| `def_success_rate_d1` | Float64 | Defense-allowed twin of success_rate_d1: successes_d1 / plays_d1: success rate on first down. Computed from the def_ counts; null when the denominator is 0. |
| `def_pass_rate_d2` | Float64 | Defense-allowed twin of pass_rate_d2: passes_d2 / plays_d2: pass rate on second down. Computed from the def_ counts; null when the denominator is 0. |
| `def_epa_per_play_d2` | Float64 | Defense-allowed twin of epa_per_play_d2: epa_d2 / plays_d2: EPA per play on second down. Computed from the def_ counts; null when the denominator is 0. |
| `def_success_rate_d2` | Float64 | Defense-allowed twin of success_rate_d2: successes_d2 / plays_d2: success rate on second down. Computed from the def_ counts; null when the denominator is 0. |
| `def_pass_rate_d3` | Float64 | Defense-allowed twin of pass_rate_d3: passes_d3 / plays_d3: pass rate on third down. Computed from the def_ counts; null when the denominator is 0. |
| `def_epa_per_play_d3` | Float64 | Defense-allowed twin of epa_per_play_d3: epa_d3 / plays_d3: EPA per play on third down. Computed from the def_ counts; null when the denominator is 0. |
| `def_success_rate_d3` | Float64 | Defense-allowed twin of success_rate_d3: successes_d3 / plays_d3: success rate on third down. Computed from the def_ counts; null when the denominator is 0. |
| `def_pass_rate_d4` | Float64 | Defense-allowed twin of pass_rate_d4: passes_d4 / plays_d4: pass rate on fourth down. Computed from the def_ counts; null when the denominator is 0. |
| `def_epa_per_play_d4` | Float64 | Defense-allowed twin of epa_per_play_d4: epa_d4 / plays_d4: EPA per play on fourth down. Computed from the def_ counts; null when the denominator is 0. |
| `def_success_rate_d4` | Float64 | Defense-allowed twin of success_rate_d4: successes_d4 / plays_d4: success rate on fourth down. Computed from the def_ counts; null when the denominator is 0. |
| `def_pass_rate_early_down` | Float64 | Defense-allowed twin of pass_rate_early_down: passes_early_down / plays_early_down. Computed from the def_ counts; null when the denominator is 0. |
| `def_epa_per_play_early_down` | Float64 | Defense-allowed twin of epa_per_play_early_down: epa_early_down / plays_early_down. Computed from the def_ counts; null when the denominator is 0. |
| `def_success_rate_early_down` | Float64 | Defense-allowed twin of success_rate_early_down: successes_early_down / plays_early_down: success rate on first or second down. Computed from the def_ counts; null when the denominator is 0. |
| `def_pass_rate_standard_down` | Float64 | Defense-allowed twin of pass_rate_standard_down: passes_standard_down / plays_standard_down. Computed from the def_ counts; null when the denominator is 0. |
| `def_epa_per_play_standard_down` | Float64 | Defense-allowed twin of epa_per_play_standard_down: epa_standard_down / plays_standard_down: EPA per play on standard downs (the play-by-play standard_down flag). Computed from the def_ counts; null when the denominator is 0. |
| `def_success_rate_standard_down` | Float64 | Defense-allowed twin of success_rate_standard_down: successes_standard_down / plays_standard_down: success rate on standard downs (the play-by-play standard_down flag). Computed from the def_ counts; null when the denominator is 0. |
| `def_pass_rate_passing_down` | Float64 | Defense-allowed twin of pass_rate_passing_down: passes_passing_down / plays_passing_down. Computed from the def_ counts; null when the denominator is 0. |
| `def_epa_per_play_passing_down` | Float64 | Defense-allowed twin of epa_per_play_passing_down: epa_passing_down / plays_passing_down: EPA per play on passing downs (the play-by-play passing_down flag). Computed from the def_ counts; null when the denominator is 0. |
| `def_success_rate_passing_down` | Float64 | Defense-allowed twin of success_rate_passing_down: successes_passing_down / plays_passing_down: success rate on passing downs (the play-by-play passing_down flag). Computed from the def_ counts; null when the denominator is 0. |
| `def_pass_rate_leading` | Float64 | Defense-allowed twin of pass_rate_leading: passes_leading / plays_leading: pass rate snapped with the offense ahead on the scoreboard (pos_score_diff_start > 0, the score at the snap, before the play; pos_score_diff where the start score is missing). Computed from the def_ counts; null when the denominator is 0. |
| `def_epa_per_play_leading` | Float64 | Defense-allowed twin of epa_per_play_leading: epa_leading / plays_leading: EPA per play snapped with the offense ahead on the scoreboard (pos_score_diff_start > 0, the score at the snap, before the play; pos_score_diff where the start score is missing). Computed from the def_ counts; null when the denominator is 0. |
| `def_success_rate_leading` | Float64 | Defense-allowed twin of success_rate_leading: successes_leading / plays_leading: success rate snapped with the offense ahead on the scoreboard (pos_score_diff_start > 0, the score at the snap, before the play; pos_score_diff where the start score is missing). Computed from the def_ counts; null when the denominator is 0. |
| `def_pass_rate_tied` | Float64 | Defense-allowed twin of pass_rate_tied: passes_tied / plays_tied: pass rate snapped with the score tied (pos_score_diff_start == 0, the score at the snap, before the play; pos_score_diff where the start score is missing). Computed from the def_ counts; null when the denominator is 0. |
| `def_epa_per_play_tied` | Float64 | Defense-allowed twin of epa_per_play_tied: epa_tied / plays_tied: EPA per play snapped with the score tied (pos_score_diff_start == 0, the score at the snap, before the play; pos_score_diff where the start score is missing). Computed from the def_ counts; null when the denominator is 0. |
| `def_success_rate_tied` | Float64 | Defense-allowed twin of success_rate_tied: successes_tied / plays_tied: success rate snapped with the score tied (pos_score_diff_start == 0, the score at the snap, before the play; pos_score_diff where the start score is missing). Computed from the def_ counts; null when the denominator is 0. |
| `def_pass_rate_trailing` | Float64 | Defense-allowed twin of pass_rate_trailing: passes_trailing / plays_trailing: pass rate snapped with the offense behind on the scoreboard (pos_score_diff_start < 0, the score at the snap, before the play; pos_score_diff where the start score is missing). Computed from the def_ counts; null when the denominator is 0. |
| `def_epa_per_play_trailing` | Float64 | Defense-allowed twin of epa_per_play_trailing: epa_trailing / plays_trailing: EPA per play snapped with the offense behind on the scoreboard (pos_score_diff_start < 0, the score at the snap, before the play; pos_score_diff where the start score is missing). Computed from the def_ counts; null when the denominator is 0. |
| `def_success_rate_trailing` | Float64 | Defense-allowed twin of success_rate_trailing: successes_trailing / plays_trailing: success rate snapped with the offense behind on the scoreboard (pos_score_diff_start < 0, the score at the snap, before the play; pos_score_diff where the start score is missing). Computed from the def_ counts; null when the denominator is 0. |
| `def_pass_rate_first_half` | Float64 | Defense-allowed twin of pass_rate_first_half: passes_first_half / plays_first_half. Computed from the def_ counts; null when the denominator is 0. |
| `def_epa_per_play_first_half` | Float64 | Defense-allowed twin of epa_per_play_first_half: epa_first_half / plays_first_half: EPA per play in the first two quarters. Computed from the def_ counts; null when the denominator is 0. |
| `def_success_rate_first_half` | Float64 | Defense-allowed twin of success_rate_first_half: successes_first_half / plays_first_half: success rate in the first two quarters. Computed from the def_ counts; null when the denominator is 0. |
| `def_pass_rate_second_half` | Float64 | Defense-allowed twin of pass_rate_second_half: passes_second_half / plays_second_half. Computed from the def_ counts; null when the denominator is 0. |
| `def_epa_per_play_second_half` | Float64 | Defense-allowed twin of epa_per_play_second_half: epa_second_half / plays_second_half: EPA per play in the third and fourth quarters (overtime belongs to neither half). Computed from the def_ counts; null when the denominator is 0. |
| `def_success_rate_second_half` | Float64 | Defense-allowed twin of success_rate_second_half: successes_second_half / plays_second_half: success rate in the third and fourth quarters (overtime belongs to neither half). Computed from the def_ counts; null when the denominator is 0. |
| `def_pass_rate_d3_short` | Float64 | Defense-allowed twin of pass_rate_d3_short: passes_d3_short / plays_d3_short: pass rate on third down with 3 or fewer yards to go. Computed from the def_ counts; null when the denominator is 0. |
| `def_epa_per_play_d3_short` | Float64 | Defense-allowed twin of epa_per_play_d3_short: epa_d3_short / plays_d3_short: EPA per play on third down with 3 or fewer yards to go. Computed from the def_ counts; null when the denominator is 0. |
| `def_success_rate_d3_short` | Float64 | Defense-allowed twin of success_rate_d3_short: successes_d3_short / plays_d3_short: success rate on third down with 3 or fewer yards to go. Computed from the def_ counts; null when the denominator is 0. |
| `def_pass_rate_d3_medium` | Float64 | Defense-allowed twin of pass_rate_d3_medium: passes_d3_medium / plays_d3_medium: pass rate on third down with 4 to 6 yards to go. Computed from the def_ counts; null when the denominator is 0. |
| `def_epa_per_play_d3_medium` | Float64 | Defense-allowed twin of epa_per_play_d3_medium: epa_d3_medium / plays_d3_medium: EPA per play on third down with 4 to 6 yards to go. Computed from the def_ counts; null when the denominator is 0. |
| `def_success_rate_d3_medium` | Float64 | Defense-allowed twin of success_rate_d3_medium: successes_d3_medium / plays_d3_medium: success rate on third down with 4 to 6 yards to go. Computed from the def_ counts; null when the denominator is 0. |
| `def_pass_rate_d3_long` | Float64 | Defense-allowed twin of pass_rate_d3_long: passes_d3_long / plays_d3_long: pass rate on third down with 7 or more yards to go. Computed from the def_ counts; null when the denominator is 0. |
| `def_epa_per_play_d3_long` | Float64 | Defense-allowed twin of epa_per_play_d3_long: epa_d3_long / plays_d3_long: EPA per play on third down with 7 or more yards to go. Computed from the def_ counts; null when the denominator is 0. |
| `def_success_rate_d3_long` | Float64 | Defense-allowed twin of success_rate_d3_long: successes_d3_long / plays_d3_long: success rate on third down with 7 or more yards to go. Computed from the def_ counts; null when the denominator is 0. |
| `def_pass_rate_red_zone` | Float64 | Defense-allowed twin of pass_rate_red_zone: passes_red_zone / plays_red_zone: pass rate in the red zone (20 or fewer yards from the opponent end zone, read from yards to goal rather than the absolute yard line; the play-by-play rz_play flag only when that distance is missing). Computed from the def_ counts; null when the denominator is 0. |
| `def_epa_per_play_red_zone` | Float64 | Defense-allowed twin of epa_per_play_red_zone: epa_red_zone / plays_red_zone: EPA per play in the red zone (20 or fewer yards from the opponent end zone, read from yards to goal rather than the absolute yard line; the play-by-play rz_play flag only when that distance is missing). Computed from the def_ counts; null when the denominator is 0. |
| `def_success_rate_red_zone` | Float64 | Defense-allowed twin of success_rate_red_zone: successes_red_zone / plays_red_zone: success rate in the red zone (20 or fewer yards from the opponent end zone, read from yards to goal rather than the absolute yard line; the play-by-play rz_play flag only when that distance is missing). Computed from the def_ counts; null when the denominator is 0. |
| `def_pass_rate_own_half` | Float64 | Defense-allowed twin of pass_rate_own_half: passes_own_half / plays_own_half: pass rate snapped in the offense's own half (50 or more yards from the opponent end zone). Computed from the def_ counts; null when the denominator is 0. |
| `def_epa_per_play_own_half` | Float64 | Defense-allowed twin of epa_per_play_own_half: epa_own_half / plays_own_half: EPA per play snapped in the offense's own half (50 or more yards from the opponent end zone). Computed from the def_ counts; null when the denominator is 0. |
| `def_success_rate_own_half` | Float64 | Defense-allowed twin of success_rate_own_half: successes_own_half / plays_own_half: success rate snapped in the offense's own half (50 or more yards from the opponent end zone). Computed from the def_ counts; null when the denominator is 0. |
| `def_pass_rate_opp_half` | Float64 | Defense-allowed twin of pass_rate_opp_half: passes_opp_half / plays_opp_half: pass rate snapped in the opponent's half (fewer than 50 yards from the opponent end zone). Computed from the def_ counts; null when the denominator is 0. |
| `def_epa_per_play_opp_half` | Float64 | Defense-allowed twin of epa_per_play_opp_half: epa_opp_half / plays_opp_half: EPA per play snapped in the opponent's half (fewer than 50 yards from the opponent end zone). Computed from the def_ counts; null when the denominator is 0. |
| `def_success_rate_opp_half` | Float64 | Defense-allowed twin of success_rate_opp_half: successes_opp_half / plays_opp_half: success rate snapped in the opponent's half (fewer than 50 yards from the opponent end zone). Computed from the def_ counts; null when the denominator is 0. |
| `def_pass_rate_one_score` | Float64 | Defense-allowed twin of pass_rate_one_score: passes_one_score / plays_one_score: pass rate snapped with the offense within 8 points either way (\|pos_score_diff_start\| <= 8, the score at the snap, before the play; pos_score_diff where the start score is missing). Computed from the def_ counts; null when the denominator is 0. |
| `def_epa_per_play_one_score` | Float64 | Defense-allowed twin of epa_per_play_one_score: epa_one_score / plays_one_score: EPA per play snapped with the offense within 8 points either way (\|pos_score_diff_start\| <= 8, the score at the snap, before the play; pos_score_diff where the start score is missing). Computed from the def_ counts; null when the denominator is 0. |
| `def_success_rate_one_score` | Float64 | Defense-allowed twin of success_rate_one_score: successes_one_score / plays_one_score: success rate snapped with the offense within 8 points either way (\|pos_score_diff_start\| <= 8, the score at the snap, before the play; pos_score_diff where the start score is missing). Computed from the def_ counts; null when the denominator is 0. |
| `def_pass_rate_home` | Float64 | Defense-allowed twin of pass_rate_home: passes_home / plays_home: pass rate in the team's home games. Computed from the def_ counts; null when the denominator is 0. |
| `def_epa_per_play_home` | Float64 | Defense-allowed twin of epa_per_play_home: epa_home / plays_home: EPA per play in the team's home games. Computed from the def_ counts; null when the denominator is 0. |
| `def_success_rate_home` | Float64 | Defense-allowed twin of success_rate_home: successes_home / plays_home: success rate in the team's home games. Computed from the def_ counts; null when the denominator is 0. |
| `def_win_rate_home` | Float64 | Defense-allowed twin of win_rate_home: wins_home / games_home: win rate in the team's home games. Computed from the def_ counts; null when the denominator is 0. |
| `def_pass_rate_away` | Float64 | Defense-allowed twin of pass_rate_away: passes_away / plays_away: pass rate in the team's away games. Computed from the def_ counts; null when the denominator is 0. |
| `def_epa_per_play_away` | Float64 | Defense-allowed twin of epa_per_play_away: epa_away / plays_away: EPA per play in the team's away games. Computed from the def_ counts; null when the denominator is 0. |
| `def_success_rate_away` | Float64 | Defense-allowed twin of success_rate_away: successes_away / plays_away: success rate in the team's away games. Computed from the def_ counts; null when the denominator is 0. |
| `def_win_rate_away` | Float64 | Defense-allowed twin of win_rate_away: wins_away / games_away: win rate in the team's away games. Computed from the def_ counts; null when the denominator is 0. |
| `def_pass_rate_neutral_site` | Float64 | Defense-allowed twin of pass_rate_neutral_site: passes_neutral_site / plays_neutral_site: pass rate in the team's neutral-site games. Computed from the def_ counts; null when the denominator is 0. |
| `def_epa_per_play_neutral_site` | Float64 | Defense-allowed twin of epa_per_play_neutral_site: epa_neutral_site / plays_neutral_site: EPA per play in the team's neutral-site games. Computed from the def_ counts; null when the denominator is 0. |
| `def_success_rate_neutral_site` | Float64 | Defense-allowed twin of success_rate_neutral_site: successes_neutral_site / plays_neutral_site: success rate in the team's neutral-site games. Computed from the def_ counts; null when the denominator is 0. |
| `def_win_rate_neutral_site` | Float64 | Defense-allowed twin of win_rate_neutral_site: wins_neutral_site / games_neutral_site: win rate in the team's neutral-site games. Computed from the def_ counts; null when the denominator is 0. |
| `def_pass_rate_vs_ranked` | Float64 | Defense-allowed twin of pass_rate_vs_ranked: passes_vs_ranked / plays_vs_ranked: pass rate in the team's games against an opponent ranked at kickoff (ESPN's displayed rank). Computed from the def_ counts; null when the denominator is 0. |
| `def_epa_per_play_vs_ranked` | Float64 | Defense-allowed twin of epa_per_play_vs_ranked: epa_vs_ranked / plays_vs_ranked: EPA per play in the team's games against an opponent ranked at kickoff (ESPN's displayed rank). Computed from the def_ counts; null when the denominator is 0. |
| `def_success_rate_vs_ranked` | Float64 | Defense-allowed twin of success_rate_vs_ranked: successes_vs_ranked / plays_vs_ranked: success rate in the team's games against an opponent ranked at kickoff (ESPN's displayed rank). Computed from the def_ counts; null when the denominator is 0. |
| `def_win_rate_vs_ranked` | Float64 | Defense-allowed twin of win_rate_vs_ranked: wins_vs_ranked / games_vs_ranked: win rate in the team's games against an opponent ranked at kickoff (ESPN's displayed rank). Computed from the def_ counts; null when the denominator is 0. |
| `def_pass_rate_after_bye` | Float64 | Defense-allowed twin of pass_rate_after_bye: passes_after_bye / plays_after_bye: pass rate in the team's games played 13 or more days after the team's previous game. Computed from the def_ counts; null when the denominator is 0. |
| `def_epa_per_play_after_bye` | Float64 | Defense-allowed twin of epa_per_play_after_bye: epa_after_bye / plays_after_bye: EPA per play in the team's games played 13 or more days after the team's previous game. Computed from the def_ counts; null when the denominator is 0. |
| `def_success_rate_after_bye` | Float64 | Defense-allowed twin of success_rate_after_bye: successes_after_bye / plays_after_bye: success rate in the team's games played 13 or more days after the team's previous game. Computed from the def_ counts; null when the denominator is 0. |
| `def_win_rate_after_bye` | Float64 | Defense-allowed twin of win_rate_after_bye: wins_after_bye / games_after_bye: win rate in the team's games played 13 or more days after the team's previous game. Computed from the def_ counts; null when the denominator is 0. |
| `def_pass_rate_opener` | Float64 | Defense-allowed twin of pass_rate_opener: passes_opener / plays_opener: pass rate in the team's regular-season openers. Computed from the def_ counts; null when the denominator is 0. |
| `def_epa_per_play_opener` | Float64 | Defense-allowed twin of epa_per_play_opener: epa_opener / plays_opener: EPA per play in the team's regular-season openers. Computed from the def_ counts; null when the denominator is 0. |
| `def_success_rate_opener` | Float64 | Defense-allowed twin of success_rate_opener: successes_opener / plays_opener: success rate in the team's regular-season openers. Computed from the def_ counts; null when the denominator is 0. |
| `def_win_rate_opener` | Float64 | Defense-allowed twin of win_rate_opener: wins_opener / games_opener: win rate in the team's regular-season openers. Computed from the def_ counts; null when the denominator is 0. |
| `def_pass_rate_one_score_game` | Float64 | Defense-allowed twin of pass_rate_one_score_game: passes_one_score_game / plays_one_score_game: pass rate in the team's games decided by 8 points or fewer. Computed from the def_ counts; null when the denominator is 0. |
| `def_epa_per_play_one_score_game` | Float64 | Defense-allowed twin of epa_per_play_one_score_game: epa_one_score_game / plays_one_score_game: EPA per play in the team's games decided by 8 points or fewer. Computed from the def_ counts; null when the denominator is 0. |
| `def_success_rate_one_score_game` | Float64 | Defense-allowed twin of success_rate_one_score_game: successes_one_score_game / plays_one_score_game: success rate in the team's games decided by 8 points or fewer. Computed from the def_ counts; null when the denominator is 0. |
| `def_win_rate_one_score_game` | Float64 | Defense-allowed twin of win_rate_one_score_game: wins_one_score_game / games_one_score_game: win rate in the team's games decided by 8 points or fewer. Computed from the def_ counts; null when the denominator is 0. |
| `def_ypp` | Float64 | Defense-allowed twin of ypp: yards / plays: yards per play. Computed from the def_ counts; null when the denominator is 0. |
| `def_ypp_rush` | Float64 | Defense-allowed twin of ypp_rush: yards_rush / rushes: yards per rush. Computed from the def_ counts; null when the denominator is 0. |
| `def_ypp_pass` | Float64 | Defense-allowed twin of ypp_pass: yards_pass / passes: yards per pass play. Computed from the def_ counts; null when the denominator is 0. |
| `def_explosive_rate` | Float64 | Defense-allowed twin of explosive_rate: explosives / plays. Computed from the def_ counts; null when the denominator is 0. |
| `def_explosive_rate_rush` | Float64 | Defense-allowed twin of explosive_rate_rush: explosives_rush / rushes. Computed from the def_ counts; null when the denominator is 0. |
| `def_explosive_rate_pass` | Float64 | Defense-allowed twin of explosive_rate_pass: explosives_pass / passes. Computed from the def_ counts; null when the denominator is 0. |
| `def_third_down_rate` | Float64 | Defense-allowed twin of third_down_rate: third_down_conversions / third_down_opportunities. Computed from the def_ counts; null when the denominator is 0. |
| `def_rz_trip_rate` | Float64 | Defense-allowed twin of rz_trip_rate: rz_trips / drives: the share of drives that reached the red zone. Computed from the def_ counts; null when the denominator is 0. |
| `def_rz_td_rate` | Float64 | Defense-allowed twin of rz_td_rate: rz_tds / rz_trips: touchdowns per red-zone trip. Computed from the def_ counts; null when the denominator is 0. |
| `def_rz_conversion_rate` | Float64 | Defense-allowed twin of rz_conversion_rate: rz_scores / rz_trips: the share of red-zone trips that scored (touchdown or field goal). Computed from the def_ counts; null when the denominator is 0. |
| `def_rz_pts_per_trip` | Float64 | Defense-allowed twin of rz_pts_per_trip: rz_points / rz_trips: points per red-zone trip. Computed from the def_ counts; null when the denominator is 0. |
| `def_so_trip_rate` | Float64 | Defense-allowed twin of so_trip_rate: so_trips / drives: the share of drives that reached the opponent's 40. Computed from the def_ counts; null when the denominator is 0. |
| `def_so_td_rate` | Float64 | Defense-allowed twin of so_td_rate: so_tds / so_trips: touchdowns per scoring-opportunity trip. Computed from the def_ counts; null when the denominator is 0. |
| `def_so_conversion_rate` | Float64 | Defense-allowed twin of so_conversion_rate: so_scores / so_trips: the share of scoring-opportunity trips that scored. Computed from the def_ counts; null when the denominator is 0. |
| `def_so_pts_per_trip` | Float64 | Defense-allowed twin of so_pts_per_trip: so_points / so_trips: points per scoring-opportunity trip. Computed from the def_ counts; null when the denominator is 0. |
| `def_pts_per_drive` | Float64 | Defense-allowed twin of pts_per_drive: drive_points / drives: points per drive. Computed from the def_ counts; null when the denominator is 0. |
| `def_scripted_epa_per_play` | Float64 | Defense-allowed twin of scripted_epa_per_play: scripted_epa / scripted_plays. Computed from the def_ counts; null when the denominator is 0. |
| `def_scripted_success_rate` | Float64 | Defense-allowed twin of scripted_success_rate: scripted_successes / scripted_plays. Computed from the def_ counts; null when the denominator is 0. |
| `def_scripted_pts_per_drive` | Float64 | Defense-allowed twin of scripted_pts_per_drive: scripted_points / scripted_drives. Computed from the def_ counts; null when the denominator is 0. |
| `def_non_scripted_epa_per_play` | Float64 | Defense-allowed twin of non_scripted_epa_per_play: non_scripted_epa / non_scripted_plays. Computed from the def_ counts; null when the denominator is 0. |
| `def_non_scripted_success_rate` | Float64 | Defense-allowed twin of non_scripted_success_rate: non_scripted_successes / non_scripted_plays. Computed from the def_ counts; null when the denominator is 0. |
| `def_non_scripted_pts_per_drive` | Float64 | Defense-allowed twin of non_scripted_pts_per_drive: non_scripted_points / non_scripted_drives. Computed from the def_ counts; null when the denominator is 0. |
| `def_third_down_over_expected` | Float64 | Defense-allowed twin of third_down_over_expected: def_third_down_conversions minus def_third_down_expected; null when no curve was available. |

```python
load_cfb_coach_careers()
```
