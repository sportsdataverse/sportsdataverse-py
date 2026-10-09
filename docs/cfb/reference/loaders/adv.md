# CFB dataset loaders — Advanced stats

> CFB dataset loaders — Advanced stats — function reference in sdv-py, the SportsDataverse Python package.

## load_cfb_adv_team

Release: [espn_cfb_adv_team](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_adv_team) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_adv_team/adv_team_{season}.parquet`
### Returns {#load_cfb_adv_team-returns}

| col_name | type | description |
|---|---|---|
| `pos_team_id` | Int64 | ESPN team id of the team on offense. Present for every season 2004+. |
| `pos_team` | String | Team name in possession at the start of the play (offense, kickoff-aware). |
| `rushing_highlight_yards_per_opp` | Float64 | Highlight yards per rushing opportunity. |
| `total_pen_yards` | Int64 | Total penalty yards assessed. |
| `EPA_penalty` | Float64 | Total EPA attributed to penalties. |
| `penalty_first_downs_created` | Int64 | Number of first downs the team gained via opponent penalty. |
| `penalty_first_downs_created_rate` | Float64 | Share of the team's first downs that came via opponent penalty. |
| `penalties` | Int64 | Number of penalties assessed against the team. |
| `penalty_yards` | Int64 | Net penalty yardage assessed against the team; can be negative when enforcement moved the team forward on balance. |
| `special_teams_plays` | Int64 | Number of special-teams plays. |
| `EPA_sp` | Float64 | Total special-teams EPA, ESPN's abbreviated field for the same phase. |
| `EPA_special_teams` | Float64 | Total EPA generated on special-teams plays. |
| `field_goals` | Int64 | Number of field-goal attempts. |
| `EPA_fg` | Float64 | Total EPA on field-goal attempts. |
| `punt_plays` | Int64 | Number of punt plays. |
| `EPA_punt` | Float64 | Total EPA on punt plays. |
| `kickoff_plays` | Int64 | Number of kickoff plays. |
| `EPA_kickoff` | Float64 | Total EPA on kickoff plays. |
| `rushes` | Int64 | Number of rushing attempts. |
| `rush_yards` | Float64 | Total yards the team gained on rush plays. |
| `yards_per_rush` | Float64 | Yards gained per rushing attempt. |
| `rushing_power_rate` | Float64 | Share of carries that were power rushing attempts. |
| `rushing_first_downs_created` | Int64 | Number of first downs created on rush plays. |
| `rushing_first_downs_created_rate` | Float64 | Share of rush plays that created a first down. |
| `EPA_rushing_overall` | Float64 | Total EPA on rush plays. |
| `EPA_rushing_per_play` | Float64 | EPA per rush play. |
| `EPA_explosive_rushing` | Int64 | Count of explosive rush plays. A play count, not an EPA total. |
| `EPA_explosive_rushing_rate` | Float64 | Explosive-play rate on rush plays, over ESPN's qualifying-play denominator. |
| `EPA_non_explosive_rushing` | Float64 | Total EPA on rush plays with explosive plays excluded. |
| `EPA_non_explosive_rushing_per_play` | Float64 | EPA per rush play with explosive plays excluded. |
| `passes` | Int64 | Number of pass plays the team ran. |
| `pass_yards` | Float64 | Total yards the team gained on pass plays. |
| `yards_per_pass` | Float64 | Team game yards per pass. |
| `passing_first_downs_created` | Int64 | Number of first downs created on pass plays. |
| `passing_first_downs_created_rate` | Float64 | Share of pass plays that created a first down. |
| `EPA_passing_overall` | Float64 | Total EPA on pass plays. |
| `EPA_passing_per_play` | Float64 | EPA per pass play. |
| `EPA_explosive_passing` | Int64 | Count of explosive pass plays. A play count, not an EPA total. |
| `EPA_explosive_passing_rate` | Float64 | Explosive-play rate on pass plays, over ESPN's qualifying-play denominator. |
| `EPA_non_explosive_passing` | Float64 | Total EPA on pass plays with explosive plays excluded. |
| `EPA_non_explosive_passing_per_play` | Float64 | EPA per pass play with explosive plays excluded. |
| `scrimmage_plays` | Int64 | Number of plays from scrimmage (rushes plus passes), excluding special teams. |
| `EPA_overall_off` | Float64 | Total offensive EPA for the team. Duplicated exactly by EPA_overall_offense in every published season checked -- prefer one and ignore the other. |
| `EPA_overall_offense` | Float64 | Total offensive EPA. An exact duplicate of EPA_overall_off. |
| `EPA_per_play` | Float64 | Offensive EPA per play. |
| `EPA_non_explosive` | Float64 | Total EPA with explosive plays excluded, isolating the team's routine-down production. |
| `EPA_non_explosive_per_play` | Float64 | EPA per play with explosive plays excluded. |
| `EPA_explosive` | Int64 | Count of explosive plays, per ESPN's advanced box score. Despite the EPA_ prefix this is a play COUNT, not an EPA total. |
| `EPA_explosive_rate` | Float64 | Explosive-play rate. Note this is NOT EPA_explosive divided by EPA_plays -- ESPN divides by its own smaller qualifying-play count, so deriving it yourself will not reproduce this value. |
| `passes_rate` | Float64 | Share of the team's plays from scrimmage that were pass plays. |
| `off_yards` | Int64 | Offensive yards gained from scrimmage. |
| `total_off_yards` | Int64 | Total offensive yards across all plays. |
| `yards_per_play` | Float64 | Yards gained per play. |
| `EPA_plays` | Int64 | Number of plays ESPN's advanced box score scored for the team. |
| `total_yards` | Int64 | Total yards the team gained across all plays. |
| `EPA_overall_total` | Float64 | Total EPA across all phases, which is why it differs from the offense-only EPA_overall_off. |
| `rushes_rate` | Float64 | Share of the team's plays from scrimmage that were rush plays. |
| `first_downs_created` | Int64 | Number of first downs the team created. |
| `first_downs_created_rate` | Float64 | Share of the team's plays that created a first down. |
| `EPA_rushing_power` | Float64 | Total EPA on power rushing situations, as classified by ESPN's advanced box score. |
| `EPA_rushing_power_per_play` | Float64 | EPA per play on power rushing situations. |
| `rushing_power_success` | Int64 | Count of power rushing attempts that gained the yardage needed. An integer count, not a rate -- the rate is published separately as rushing_power_success_rate. |
| `rushing_power_success_rate` | Float64 | Share of power rushing attempts that succeeded. |
| `rushing_power` | Int64 | Count of power rushing attempts, in short-yardage situations as classified by ESPN's advanced box score. |
| `rushing_stuff` | Int64 | Count of stuffed rushing attempts. |
| `rushing_stuff_rate` | Float64 | Share of the team's carries that were stuffed at or behind the line of scrimmage. |
| `rushing_stopped` | Int64 | Count of rushing attempts stopped at or behind the line of scrimmage. |
| `rushing_stopped_rate` | Float64 | Share of carries stopped at or behind the line of scrimmage. |
| `rushing_opportunity` | Int64 | Count of rushing opportunities -- carries that reached ESPN's opportunity threshold. |
| `rushing_opportunity_rate` | Float64 | Share of carries that qualified as rushing opportunities. |
| `rushing_highlight` | Int64 | Highlight yards -- rushing yardage credited to the back rather than the offensive line. |
| `rushing_highlight_rate` | Float64 | Share of rushing yardage that was highlight (back-credited) yardage. |
| `rushing_highlight_yards` | Float64 | Total highlight yards the team accumulated -- the yardage credited to ball carriers rather than the line. The per-carry figure is rushing_highlight_yards_per_opp. |
| `line_yards` | Float64 | Line yards -- the portion of rushing yardage credited to the offensive line under the standard rushing decomposition. ESPN applies its own qualifying threshold for the yardage split. |
| `line_yards_per_carry` | Float64 | Line yards per rushing attempt. |
| `second_level_yards` | Float64 | Second-level yards -- rushing yardage earned just beyond the line of scrimmage. |
| `open_field_yards` | Float64 | Open-field yards -- rushing yardage earned well downfield, past the second level. |
| `game_id` | Int64 | ESPN game identifier. |
| `season` | Int64 | Season (4-digit year). |
| `week` | Int64 | Game week of the season. |

```python
load_cfb_adv_team(seasons=2024)
```

## load_cfb_adv_passing

Release: [espn_cfb_adv_passing](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_adv_passing) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_adv_passing/adv_passing_{season}.parquet`
### Returns {#load_cfb_adv_passing-returns}

| col_name | type | description |
|---|---|---|
| `pos_team_id` | Int64 | ESPN team id of the team on offense. Present for every season 2004+. |
| `pos_team` | String | Team name in possession at the start of the play (offense, kickoff-aware). |
| `passer_player_name` | String | Display name of the passer -- the FIRST participant in that role on the play. |
| `Comp` | Int64 | Completed passes recorded in the advanced box score. |
| `Att` | Int64 | Pass attempts recorded in the advanced box score. |
| `xComp` | Float64 | Expected completions, summed from the per-play completion model. |
| `Yds` | Float64 | Passing yards from the advanced box score. |
| `Pass_TD` | Int64 | Passing touchdowns. |
| `Int` | Int64 | Interceptions thrown. |
| `YPA` | Float64 | Yards per pass attempt. |
| `EPA` | Float64 | Expected Points Added on the play (cfbfastR EPA model output). |
| `EPA_per_Play` | Float64 | EPA per play on the passer's plays. |
| `WPA` | Float64 |  |
| `SR` | Float64 | Success rate on the passer's plays. |
| `Sck` | Int64 | Times the passer was sacked. |
| `CompPct` | Float64 | Completion percentage from the advanced box score. |
| `xCompPct` | Float64 | Expected completion percentage from the per-play completion model. |
| `CPOE` | Float64 | Completion percentage over expected -- actual minus modelled completion rate. |
| `AirYds` | Int64 | Air yards -- distance the ball travelled in the air, past the line of scrimmage, summed over the player's attempts/targets. |
| `aDOT` | Float64 | Average depth of target -- mean air yards per attempt/target. |
| `CompAirYds` | Int64 | Air yards on completed passes only. |
| `YAC` | Int64 | Yards after catch. |
| `AirYdsPct` | Float64 | Air-yards rate published alongside AirYds. Despite the `Pct` suffix this is NOT a 0-100 percentage and is not AirYds/Yds (verified: max absolute difference 33.0); observed values run negative to slightly above 1. |
| `qbr_epa` | Float64 | EPA variant used as an input to the QBR calculation. |
| `sack_epa` | Float64 | EPA credited to the player's sacks taken. |
| `pass_epa` | Float64 | EPA credited to the player's pass plays. |
| `rush_epa` | Float64 | EPA credited to the player's rush plays. |
| `pen_epa` | Float64 | EPA attributable to penalties on the player's plays. |
| `spread` | Float64 | Pre-game point spread from the selected provider. |
| `era0` | Int64 | Rule-era indicator for the earliest modelled era. |
| `era1` | Int64 | Rule-era indicator for the second modelled era. |
| `era2` | Int64 | Rule-era indicator for the third modelled era. |
| `era3` | Int64 | Rule-era indicator for the most recent modelled era. |
| `exp_qbr` | Float64 | Expected QBR for the passer. |
| `game_id` | Int64 | ESPN game identifier. |
| `season` | Int64 | Season (4-digit year). |
| `week` | Int64 | Game week of the season. |

```python
load_cfb_adv_passing(seasons=2024)
```

## load_cfb_adv_rushing

Release: [espn_cfb_adv_rushing](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_adv_rushing) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_adv_rushing/adv_rushing_{season}.parquet`
### Returns {#load_cfb_adv_rushing-returns}

| col_name | type | description |
|---|---|---|
| `pos_team_id` | Int64 | ESPN team id of the team on offense. Present for every season 2004+. |
| `pos_team` | String | Team name in possession at the start of the play (offense, kickoff-aware). |
| `rusher_player_name` | String | Display name of the ball carrier on a rush -- the FIRST participant in that role on the play. |
| `Car` | Int64 | Rushing attempts credited to this ball carrier in the game. |
| `Yds` | Float64 | Passing yards from the advanced box score. |
| `Rush_TD` | Int64 | Rushing touchdowns scored by this ball carrier in the game. |
| `YPC` | Float64 | Yards per carry, the mean rushing yardage across the player's attempts in the game. |
| `EPA` | Float64 | Expected Points Added on the play (cfbfastR EPA model output). |
| `EPA_per_Play` | Float64 | EPA per play on the passer's plays. |
| `WPA` | Float64 |  |
| `SR` | Float64 | Success rate on the passer's plays. |
| `Fum` | Int64 | Count of the carrier's rush attempts whose play text mentions a fumble; it is a play-level flag, not a fumble charged to this player. |
| `Fum_Lost` | Int64 | Count of the carrier's rush attempts on which a fumble was lost to the opponent. |
| `game_id` | Int64 | ESPN game identifier. |
| `season` | Int64 | Season (4-digit year). |
| `week` | Int64 | Game week of the season. |

```python
load_cfb_adv_rushing(seasons=2024)
```

## load_cfb_adv_receiving

Release: [espn_cfb_adv_receiving](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_adv_receiving) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_adv_receiving/adv_receiving_{season}.parquet`
### Returns {#load_cfb_adv_receiving-returns}

| col_name | type | description |
|---|---|---|
| `pos_team_id` | Int64 | ESPN team id of the team on offense. Present for every season 2004+. |
| `pos_team` | String | Team name in possession at the start of the play (offense, kickoff-aware). |
| `receiver_player_name` | String | Display name of the targeted receiver -- the FIRST participant in that role on the play. |
| `Rec` | Int64 | Receptions credited to the receiver, the number of completions on plays where this player was the targeted receiver. |
| `Tar` | Int64 | Times the player was targeted on a pass attempt, the denominator behind YPT. |
| `Yds` | Float64 | Passing yards from the advanced box score. |
| `Rec_TD` | Int64 | Receiving touchdowns, the count of the player's targeted plays that ended in a passing touchdown. |
| `YPT` | Float64 | Receiving yards per target, the mean of receiving yardage over every target rather than over receptions only. |
| `EPA` | Float64 | Expected Points Added on the play (cfbfastR EPA model output). |
| `EPA_per_Play` | Float64 | EPA per play on the passer's plays. |
| `WPA` | Float64 |  |
| `SR` | Float64 | Success rate on the passer's plays. |
| `Fum` | Int64 | Count of the receiver's targeted pass plays whose text mentions a fumble; it is a play-level flag, not a fumble charged to this player. |
| `Fum_Lost` | Int64 | Count of the receiver's targeted plays on which a fumble was lost to the opponent. |
| `AirYds` | Int64 | Air yards -- distance the ball travelled in the air, past the line of scrimmage, summed over the player's attempts/targets. |
| `aDOT` | Float64 | Average depth of target -- mean air yards per attempt/target. |
| `CompAirYds` | Int64 | Air yards on completed passes only. |
| `YAC` | Int64 | Yards after catch. |
| `AirYdsPct` | Float64 | Air-yards rate published alongside AirYds. Despite the `Pct` suffix this is NOT a 0-100 percentage and is not AirYds/Yds (verified: max absolute difference 33.0); observed values run negative to slightly above 1. |
| `game_id` | Int64 | ESPN game identifier. |
| `season` | Int64 | Season (4-digit year). |
| `week` | Int64 | Game week of the season. |

```python
load_cfb_adv_receiving(seasons=2024)
```

## load_cfb_adv_defensive

Release: [espn_cfb_adv_defensive](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_adv_defensive) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_adv_defensive/adv_defensive_{season}.parquet`
### Returns {#load_cfb_adv_defensive-returns}

| col_name | type | description |
|---|---|---|
| `def_pos_team_id` | Int64 | ESPN team id of the team on defense. Present for every season 2004+. |
| `def_pos_team` | String | Team name on defense at the start of the play. |
| `scrimmage_plays` | Int64 | Number of plays from scrimmage (rushes plus passes), excluding special teams. |
| `TFL` | Int64 | Count of scrimmage plays the defense held to negative yardage (non-penalty, non-special-teams, ESPN statYardage below zero) plus every sack. |
| `TFL_pass` | Int64 | The TFL count restricted to plays classified as passes, so it covers sacks together with completions and laterals stopped behind the line. |
| `TFL_rush` | Int64 | The TFL count restricted to plays classified as rushes, that is rushing attempts the defense stopped for negative yardage. |
| `havoc_total` | Int64 | Total havoc rate. |
| `havoc_total_rate` | Float64 | Share of the defense's scrimmage plays producing a havoc event, a 0-to-1 fraction equal to havoc_total divided by scrimmage_plays. |
| `fumbles` | Int64 | Fumbles the defense forced, counted from plays whose narrative contains the phrase forced by, not the total number of fumbles on the play. |
| `def_int` | Int64 | Interceptions the defense recorded, counted from plays ESPN types as Interception Return or Interception Return Touchdown. |
| `drive_stopped_rate` | Float64 | Percentage from 0 to 100 of the defense's scrimmage plays that occurred on drives ending in a punt, fumble, interception, or turnover on downs; the denominator is plays, not drives. |
| `num_pass_plays` | Int64 | Number of pass scrimmage plays the defense faced, the denominator behind havoc_total_pass_rate and sacks_rate. |
| `havoc_total_pass` | Int64 | Havoc events (tackle for loss, sack, interception, forced fumble, or pass breakup) recorded on the pass plays the defense faced. |
| `havoc_total_pass_rate` | Float64 | havoc_total_pass divided by num_pass_plays, the defense's havoc rate against the pass as a 0-to-1 fraction. |
| `sacks` | Int64 | Team sacks. |
| `sacks_rate` | Float64 | Sacks divided by pass plays faced, the defense's per-pass-play sack rate as a 0-to-1 fraction. |
| `pass_breakups` | Int64 | Passes the defense broke up, counted from plays whose narrative contains the phrase broken up by. |
| `havoc_total_rush` | Int64 | Havoc events recorded on the rush plays the defense faced, in practice tackles for loss and forced fumbles. |
| `havoc_total_rush_rate` | Float64 | Havoc events per rush play faced, the mean of the havoc flag over the defense's rush scrimmage plays. |
| `game_id` | Int64 | ESPN game identifier. |
| `season` | Int64 | Season (4-digit year). |
| `week` | Int64 | Game week of the season. |

```python
load_cfb_adv_defensive(seasons=2024)
```

## load_cfb_adv_defensive_players

Release: [espn_cfb_adv_defensive_players](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_adv_defensive_players) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_adv_defensive_players/adv_defensive_players_{season}.parquet`
### Returns {#load_cfb_adv_defensive_players-returns}

| col_name | type | description |
|---|---|---|
| `def_pos_team_id` | Int64 | ESPN team id of the team on defense. Present for every season 2004+. |
| `def_pos_team` | String | Display name of the team on defense (e.g. 'Ohio State Buckeyes'). Held an ESPN team id until the 2026-08 republish; the id now lives in def_pos_team_id. |
| `player_name` | String | Display name of the defender. |
| `sacks` | Float64 | Sacks recorded by the defender. Available from 2005 on; null for 2004. |
| `sacks_yards` | Float64 | Yards lost by the offense on the defender's sacks. Available from 2005 on; null for 2004. |
| `pass_breakups` | Int64 | Passes broken up by the defender. Available from 2005 on; null for 2004. |
| `interceptions` | Int64 | Passes intercepted by the defender. Available from 2014 on; null for 2004-2013, which ESPN ships without interception statistics in this block. |
| `interceptions_yards` | Int64 | Yards returned on the defender's interceptions. Available from 2014 on; null for 2004-2013. |
| `game_id` | Int64 | ESPN game identifier. |
| `season` | Int64 | Season (4-digit year). |
| `week` | Int64 | Game week of the season. |
| `forced_fumbles` | Int64 | Fumbles forced by the defender. Available from 2005 on; null for 2004, which ESPN ships with only the fumble-recovery statistics. |
| `fumble_recoveries` | Int64 | Fumbles recovered by the defender. Available for every season 2004+. |
| `fumble_recoveries_yards` | Int64 | Yards returned on the defender's fumble recoveries. Available for every season 2004+. |

```python
load_cfb_adv_defensive_players(seasons=2024)
```

## load_cfb_adv_drives

Release: [espn_cfb_adv_drives](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_adv_drives) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_adv_drives/adv_drives_{season}.parquet`
### Returns {#load_cfb_adv_drives-returns}

| col_name | type | description |
|---|---|---|
| `pos_team_id` | Int64 | ESPN team id of the team on offense. Present for every season 2004+. |
| `pos_team` | String | Team name in possession at the start of the play (offense, kickoff-aware). |
| `drive_total_available_yards` | Float64 | Sum of each drive's starting distance to the opponent end zone taken across every scrimmage play, so a drive contributes its available yards once per play rather than once per drive. |
| `drive_total_gained_yards` | Int64 | Sum of ESPN's per-drive yardage repeated across every scrimmage play of that drive, so a drive contributes its yardage once per play. |
| `avg_field_position` | Float64 | Mean distance to the opponent end zone at drive start averaged over the team's scrimmage plays, exactly drive_total_available_yards divided by that play count. |
| `plays_per_drive` | Float64 | Mean of ESPN's per-drive offensivePlays taken over plays rather than over drives, which weights every drive by its own length. |
| `yards_per_drive` | Float64 | Mean of ESPN's per-drive yardage taken over plays rather than over drives, exactly drive_total_gained_yards divided by the team's scrimmage-play count. |
| `drives` | Int64 | Number of distinct ESPN drive ids on which the team ran at least one scrimmage play. |
| `drive_total_gained_yards_rate` | Float64 | Available-yards conversion as a percentage, 100 times drive_total_gained_yards over drive_total_available_yards with both sums play-weighted. |
| `game_id` | Int64 | ESPN game identifier. |
| `season` | Int64 | Season (4-digit year). |
| `week` | Int64 | Game week of the season. |

```python
load_cfb_adv_drives(seasons=2024)
```

## load_cfb_adv_situational

Release: [espn_cfb_adv_situational](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_adv_situational) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_adv_situational/adv_situational_{season}.parquet`
### Returns {#load_cfb_adv_situational-returns}

| col_name | type | description |
|---|---|---|
| `pos_team_id` | Int64 | ESPN team id of the team on offense. Present for every season 2004+. |
| `pos_team` | String | Display name of the team on offense (e.g. 'Ohio State Buckeyes'). |
| `EPA_success` | Int64 | Count of successful plays. Despite the EPA_ prefix this is a play COUNT, not an EPA total. |
| `EPA_success_rate` | Float64 | Success rate -- the share of those plays ESPN scored as successful. |
| `EPA_success_pass` | Int64 | Count of successful plays on pass plays. Despite the EPA_ prefix this is a play COUNT, not an EPA total. |
| `EPA_success_pass_rate` | Float64 | Success rate on pass plays -- the share of those plays ESPN scored as successful. |
| `EPA_success_rush` | Int64 | Count of successful plays on rush plays. Despite the EPA_ prefix this is a play COUNT, not an EPA total. |
| `EPA_success_rush_rate` | Float64 | Success rate on rush plays -- the share of those plays ESPN scored as successful. |
| `EPA_success_rz` | Int64 | Count of successful plays on the red zone. Despite the EPA_ prefix this is a play COUNT, not an EPA total. |
| `EPA_success_rate_rz` | Float64 | Success rate on the red zone -- the share of those plays ESPN scored as successful. |
| `EPA_success_third` | Int64 | Count of successful plays on third down. Despite the EPA_ prefix this is a play COUNT, not an EPA total. |
| `EPA_success_rate_third` | Float64 | Success rate on third down -- the share of those plays ESPN scored as successful. |
| `EPA_success_early_down` | Int64 | Count of successful plays on early downs. Despite the EPA_ prefix this is a play COUNT, not an EPA total. |
| `EPA_success_early_down_rate` | Float64 | Success rate on early downs -- the share of those plays ESPN scored as successful. |
| `early_downs` | Int64 | Number of plays the team ran on early downs. |
| `early_down_pass_rate` | Float64 | Share of the team's plays on early downs that were pass plays. |
| `early_down_rush_rate` | Float64 | Share of the team's plays on early downs that were rush plays. |
| `EPA_early_down` | Float64 | Total EPA the team generated on early downs. |
| `EPA_early_down_per_play` | Float64 | EPA per play on early downs. |
| `early_down_first_down` | Int64 | Number of early-down plays that produced a first down. |
| `early_down_first_down_rate` | Float64 | Share of early-down plays that produced a first down. |
| `early_down_pass` | Int64 | Number of pass plays the team ran on early downs. |
| `EPA_early_down_pass` | Float64 | Total EPA the team generated on early downs on pass plays. |
| `EPA_early_down_pass_per_play` | Float64 | EPA per play on early downs on pass plays. |
| `EPA_success_early_down_pass` | Int64 | Count of successful plays on early downs on pass plays. Despite the EPA_ prefix this is a play COUNT, not an EPA total. |
| `EPA_success_early_down_pass_rate` | Float64 | Success rate on early downs on pass plays -- the share of those plays ESPN scored as successful. |
| `early_down_rush` | Int64 | Number of rush plays the team ran on early downs. |
| `EPA_early_down_rush` | Float64 | Total EPA the team generated on early downs on rush plays. |
| `EPA_early_down_rush_per_play` | Float64 | EPA per play on early downs on rush plays. |
| `EPA_success_early_down_rush` | Int64 | Count of successful plays on early downs on rush plays. Despite the EPA_ prefix this is a play COUNT, not an EPA total. |
| `EPA_success_early_down_rush_rate` | Float64 | Success rate on early downs on rush plays -- the share of those plays ESPN scored as successful. |
| `middle_8` | Int64 | Number of plays the team ran in the middle eight -- the closing minutes of the first half and opening minutes of the second. |
| `middle_8_pass_rate` | Float64 | Share of the team's plays on the middle eight -- the closing minutes of the first half and opening minutes of the second that were pass plays. |
| `middle_8_rush_rate` | Float64 | Share of the team's plays on the middle eight -- the closing minutes of the first half and opening minutes of the second that were rush plays. |
| `EPA_middle_8` | Float64 | Total EPA the team generated on the middle eight -- the closing minutes of the first half and opening minutes of the second. |
| `EPA_middle_8_per_play` | Float64 | EPA per play on the middle eight -- the closing minutes of the first half and opening minutes of the second. |
| `EPA_middle_8_success` | Int64 | Count of successful plays on the middle eight -- the closing minutes of the first half and opening minutes of the second. Despite the EPA_ prefix this is a play COUNT, not an EPA total. |
| `EPA_middle_8_success_rate` | Float64 | Success rate on the middle eight -- the closing minutes of the first half and opening minutes of the second -- the share of those plays ESPN scored as successful. |
| `middle_8_pass` | Int64 | Number of pass plays the team ran on the middle eight -- the closing minutes of the first half and opening minutes of the second. |
| `EPA_middle_8_pass` | Float64 | Total EPA the team generated on the middle eight -- the closing minutes of the first half and opening minutes of the second on pass plays. |
| `EPA_middle_8_pass_per_play` | Float64 | EPA per play on the middle eight -- the closing minutes of the first half and opening minutes of the second on pass plays. |
| `EPA_middle_8_success_pass` | Int64 | Count of successful plays on the middle eight -- the closing minutes of the first half and opening minutes of the second on pass plays. Despite the EPA_ prefix this is a play COUNT, not an EPA total. |
| `EPA_middle_8_success_pass_rate` | Float64 | Success rate on the middle eight -- the closing minutes of the first half and opening minutes of the second on pass plays -- the share of those plays ESPN scored as successful. |
| `middle_8_rush` | Int64 | Number of rush plays the team ran on the middle eight -- the closing minutes of the first half and opening minutes of the second. |
| `EPA_middle_8_rush` | Float64 | Total EPA the team generated on the middle eight -- the closing minutes of the first half and opening minutes of the second on rush plays. |
| `EPA_middle_8_rush_per_play` | Float64 | EPA per play on the middle eight -- the closing minutes of the first half and opening minutes of the second on rush plays. |
| `EPA_middle_8_success_rush` | Int64 | Count of successful plays on the middle eight -- the closing minutes of the first half and opening minutes of the second on rush plays. Despite the EPA_ prefix this is a play COUNT, not an EPA total. |
| `EPA_middle_8_success_rush_rate` | Float64 | Success rate on the middle eight -- the closing minutes of the first half and opening minutes of the second on rush plays -- the share of those plays ESPN scored as successful. |
| `EPA_success_late_down` | Int64 | Count of successful plays on late downs. Despite the EPA_ prefix this is a play COUNT, not an EPA total. |
| `EPA_success_late_down_pass` | Int64 | Count of successful plays on late downs on pass plays. Despite the EPA_ prefix this is a play COUNT, not an EPA total. |
| `EPA_success_late_down_rush` | Int64 | Count of successful plays on late downs on rush plays. Despite the EPA_ prefix this is a play COUNT, not an EPA total. |
| `late_downs` | Int64 | Number of plays the team ran on late downs. |
| `late_down_pass` | Int64 | Number of pass plays the team ran on late downs. |
| `late_down_rush` | Int64 | Number of rush plays the team ran on late downs. |
| `EPA_late_down` | Float64 | Total EPA the team generated on late downs. |
| `EPA_late_down_per_play` | Float64 | EPA per play on late downs. |
| `EPA_success_late_down_rate` | Float64 | Success rate on late downs -- the share of those plays ESPN scored as successful. |
| `EPA_success_late_down_pass_rate` | Float64 | Success rate on late downs on pass plays -- the share of those plays ESPN scored as successful. |
| `EPA_success_late_down_rush_rate` | Float64 | Success rate on late downs on rush plays -- the share of those plays ESPN scored as successful. |
| `late_down_pass_rate` | Float64 | Share of the team's plays on late downs that were pass plays. |
| `late_down_rush_rate` | Float64 | Share of the team's plays on late downs that were rush plays. |
| `EPA_success_standard_down` | Int64 | Count of successful plays on standard downs (the team ahead of schedule for the series). Despite the EPA_ prefix this is a play COUNT, not an EPA total. |
| `EPA_success_standard_down_rate` | Float64 | Success rate on standard downs (the team ahead of schedule for the series) -- the share of those plays ESPN scored as successful. |
| `EPA_standard_down` | Float64 | Total EPA the team generated on standard downs (the team ahead of schedule for the series). |
| `EPA_standard_down_per_play` | Float64 | EPA per play on standard downs (the team ahead of schedule for the series). |
| `standard_downs` | Int64 | Number of plays the team ran on standard downs (the team ahead of schedule for the series). |
| `EPA_success_passing_down` | Int64 | Count of successful plays on passing downs (the team behind schedule for the series). Despite the EPA_ prefix this is a play COUNT, not an EPA total. |
| `EPA_success_passing_down_rate` | Float64 | Success rate on passing downs (the team behind schedule for the series) -- the share of those plays ESPN scored as successful. |
| `EPA_passing_down` | Float64 | Total EPA the team generated on passing downs (the team behind schedule for the series). |
| `EPA_passing_down_per_play` | Float64 | EPA per play on passing downs (the team behind schedule for the series). |
| `passing_downs` | Int64 | Number of plays the team ran on passing downs (the team behind schedule for the series). |
| `game_id` | Int64 | ESPN game identifier. |
| `season` | Int64 | Season (4-digit year). |
| `week` | Int64 | Game week of the season. |

```python
load_cfb_adv_situational(seasons=2024)
```

## load_cfb_adv_specialists

Release: [espn_cfb_adv_specialists](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_adv_specialists) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_adv_specialists/adv_specialists_{season}.parquet`
### Returns {#load_cfb_adv_specialists-returns}

| col_name | type | description |
|---|---|---|
| `pos_team_id` | Int64 | ESPN team id of the team on offense. Present for every season 2004+. |
| `pos_team` | String | Team name in possession at the start of the play (offense, kickoff-aware). |
| `player_name` | String |  |
| `field_goals` | Int64 | Number of field-goal attempts. |
| `field_goals_yards` | Int64 | Sum of the field-goal attempt distances parsed out of the play text; it stays at zero when no distance could be parsed from the narrative. |
| `punts` | Int64 | Punts attempted. |
| `punts_yards` | Int64 | Total gross punt yardage parsed from the play text for this punter, working out to roughly 42 yards per punt league-wide. |
| `kick_returns` | Int64 | Number of kick returns. |
| `kick_returns_yards` | Int64 | Total yards the team gained returning kickoffs. |
| `punt_returns` | Int64 | Number of punt returns. |
| `punt_returns_yards` | Int64 | Total punt-return yardage credited to this returner, with fair catches, downed punts, and out-of-bounds punts scored as zero. |
| `game_id` | Int64 | ESPN game identifier. |
| `season` | Int64 | Season (4-digit year). |
| `week` | Int64 | Game week of the season. |

```python
load_cfb_adv_specialists(seasons=2024)
```

## load_cfb_adv_turnover

Release: [espn_cfb_adv_turnover](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_adv_turnover) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_adv_turnover/adv_turnover_{season}.parquet`
### Returns {#load_cfb_adv_turnover-returns}

| col_name | type | description |
|---|---|---|
| `pos_team_id` | Int64 | ESPN team id of the team on offense. Present for every season 2004+. |
| `pos_team` | String | Team name in possession at the start of the play (offense, kickoff-aware). |
| `turnovers` | Int64 | Turnovers total. |
| `st_turnovers_lost` | Int64 | Turnovers the team lost on special-teams plays. |
| `Int` | Int64 | Interceptions thrown. |
| `fumbles_lost` | Int64 | Fumbles lost. |
| `pass_breakups` | Int64 | Passes thrown by this offense that the opposing defense broke up; it equals the opponent's row in the advanced defensive table exactly. |
| `total_fumbles` | Int64 | Team total fumbles. |
| `fumbles_recovered` | Int64 | Team fumbles recovered. |
| `team_id` | Int64 | ESPN team id. |
| `turnovers_pbp` | Int64 | Turnover count derived from the play-by-play, retained unchanged so it can be reconciled against the ESPN-sourced turnovers total. |
| `Int_pbp` | Int64 | Interception count derived from the play-by-play, kept alongside the ESPN-sourced Int for reconciliation. |
| `fumbles_lost_pbp` | Int64 | Fumbles-lost count derived from the play-by-play, kept alongside the ESPN-sourced fumbles_lost for reconciliation. |
| `espn_sourced` | Boolean | CONSTANT: true on every published row. It records that the row was built from the ESPN feed rather than an alternate provider, and no other provider is currently used. |
| `expected_turnovers` | Float64 | Turnover expectation for this team, computed as half its total fumbles plus 0.22 times its passes_defended, or 0.22 times its pass breakups plus interceptions where passes_defended is null. |
| `expected_turnover_margin` | Float64 | The opponent's expected_turnovers minus this team's, so positive means the team was expected to win the turnover battle. |
| `turnover_margin` | Int64 | The opponent's turnovers minus this team's turnovers, positive when the team gained more possessions than it gave away. |
| `turnover_luck` | Float64 | Points of scoring luck attributed to turnovers, five points per turnover times the gap between turnover_margin and expected_turnover_margin. |
| `takeaways` | Int64 |  |
| `st_turnovers_gained` | Int64 | Special-teams turnovers this team recovered, taken as the opponent's st_turnovers_lost. |
| `fumble_recoveries_gained` | Int64 | Opponent fumbles this team recovered, taken as the opponent's fumbles_lost. |
| `game_id` | Int64 | ESPN game identifier. |
| `season` | Int64 | Season (4-digit year). |
| `week` | Int64 | Game week of the season. |

```python
load_cfb_adv_turnover(seasons=2024)
```

## load_cfb_adv_team_gamelog

Release: [espn_cfb_adv_team_gamelog](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_adv_team_gamelog) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_adv_team_gamelog/adv_team_gamelog_{season}.parquet`
### Returns {#load_cfb_adv_team_gamelog-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season (4-digit year). |
| `week` | Int64 | Game week of the season. |
| `season_type` | Int32 | ESPN season type (2 = regular, 3 = postseason). |
| `game_id` | Int64 | ESPN game identifier. |
| `start_date` | String | Season start timestamp (ISO 8601, UTC). |
| `team_id` | Int64 | ESPN team id. |
| `team` | String | Team name. |
| `opponent_id` | Int64 | ESPN team id of the opponent. |
| `opponent` | String | Opponent team name. |
| `is_home` | Boolean |  |
| `neutral_site` | Boolean | TRUE/FALSE flag for if the game took place at a neutral site. |
| `points_for` | Int64 |  |
| `points_against` | Int64 |  |
| `margin` | Int64 | Final scoring margin from this team's perspective, exactly points_for minus points_against. |
| `win` | Boolean |  |
| `rushing_highlight_yards_per_opp` | Float64 | Highlight yards per rushing opportunity. |
| `total_pen_yards` | Int64 | Total penalty yards assessed. |
| `EPA_penalty` | Float64 | Total EPA attributed to penalties. |
| `penalty_first_downs_created` | Int64 | Number of first downs the team gained via opponent penalty. |
| `penalty_first_downs_created_rate` | Float64 | Share of the team's first downs that came via opponent penalty. |
| `penalties` | Int64 | Number of penalties assessed against the team. |
| `penalty_yards` | Int64 | Net penalty yardage assessed against the team; can be negative when enforcement moved the team forward on balance. |
| `special_teams_plays` | Int64 | Number of special-teams plays. |
| `EPA_sp` | Float64 | Total special-teams EPA, ESPN's abbreviated field for the same phase. |
| `EPA_special_teams` | Float64 | Total EPA generated on special-teams plays. |
| `field_goals` | Int64 | Number of field-goal attempts. |
| `EPA_fg` | Float64 | Total EPA on field-goal attempts. |
| `punt_plays` | Int64 | Number of punt plays. |
| `EPA_punt` | Float64 | Total EPA on punt plays. |
| `kickoff_plays` | Int64 | Number of kickoff plays. |
| `EPA_kickoff` | Float64 | Total EPA on kickoff plays. |
| `rushes` | Int64 | Number of rushing attempts. |
| `rush_yards` | Float64 | Total yards the team gained on rush plays. |
| `yards_per_rush` | Float64 | Yards gained per rushing attempt. |
| `rushing_power_rate` | Float64 | Share of carries that were power rushing attempts. |
| `rushing_first_downs_created` | Int64 | Number of first downs created on rush plays. |
| `rushing_first_downs_created_rate` | Float64 | Share of rush plays that created a first down. |
| `EPA_rushing_overall` | Float64 | Total EPA on rush plays. |
| `EPA_rushing_per_play` | Float64 | EPA per rush play. |
| `EPA_explosive_rushing` | Int64 | Count of explosive rush plays. A play count, not an EPA total. |
| `EPA_explosive_rushing_rate` | Float64 | Explosive-play rate on rush plays, over ESPN's qualifying-play denominator. |
| `EPA_non_explosive_rushing` | Float64 | Total EPA on rush plays with explosive plays excluded. |
| `EPA_non_explosive_rushing_per_play` | Float64 | EPA per rush play with explosive plays excluded. |
| `passes` | Int64 | Number of pass plays the team ran. |
| `pass_yards` | Float64 | Total yards the team gained on pass plays. |
| `yards_per_pass` | Float64 | Team game yards per pass. |
| `passing_first_downs_created` | Int64 | Number of first downs created on pass plays. |
| `passing_first_downs_created_rate` | Float64 | Share of pass plays that created a first down. |
| `EPA_passing_overall` | Float64 | Total EPA on pass plays. |
| `EPA_passing_per_play` | Float64 | EPA per pass play. |
| `EPA_explosive_passing` | Int64 | Count of explosive pass plays. A play count, not an EPA total. |
| `EPA_explosive_passing_rate` | Float64 | Explosive-play rate on pass plays, over ESPN's qualifying-play denominator. |
| `EPA_non_explosive_passing` | Float64 | Total EPA on pass plays with explosive plays excluded. |
| `EPA_non_explosive_passing_per_play` | Float64 | EPA per pass play with explosive plays excluded. |
| `scrimmage_plays` | Int64 | Number of plays from scrimmage (rushes plus passes), excluding special teams. |
| `EPA_overall_off` | Float64 | Total offensive EPA for the team. Duplicated exactly by EPA_overall_offense in every published season checked -- prefer one and ignore the other. |
| `EPA_overall_offense` | Float64 | Total offensive EPA. An exact duplicate of EPA_overall_off. |
| `EPA_per_play` | Float64 | Offensive EPA per play. |
| `EPA_non_explosive` | Float64 | Total EPA with explosive plays excluded, isolating the team's routine-down production. |
| `EPA_non_explosive_per_play` | Float64 | EPA per play with explosive plays excluded. |
| `EPA_explosive` | Int64 | Count of explosive plays, per ESPN's advanced box score. Despite the EPA_ prefix this is a play COUNT, not an EPA total. |
| `EPA_explosive_rate` | Float64 | Explosive-play rate. Note this is NOT EPA_explosive divided by EPA_plays -- ESPN divides by its own smaller qualifying-play count, so deriving it yourself will not reproduce this value. |
| `passes_rate` | Float64 | Share of the team's plays from scrimmage that were pass plays. |
| `off_yards` | Int64 | Offensive yards gained from scrimmage. |
| `total_off_yards` | Int64 | Total offensive yards across all plays. |
| `yards_per_play` | Float64 | Yards gained per play. |
| `EPA_plays` | Int64 | Number of plays ESPN's advanced box score scored for the team. |
| `total_yards` | Int64 | Total yards the team gained across all plays. |
| `EPA_overall_total` | Float64 | Total EPA across all phases, which is why it differs from the offense-only EPA_overall_off. |
| `rushes_rate` | Float64 | Share of the team's plays from scrimmage that were rush plays. |
| `first_downs_created` | Int64 | Number of first downs the team created. |
| `first_downs_created_rate` | Float64 | Share of the team's plays that created a first down. |
| `EPA_rushing_power` | Float64 | Total EPA on power rushing situations, as classified by ESPN's advanced box score. |
| `EPA_rushing_power_per_play` | Float64 | EPA per play on power rushing situations. |
| `rushing_power_success` | Int64 | Count of power rushing attempts that gained the yardage needed. An integer count, not a rate -- the rate is published separately as rushing_power_success_rate. |
| `rushing_power_success_rate` | Float64 | Share of power rushing attempts that succeeded. |
| `rushing_power` | Int64 | Count of power rushing attempts, in short-yardage situations as classified by ESPN's advanced box score. |
| `rushing_stuff` | Int64 | Count of stuffed rushing attempts. |
| `rushing_stuff_rate` | Float64 | Share of the team's carries that were stuffed at or behind the line of scrimmage. |
| `rushing_stopped` | Int64 | Count of rushing attempts stopped at or behind the line of scrimmage. |
| `rushing_stopped_rate` | Float64 | Share of carries stopped at or behind the line of scrimmage. |
| `rushing_opportunity` | Int64 | Count of rushing opportunities -- carries that reached ESPN's opportunity threshold. |
| `rushing_opportunity_rate` | Float64 | Share of carries that qualified as rushing opportunities. |
| `rushing_highlight` | Int64 | Highlight yards -- rushing yardage credited to the back rather than the offensive line. |
| `rushing_highlight_rate` | Float64 | Share of rushing yardage that was highlight (back-credited) yardage. |
| `rushing_highlight_yards` | Float64 | Total highlight yards the team accumulated -- the yardage credited to ball carriers rather than the line. The per-carry figure is rushing_highlight_yards_per_opp. |
| `line_yards` | Float64 | Line yards -- the portion of rushing yardage credited to the offensive line under the standard rushing decomposition. ESPN applies its own qualifying threshold for the yardage split. |
| `line_yards_per_carry` | Float64 | Line yards per rushing attempt. |
| `second_level_yards` | Float64 | Second-level yards -- rushing yardage earned just beyond the line of scrimmage. |
| `open_field_yards` | Float64 | Open-field yards -- rushing yardage earned well downfield, past the second level. |

```python
load_cfb_adv_team_gamelog(seasons=2024)
```
