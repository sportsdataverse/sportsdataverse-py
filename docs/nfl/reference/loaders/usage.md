# NFL dataset loaders — Usage

> NFL dataset loaders — Usage — function reference in sdv-py, the SportsDataverse Python package.

## load_nfl_usage_players

Release: [espn_nfl_usage_players](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nfl_usage_players) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_nfl_usage_players/usage_players_{season}.parquet`

:::caution[Coverage]
2005 has no asset: ESPN's 2005 NFL feed carries no play text, so no usage rows exist for it. position_group is null before 2014, when the feed starts carrying participant positions. A season with no asset raises NoDataError.
:::

### Returns {#load_nfl_usage_players-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season the rows were summed over, keyed by the season's starting calendar year -- the same season stamp the released play-by-play carries. |
| `pos_team_id` | Int64 | ESPN team id of the possession team (offense); joins to the ESPN teams and schedule datasets on team_id. |
| `pos_team` | String | Display name of the possession team (offense), as carried on the released play-by-play (e.g. "Kansas City Chiefs", "Georgia Bulldogs"). |
| `player_id` | String | ESPN athlete id of the player, as a string; null when the play-by-play named the player without an id (the row is then keyed on the name). |
| `player_name` | String | Player display name, as carried on the play-by-play participants; the season row carries the most frequent non-null name over his games, so a renamed player with a player_id keeps one row per team (a row with no player_id is keyed on its name, so a rename there splits it). |
| `position_group` | String | Position group of the player (QB, RB, WR, TE, OL, DL, LB, DB, K, P, ...) resolved from the play participants' ESPN position ids; null when no participant row carried a position for the player; the season row carries his most frequent non-null group, so a game with no group does not split his season. |
| `rushes` | Int64 | Rushing attempts on which the player was the rusher, on standing scrimmage plays (plays nullified by penalty are excluded). |
| `targets` | Int64 | Pass targets on which the player was the receiver, complete or not, on standing scrimmage plays. |
| `receptions` | Int64 | Targets the player caught (completed passes). |
| `touches` | Int64 | Rushes plus receptions. |
| `opportunities` | Int64 | Rushes plus targets -- the denominator of the per-opportunity rates. |
| `rush_yards` | Float64 | Yards gained on the player's rushes (yds_rushed, falling back to the play's statYardage). |
| `receiving_yards` | Float64 | Yards gained on the player's receptions (yds_receiving, falling back to statYardage); an incomplete target adds 0. |
| `first_downs` | Int64 | Rushes and targets of the player that created a first down (first_down_created). |
| `touchdowns` | Int64 | Rushes and targets of the player that scored an offensive touchdown. |
| `fd_or_td` | Int64 | Rushes and targets of the player that produced a first down or an offensive touchdown (a play counts once even when both flags are set). |
| `explosive_plays` | Int64 | Rushes and targets of the player flagged EPA_explosive on the play-by-play. |
| `successful_plays` | Int64 | Rushes and targets of the player flagged EPA_success (positive EPA) on the play-by-play. |
| `epa` | Float64 | Play EPA summed over the player's rushes and targets. |
| `rz_rushes` | Int64 | Rushes by the player snapped in the red zone (rz_play: 20 or fewer yards to the end zone at the snap). |
| `rz_targets` | Int64 | Targets of the player snapped in the red zone. |
| `rz_touches` | Int64 | Touches (rushes plus receptions) by the player snapped in the red zone. |
| `rz_touchdowns` | Int64 | Red-zone rushes and targets of the player that scored an offensive touchdown. |
| `so_rushes` | Int64 | Rushes by the player snapped in scoring-opportunity territory (scoring_opp: 40 or fewer yards to the end zone at the snap). |
| `so_targets` | Int64 | Targets of the player snapped in scoring-opportunity territory. |
| `so_touches` | Int64 | Touches (rushes plus receptions) by the player snapped in scoring-opportunity territory. |
| `so_touchdowns` | Int64 | Scoring-opportunity rushes and targets of the player that scored an offensive touchdown. |
| `third_down_opportunities` | Int64 | Rushes and targets of the player that came on third down. |
| `third_down_conversions` | Int64 | Third-down rushes and targets of the player that converted (a first down or an offensive touchdown). |
| `third_down_expected` | Float64 | Expected third-down conversions for the player: the league's bundled third-down yards-to-go conversion curve summed over the third-down opportunities; null when no curve was available. |
| `team_targets` | Int64 | The team's targets over the same games, counting every standing scrimmage target whether or not a receiver was attributed -- the denominator of target_share. |
| `team_first_downs` | Int64 | The team's first downs created on standing scrimmage plays over the same games -- the denominator of first_down_share. |
| `team_touches` | Int64 | The team's rushes plus completions over the same games -- the denominator of touch_share. |
| `games` | UInt32 | Per-game rows summed into this season row -- the games in which this key appeared in the section -- so it counts games with activity, not games played. |
| `fd_td_rate` | Float64 | fd_or_td / opportunities: the share of opportunities that produced a first down or an offensive touchdown; null with no opportunities. |
| `explosive_rate` | Float64 | explosive_plays / opportunities; null with no opportunities. |
| `success_rate` | Float64 | successful_plays / opportunities; null with no opportunities. |
| `epa_per_opportunity` | Float64 | epa / opportunities; null with no opportunities. |
| `rz_touchdown_rate` | Float64 | rz_touchdowns / rz_touches; null with no red-zone touches. |
| `so_touchdown_rate` | Float64 | so_touchdowns / so_touches; null with no scoring-opportunity touches. |
| `third_down_rate` | Float64 | third_down_conversions / third_down_opportunities; null with no third downs. |
| `third_down_over_expected` | Float64 | third_down_conversions minus third_down_expected: conversions above the distance-adjusted expectation; null when no curve was available. |
| `target_share` | Float64 | targets / team_targets: the player's share of the team's targets over the same games. |
| `first_down_share` | Float64 | first_downs / team_first_downs: the player's share of the team's first downs. |
| `touch_share` | Float64 | touches / team_touches: the player's share of the team's touches. |

```python
load_nfl_usage_players(seasons=2024)
```

## load_nfl_usage_position_groups

Release: [espn_nfl_usage_position_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nfl_usage_position_groups) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_nfl_usage_position_groups/usage_position_groups_{season}.parquet`

:::caution[Coverage]
Built from ESPN play participants, which the NFL feed carries from 2014; earlier seasons have no asset (NoDataError).
:::

### Returns {#load_nfl_usage_position_groups-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season the rows were summed over, keyed by the season's starting calendar year -- the same season stamp the released play-by-play carries. |
| `pos_team_id` | Int64 | ESPN team id of the possession team (offense); joins to the ESPN teams and schedule datasets on team_id. |
| `pos_team` | String | Display name of the possession team (offense), as carried on the released play-by-play (e.g. "Kansas City Chiefs", "Georgia Bulldogs"). |
| `position_group` | String | Position group of the player (QB, RB, WR, TE, OL, DL, LB, DB, K, P, ...) resolved from the play participants' ESPN position ids; null when no participant row carried a position for the player. |
| `rushes` | Int64 | Rushing attempts on which the position group was the rusher, on standing scrimmage plays (plays nullified by penalty are excluded). |
| `targets` | Int64 | Pass targets on which the position group was the receiver, complete or not, on standing scrimmage plays. |
| `receptions` | Int64 | Targets the position group caught (completed passes). |
| `touches` | Int64 | Rushes plus receptions. |
| `opportunities` | Int64 | Rushes plus targets -- the denominator of the per-opportunity rates. |
| `rush_yards` | Float64 | Yards gained on the position group's rushes (yds_rushed, falling back to the play's statYardage). |
| `receiving_yards` | Float64 | Yards gained on the position group's receptions (yds_receiving, falling back to statYardage); an incomplete target adds 0. |
| `first_downs` | Int64 | Rushes and targets of the position group that created a first down (first_down_created). |
| `touchdowns` | Int64 | Rushes and targets of the position group that scored an offensive touchdown. |
| `fd_or_td` | Int64 | Rushes and targets of the position group that produced a first down or an offensive touchdown (a play counts once even when both flags are set). |
| `explosive_plays` | Int64 | Rushes and targets of the position group flagged EPA_explosive on the play-by-play. |
| `successful_plays` | Int64 | Rushes and targets of the position group flagged EPA_success (positive EPA) on the play-by-play. |
| `epa` | Float64 | Play EPA summed over the position group's rushes and targets. |
| `rz_rushes` | Int64 | Rushes by the position group snapped in the red zone (rz_play: 20 or fewer yards to the end zone at the snap). |
| `rz_targets` | Int64 | Targets of the position group snapped in the red zone. |
| `rz_touches` | Int64 | Touches (rushes plus receptions) by the position group snapped in the red zone. |
| `rz_touchdowns` | Int64 | Red-zone rushes and targets of the position group that scored an offensive touchdown. |
| `so_rushes` | Int64 | Rushes by the position group snapped in scoring-opportunity territory (scoring_opp: 40 or fewer yards to the end zone at the snap). |
| `so_targets` | Int64 | Targets of the position group snapped in scoring-opportunity territory. |
| `so_touches` | Int64 | Touches (rushes plus receptions) by the position group snapped in scoring-opportunity territory. |
| `so_touchdowns` | Int64 | Scoring-opportunity rushes and targets of the position group that scored an offensive touchdown. |
| `third_down_opportunities` | Int64 | Rushes and targets of the position group that came on third down. |
| `third_down_conversions` | Int64 | Third-down rushes and targets of the position group that converted (a first down or an offensive touchdown). |
| `third_down_expected` | Float64 | Expected third-down conversions for the position group: the league's bundled third-down yards-to-go conversion curve summed over the third-down opportunities; null when no curve was available. |
| `team_targets` | Int64 | The team's targets over the same games, counting every standing scrimmage target whether or not a receiver was attributed -- the denominator of target_share. |
| `team_first_downs` | Int64 | The team's first downs created on standing scrimmage plays over the same games -- the denominator of first_down_share. |
| `team_touches` | Int64 | The team's rushes plus completions over the same games -- the denominator of touch_share. |
| `games` | UInt32 | Per-game rows summed into this season row -- the games in which this key appeared in the section -- so it counts games with activity, not games played. |
| `fd_td_rate` | Float64 | fd_or_td / opportunities: the share of opportunities that produced a first down or an offensive touchdown; null with no opportunities. |
| `explosive_rate` | Float64 | explosive_plays / opportunities; null with no opportunities. |
| `success_rate` | Float64 | successful_plays / opportunities; null with no opportunities. |
| `epa_per_opportunity` | Float64 | epa / opportunities; null with no opportunities. |
| `rz_touchdown_rate` | Float64 | rz_touchdowns / rz_touches; null with no red-zone touches. |
| `so_touchdown_rate` | Float64 | so_touchdowns / so_touches; null with no scoring-opportunity touches. |
| `third_down_rate` | Float64 | third_down_conversions / third_down_opportunities; null with no third downs. |
| `third_down_over_expected` | Float64 | third_down_conversions minus third_down_expected: conversions above the distance-adjusted expectation; null when no curve was available. |
| `target_share` | Float64 | targets / team_targets: the position group's share of the team's targets over the same games. |
| `first_down_share` | Float64 | first_downs / team_first_downs: the position group's share of the team's first downs. |
| `touch_share` | Float64 | touches / team_touches: the position group's share of the team's touches. |

```python
load_nfl_usage_position_groups(seasons=2024)
```

## load_nfl_usage_tackles

Release: [espn_nfl_usage_tackles](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nfl_usage_tackles) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_nfl_usage_tackles/usage_tackles_{season}.parquet`

:::caution[Coverage]
Built from ESPN play participants (tackler / assist ids), which the NFL feed carries from 2014; earlier seasons have no asset (NoDataError).
:::

### Returns {#load_nfl_usage_tackles-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season the rows were summed over, keyed by the season's starting calendar year -- the same season stamp the released play-by-play carries. |
| `def_pos_team_id` | Int64 | ESPN team id of the play's defense (the NFL build passes no game roster, so a tackle is not moved to the tackler's own team); joins to the ESPN teams dataset on team_id. |
| `def_pos_team` | String | Display name of the play's defense. The NFL build passes no game roster, so every tackle stays with the play's defense, including kickoff / punt coverage tackles and tackles after a turnover. |
| `player_id` | String | ESPN athlete id of the player, as a string; null when the play-by-play named the player without an id (the row is then keyed on the name). |
| `player_name` | String | Player display name, as carried on the play-by-play participants; the season row carries the most frequent non-null name over his games, so a renamed player with a player_id keeps one row per team (a row with no player_id is keyed on its name, so a rename there splits it). |
| `position_group` | String | Position group of the player (QB, RB, WR, TE, OL, DL, LB, DB, K, P, ...) resolved from the play participants' ESPN position ids; null when no participant row carried a position for the player; the season row carries his most frequent non-null group, so a game with no group does not split his season. |
| `tackles` | Int64 | Solo tackles credited to the player in the play participants (tackler_player_ids). |
| `assists` | Int64 | Assisted tackles credited to the player in the play participants (assisted_by_player_ids). |
| `scrimmage_tackle_points` | Float64 | tackle_points (tackles plus 0.5 times assists) earned on the defense's own standing scrimmage snaps only -- kickoff, punt and field-goal coverage, plays a penalty wiped out and tackles after a turnover are excluded; tackle_share's numerator. |
| `tackle_points` | Float64 | tackles plus 0.5 times assists. |
| `games` | UInt32 | Per-game rows summed into this season row -- the games in which this key appeared in the section -- so it counts games with activity, not games played. |
| `team_tackle_points` | Float64 | tackle_points summed over every player credited to the defense for the season, special-teams tackles included; tackle_share's denominator is the scrimmage-only total. |
| `team_scrimmage_tackle_points` | Float64 | scrimmage_tackle_points summed over every player credited to the defense for the season; tackle_share's denominator. |
| `tackle_share` | Float64 | scrimmage_tackle_points / team_scrimmage_tackle_points: the player's share of the defense's tackle points on its own standing scrimmage snaps -- kickoff, punt and field-goal coverage and plays a penalty wiped out are left out (the NFL build passes no game roster, so a tackle after a turnover stays with the play's defense and is shared); null when the defense has none. |

```python
load_nfl_usage_tackles(seasons=2024)
```

## load_nfl_usage_position_group_tackles

Release: [espn_nfl_usage_position_group_tackles](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nfl_usage_position_group_tackles) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_nfl_usage_position_group_tackles/usage_position_group_tackles_{season}.parquet`

:::caution[Coverage]
Built from ESPN play participants, which the NFL feed carries from 2014; earlier seasons have no asset (NoDataError).
:::

### Returns {#load_nfl_usage_position_group_tackles-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season the rows were summed over, keyed by the season's starting calendar year -- the same season stamp the released play-by-play carries. |
| `def_pos_team_id` | Int64 | ESPN team id of the play's defense (the NFL build passes no game roster, so a tackle is not moved to the tackler's own team); joins to the ESPN teams dataset on team_id. |
| `def_pos_team` | String | Display name of the play's defense. The NFL build passes no game roster, so every tackle stays with the play's defense, including kickoff / punt coverage tackles and tackles after a turnover. |
| `position_group` | String | Position group of the player (QB, RB, WR, TE, OL, DL, LB, DB, K, P, ...) resolved from the play participants' ESPN position ids; null when no participant row carried a position for the player. |
| `tackles` | Int64 | Solo tackles credited to the position group in the play participants (tackler_player_ids). |
| `assists` | Int64 | Assisted tackles credited to the position group in the play participants (assisted_by_player_ids). |
| `scrimmage_tackle_points` | Float64 | tackle_points (tackles plus 0.5 times assists) earned by the position group on the defense's own standing scrimmage snaps only -- kickoff, punt and field-goal coverage, plays a penalty wiped out and tackles after a turnover are excluded; tackle_share's numerator. |
| `tackle_points` | Float64 | tackles plus 0.5 times assists. |
| `games` | UInt32 | Per-game rows summed into this season row -- the games in which this key appeared in the section -- so it counts games with activity, not games played. |
| `team_tackle_points` | Float64 | tackle_points summed over every player credited to the defense for the season, special-teams tackles included; tackle_share's denominator is the scrimmage-only total. |
| `team_scrimmage_tackle_points` | Float64 | scrimmage_tackle_points summed over every position group credited to the defense for the season; tackle_share's denominator. |
| `tackle_share` | Float64 | scrimmage_tackle_points / team_scrimmage_tackle_points: the position group's share of the defense's tackle points on its own standing scrimmage snaps -- kickoff, punt and field-goal coverage and plays a penalty wiped out are left out (the NFL build passes no game roster, so a tackle after a turnover stays with the play's defense and is shared); null when the defense has none. |

```python
load_nfl_usage_position_group_tackles(seasons=2024)
```

## load_nfl_usage_teams

Release: [espn_nfl_usage_teams](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nfl_usage_teams) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_nfl_usage_teams/usage_teams_{season}.parquet`

:::caution[Coverage]
Published 2002-2026 (2005 is built from ESPN's play-text-less 2005 feed, so it is thin). A season with no asset raises NoDataError.
:::

### Returns {#load_nfl_usage_teams-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season the rows were summed over, keyed by the season's starting calendar year -- the same season stamp the released play-by-play carries. |
| `pos_team_id` | Int64 | ESPN team id of the possession team (offense); joins to the ESPN teams and schedule datasets on team_id. |
| `pos_team` | String | Display name of the possession team (offense), as carried on the released play-by-play (e.g. "Kansas City Chiefs", "Georgia Bulldogs"). |
| `plays` | Int64 | Standing scrimmage plays the offense ran (plays nullified by penalty are excluded). |
| `rushes` | Int64 | Rushing plays among the standing scrimmage plays. |
| `targets` | Int64 | Pass plays with a targeted receiver (the target flag). |
| `completions` | Int64 | Completed passes among the standing scrimmage plays. |
| `first_downs` | Int64 | Scrimmage plays that created a first down (first_down_created). |
| `touchdowns` | Int64 | Scrimmage plays that scored an offensive touchdown. |
| `explosive_plays` | Int64 | Scrimmage plays flagged EPA_explosive on the play-by-play. |
| `successful_plays` | Int64 | Scrimmage plays flagged EPA_success (positive EPA) on the play-by-play. |
| `epa` | Float64 | Play EPA summed over the standing scrimmage plays. |
| `third_down_opportunities` | Int64 | Third-down scrimmage plays with a known distance. |
| `third_down_conversions` | Int64 | Third-down plays that produced a first down or an offensive touchdown. |
| `third_down_expected` | Float64 | Expected third-down conversions: the league's bundled third-down yards-to-go conversion curve summed over the third-down plays; null when no curve was available. |
| `rz_plays` | Int64 | Scrimmage plays snapped in the red zone (rz_play: 20 or fewer yards to the end zone at the snap). |
| `rz_successes` | Int64 | Red-zone plays flagged EPA_success. |
| `rz_epa` | Float64 | Play EPA summed over the red-zone plays. |
| `rz_touchdowns` | Int64 | Red-zone plays that scored an offensive touchdown. |
| `rz_targets` | Int64 | Red-zone pass plays with a targeted receiver. |
| `rz_rushes` | Int64 | Red-zone rushing plays. |
| `rz_trips` | Int64 | Drives with at least one red-zone play. |
| `rz_points` | Float64 | Drive points (touchdown 7, field goal 3, from drive.result) summed over the drives that reached the red zone. |
| `so_plays` | Int64 | Scrimmage plays snapped in scoring-opportunity territory (scoring_opp: 40 or fewer yards to the end zone at the snap). |
| `so_successes` | Int64 | Scoring-opportunity plays flagged EPA_success. |
| `so_epa` | Float64 | Play EPA summed over the scoring-opportunity plays. |
| `so_touchdowns` | Int64 | Scoring-opportunity plays that scored an offensive touchdown. |
| `so_targets` | Int64 | Scoring-opportunity pass plays with a targeted receiver. |
| `so_rushes` | Int64 | Scoring-opportunity rushing plays. |
| `so_trips` | Int64 | Drives with at least one play snapped in scoring-opportunity territory (the opponent's 40). |
| `so_points` | Float64 | Drive points (touchdown 7, field goal 3, from drive.result) summed over the drives that reached the opponent's 40. |
| `games` | UInt32 | Per-game rows summed into this season row -- the games in which this key appeared in the section -- so it counts games with activity, not games played. |
| `success_rate` | Float64 | successful_plays / plays; null with no plays. |
| `explosive_rate` | Float64 | explosive_plays / plays; null with no plays. |
| `epa_per_play` | Float64 | epa / plays; null with no plays. |
| `third_down_rate` | Float64 | third_down_conversions / third_down_opportunities; null with no third downs. |
| `third_down_over_expected` | Float64 | third_down_conversions minus third_down_expected: conversions above the distance-adjusted expectation; null when no curve was available. |
| `rz_touchdown_rate` | Float64 | rz_touchdowns / rz_trips: touchdowns per red-zone trip; null with no trips. |
| `rz_points_per_trip` | Float64 | rz_points / rz_trips; null with no trips. |
| `rz_success_rate` | Float64 | rz_successes / rz_plays; null with no red-zone plays. |
| `rz_epa_per_play` | Float64 | rz_epa / rz_plays; null with no red-zone plays. |
| `so_touchdown_rate` | Float64 | so_touchdowns / so_trips: touchdowns per scoring-opportunity trip; null with no trips. |
| `so_points_per_trip` | Float64 | so_points / so_trips; null with no trips. |
| `so_success_rate` | Float64 | so_successes / so_plays; null with no scoring-opportunity plays. |
| `so_epa_per_play` | Float64 | so_epa / so_plays; null with no scoring-opportunity plays. |

```python
load_nfl_usage_teams(seasons=2024)
```

## load_nfl_usage_drive_scripting

Release: [espn_nfl_usage_drive_scripting](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nfl_usage_drive_scripting) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_nfl_usage_drive_scripting/usage_drive_scripting_{season}.parquet`

:::caution[Coverage]
Published 2002-2026 (2005 is thin: ESPN's 2005 feed carries no play text). A season with no asset raises NoDataError.
:::

### Returns {#load_nfl_usage_drive_scripting-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season the rows were summed over, keyed by the season's starting calendar year -- the same season stamp the released play-by-play carries. |
| `pos_team_id` | Int64 | ESPN team id of the possession team (offense); joins to the ESPN teams and schedule datasets on team_id. |
| `pos_team` | String | Display name of the possession team (offense), as carried on the released play-by-play (e.g. "Kansas City Chiefs", "Georgia Bulldogs"). |
| `script` | String | "scripted" for the offense's first two drives of each half, "non_scripted" for every other drive. |
| `drives` | Int64 | Drives of this script type (distinct drive.id values with at least one standing scrimmage play). |
| `plays` | Int64 | Standing scrimmage plays on those drives. |
| `epa` | Float64 | Play EPA summed over those drives. |
| `successes` | Int64 | Plays flagged EPA_success on those drives. |
| `yards` | Float64 | statYardage summed over the plays on those drives. |
| `points` | Float64 | Drive points (touchdown 7, field goal 3, from drive.result) summed over those drives. |
| `touchdowns` | Int64 | Drives that included an offensive touchdown play. |
| `scoring_opps` | Int64 | Drives that reached scoring-opportunity territory (a play snapped 40 or fewer yards from the end zone). |
| `games` | UInt32 | Per-game rows summed into this season row -- the games in which this key appeared in the section -- so it counts games with activity, not games played. |
| `epa_per_play` | Float64 | epa / plays; null with no plays. |
| `success_rate` | Float64 | successes / plays; null with no plays. |
| `yards_per_play` | Float64 | yards / plays; null with no plays. |
| `points_per_drive` | Float64 | points / drives; null with no drives. |
| `touchdown_rate` | Float64 | touchdowns / drives: the share of drives that scored a touchdown; null with no drives. |
| `scoring_opp_rate` | Float64 | scoring_opps / drives: the share of drives that reached the opponent's 40; null with no drives. |

```python
load_nfl_usage_drive_scripting(seasons=2024)
```

## load_nfl_usage_st_kickers

Release: [espn_nfl_usage_st_kickers](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nfl_usage_st_kickers) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_nfl_usage_st_kickers/usage_st_kickers_{season}.parquet`

:::caution[Coverage]
No asset for 2005-2007 (2005 has no play text upstream); a season with no asset raises NoDataError.
:::

### Returns {#load_nfl_usage_st_kickers-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season the rows were summed over, keyed by the season's starting calendar year -- the same season stamp the released play-by-play carries. |
| `pos_team_id` | Int64 | ESPN team id of the kicking team (the kicker's own team); joins to the ESPN teams and schedule datasets on team_id. |
| `pos_team` | String | Display name of the kicking team (the kicker's own team), as carried on the released play-by-play (e.g. "Kansas City Chiefs", "Georgia Bulldogs"). |
| `player_id` | String | ESPN athlete id of the player, as a string; null when the play-by-play named the player without an id (the row is then keyed on the name). |
| `player_name` | String | Player display name, as carried on the play-by-play participants; the season row carries the most frequent non-null name over his games, so a renamed player with a player_id keeps one row per team (a row with no player_id is keyed on its name, so a rename there splits it). |
| `kickoffs` | Int64 | Kickoffs by the kicker that stood (kicks nullified by penalty are excluded). |
| `kickoff_yards` | Float64 | Kickoff distance summed over the kickoffs (yds_kickoff). |
| `kickoff_touchbacks` | Int64 | Kickoffs that resulted in a touchback. |
| `kickoff_onside` | Int64 | Onside kicks attempted by the kicker. |
| `kickoff_out_of_bounds` | Int64 | Kickoffs that went out of bounds. |
| `kickoff_returns_allowed` | Int64 | Kickoffs that were returned: not a touchback, onside, out of bounds or fair catch, and with a named returner. |
| `kickoff_return_yards_allowed` | Float64 | Return yards allowed on the returned kickoffs. |
| `kickoff_return_tds_allowed` | Int64 | Returned kickoffs that were taken back for a touchdown. |
| `kickoff_epa` | Float64 | Play EPA summed over the kickoffs from the kicking side (the play EPA negated, because the receiving team is the possession team on a kickoff). |
| `fg_attempts` | Int64 | Field-goal attempts by the kicker that stood (attempts nullified by penalty are excluded). |
| `fg_made` | Int64 | Field goals made by the kicker. |
| `fg_blocked` | Int64 | Field-goal attempts on which a blocker was credited. |
| `fg_0_39_attempts` | Int64 | Field-goal attempts from under 40 yards. |
| `fg_0_39_made` | Int64 | Field goals made from under 40 yards. |
| `fg_40_49_attempts` | Int64 | Field-goal attempts from 40 to 49 yards. |
| `fg_40_49_made` | Int64 | Field goals made from 40 to 49 yards. |
| `fg_50_plus_attempts` | Int64 | Field-goal attempts from 50 yards or more. |
| `fg_50_plus_made` | Int64 | Field goals made from 50 yards or more. |
| `fg_epa` | Float64 | Play EPA summed over the field-goal attempts. |
| `xp_attempts` | Int64 | Extra-point kick attempts by the kicker that stood. |
| `xp_made` | Int64 | Extra-point kicks made by the kicker. |
| `fg_long` | Float64 | Longest field goal made, in yards (a season maximum, not a sum). |
| `games` | UInt32 | Per-game rows summed into this season row -- the games in which this key appeared in the section -- so it counts games with activity, not games played. |
| `kickoff_avg` | Float64 | kickoff_yards / kickoffs; null with no kickoffs. |
| `kickoff_touchback_rate` | Float64 | kickoff_touchbacks / kickoffs; null with no kickoffs. |
| `kickoff_return_avg_allowed` | Float64 | kickoff_return_yards_allowed / kickoff_returns_allowed; null with no returns allowed. |
| `fg_pct` | Float64 | fg_made / fg_attempts; null with no attempts. |
| `xp_pct` | Float64 | xp_made / xp_attempts; null with no attempts. |

```python
load_nfl_usage_st_kickers(seasons=2024)
```

## load_nfl_usage_st_punters

Release: [espn_nfl_usage_st_punters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nfl_usage_st_punters) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_nfl_usage_st_punters/usage_st_punters_{season}.parquet`

:::caution[Coverage]
No asset for 2005-2007 (2005 has no play text upstream); a season with no asset raises NoDataError.
:::

### Returns {#load_nfl_usage_st_punters-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season the rows were summed over, keyed by the season's starting calendar year -- the same season stamp the released play-by-play carries. |
| `pos_team_id` | Int64 | ESPN team id of the punting team (the punter's own team); joins to the ESPN teams and schedule datasets on team_id. |
| `pos_team` | String | Display name of the punting team (the punter's own team), as carried on the released play-by-play (e.g. "Kansas City Chiefs", "Georgia Bulldogs"). |
| `player_id` | String | ESPN athlete id of the player, as a string; null when the play-by-play named the player without an id (the row is then keyed on the name). |
| `player_name` | String | Player display name, as carried on the play-by-play participants; the season row carries the most frequent non-null name over his games, so a renamed player with a player_id keeps one row per team (a row with no player_id is keyed on its name, so a rename there splits it). |
| `punts` | Int64 | Punts by the punter that stood (punts nullified by penalty are excluded). |
| `punt_yards` | Float64 | Gross punt distance summed over the punts (yds_punted). |
| `punt_touchbacks` | Int64 | Punts that resulted in a touchback. |
| `punt_inside_20` | Int64 | Punts that landed inside the receiving team's 20: yards to the end zone at the snap minus punt distance between 0 and 20, touchbacks excluded. |
| `punt_fair_catches` | Int64 | Punts that were fair caught. |
| `punt_downed` | Int64 | Punts downed by the coverage team. |
| `punt_out_of_bounds` | Int64 | Punts that went out of bounds. |
| `punt_blocked` | Int64 | Punts by the punter that were blocked. |
| `punt_returns_allowed` | Int64 | Punts that were returned: not a touchback, fair catch, downed, out of bounds or blocked, and with a named returner. |
| `punt_return_yards_allowed` | Float64 | Return yards allowed on the returned punts. |
| `punt_return_tds_allowed` | Int64 | Returned punts that were taken back for a touchdown. |
| `punt_epa` | Float64 | Play EPA summed over the punts (the punting team is the possession team on a punt, so this already reads from the punter's side). |
| `punt_long` | Float64 | Longest punt, in yards (a season maximum, not a sum). |
| `games` | UInt32 | Per-game rows summed into this season row -- the games in which this key appeared in the section -- so it counts games with activity, not games played. |
| `punt_net_yards` | Float64 | punt_yards minus punt_return_yards_allowed minus 20 yards per touchback. |
| `punt_avg` | Float64 | punt_yards / punts: gross punting average; null with no punts. |
| `punt_net_avg` | Float64 | punt_net_yards / punts: net punting average; null with no punts. |
| `punt_inside_20_rate` | Float64 | punt_inside_20 / punts; null with no punts. |
| `punt_return_avg_allowed` | Float64 | punt_return_yards_allowed / punt_returns_allowed; null with no returns allowed. |

```python
load_nfl_usage_st_punters(seasons=2024)
```

## load_nfl_usage_st_returners

Release: [espn_nfl_usage_st_returners](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nfl_usage_st_returners) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_nfl_usage_st_returners/usage_st_returners_{season}.parquet`

:::caution[Coverage]
No asset for 2005-2007 (2005 has no play text upstream); a season with no asset raises NoDataError.
:::

### Returns {#load_nfl_usage_st_returners-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season the rows were summed over, keyed by the season's starting calendar year -- the same season stamp the released play-by-play carries. |
| `pos_team_id` | Int64 | ESPN team id of the returning team (the returner's own team); joins to the ESPN teams and schedule datasets on team_id. |
| `pos_team` | String | Display name of the returning team (the returner's own team), as carried on the released play-by-play (e.g. "Kansas City Chiefs", "Georgia Bulldogs"). |
| `player_id` | String | ESPN athlete id of the player, as a string; null when the play-by-play named the player without an id (the row is then keyed on the name). |
| `player_name` | String | Player display name, as carried on the play-by-play participants; the season row carries the most frequent non-null name over his games, so a renamed player with a player_id keeps one row per team (a row with no player_id is keyed on its name, so a rename there splits it). |
| `kick_returns` | Int64 | Kickoff returns by the returner (kickoffs that were neither a touchback, onside, out of bounds nor fair caught). |
| `kick_return_yards` | Float64 | Kickoff return yards summed over the returns (yds_kickoff_return). |
| `kick_return_tds` | Int64 | Kickoff returns that scored a touchdown. |
| `kick_return_epa` | Float64 | Play EPA summed over the kickoff returns (the returning team is the possession team on a kickoff, so this reads from the returner's side). |
| `punt_returns` | Int64 | Punt returns by the returner (punts that were neither a touchback, fair catch, downed, out of bounds nor blocked). |
| `punt_return_yards` | Float64 | Punt return yards summed over the returns (yds_punt_return). |
| `punt_return_tds` | Int64 | Punt returns that scored a touchdown. |
| `punt_return_epa` | Float64 | Play EPA summed over the punt returns, negated so it reads from the return team's side (the punting team is the possession team on a punt). |
| `kick_return_long` | Float64 | Longest kickoff return, in yards (a season maximum, not a sum). |
| `punt_return_long` | Float64 | Longest punt return, in yards (a season maximum, not a sum). |
| `games` | UInt32 | Per-game rows summed into this season row -- the games in which this key appeared in the section -- so it counts games with activity, not games played. |
| `kick_return_avg` | Float64 | kick_return_yards / kick_returns; null with no kickoff returns. |
| `punt_return_avg` | Float64 | punt_return_yards / punt_returns; null with no punt returns. |

```python
load_nfl_usage_st_returners(seasons=2024)
```

## load_nfl_usage_st_blocks

Release: [espn_nfl_usage_st_blocks](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nfl_usage_st_blocks) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_nfl_usage_st_blocks/usage_st_blocks_{season}.parquet`

:::caution[Coverage]
Published from 2007 (no block participants earlier); a season with no asset raises NoDataError.
:::

### Returns {#load_nfl_usage_st_blocks-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season the rows were summed over, keyed by the season's starting calendar year -- the same season stamp the released play-by-play carries. |
| `def_pos_team_id` | Int64 | ESPN team id of the team that made the block (the defense on the kick). |
| `def_pos_team` | String | Display name of the team that made the block (the defense on the kick). |
| `player_id` | String | ESPN athlete id of the player, as a string; null when the play-by-play named the player without an id (the row is then keyed on the name). |
| `player_name` | String | Player display name, as carried on the play-by-play participants; the season row carries the most frequent non-null name over his games, so a renamed player with a player_id keeps one row per team (a row with no player_id is keyed on its name, so a rename there splits it). |
| `punt_blocks` | Int64 | Punts the player blocked (credited as the punt_block_player on the play). |
| `fg_blocks` | Int64 | Field-goal attempts the player blocked (credited as the fg_block_player on the play). |
| `blocks` | Int64 | punt_blocks plus fg_blocks. |
| `games` | UInt32 | Per-game rows summed into this season row -- the games in which this key appeared in the section -- so it counts games with activity, not games played. |

```python
load_nfl_usage_st_blocks(seasons=2024)
```

## load_nfl_usage_st_team

Release: [espn_nfl_usage_st_team](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nfl_usage_st_team) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_nfl_usage_st_team/usage_st_team_{season}.parquet`

:::caution[Coverage]
Published 2002-2026 (2005 is thin: ESPN's 2005 feed carries no play text). A season with no asset raises NoDataError.
:::

### Returns {#load_nfl_usage_st_team-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season the rows were summed over, keyed by the season's starting calendar year -- the same season stamp the released play-by-play carries. |
| `pos_team_id` | Int64 | ESPN team id of the team (its own kicking, punting and returns, plus what it allowed); joins to the ESPN teams and schedule datasets on team_id. |
| `pos_team` | String | Display name of the team (its own kicking, punting and returns, plus what it allowed), as carried on the released play-by-play (e.g. "Kansas City Chiefs", "Georgia Bulldogs"). |
| `kickoffs` | Int64 | Kickoffs by the team that stood (kicks nullified by penalty are excluded). |
| `kickoff_touchbacks` | Int64 | The team's kickoffs that resulted in a touchback. |
| `kickoff_returns_allowed` | Int64 | Kickoffs that were returned: not a touchback, onside, out of bounds or fair catch, and with a named returner. |
| `kickoff_return_yards_allowed` | Float64 | Return yards allowed on the team's returned kickoffs. |
| `kickoff_return_tds_allowed` | Int64 | The team's kickoffs that were returned for a touchdown. |
| `kickoff_epa` | Float64 | Play EPA summed over the kickoffs from the kicking side (the play EPA negated, because the receiving team is the possession team on a kickoff). |
| `kick_returns` | Int64 | Kickoff returns by the team (kickoffs received that were neither a touchback, onside, out of bounds nor fair caught). |
| `kick_return_yards` | Float64 | Kickoff return yards summed over the team's returns (yds_kickoff_return). |
| `kick_return_tds` | Int64 | The team's kickoff returns that scored a touchdown. |
| `kick_return_epa` | Float64 | Play EPA summed over the team's kickoff returns (the returning team is the possession team on a kickoff). |
| `punts` | Int64 | Punts by the team that stood (punts nullified by penalty are excluded). |
| `punt_yards` | Float64 | Gross punt distance summed over the team's punts (yds_punted). |
| `punt_touchbacks` | Int64 | The team's punts that resulted in a touchback. |
| `punts_blocked` | Int64 | The team's punts that were blocked. |
| `punt_returns_allowed` | Int64 | Punts that were returned: not a touchback, fair catch, downed, out of bounds or blocked, and with a named returner. |
| `punt_return_yards_allowed` | Float64 | Return yards allowed on the team's returned punts. |
| `punt_return_tds_allowed` | Int64 | The team's punts that were returned for a touchdown. |
| `punt_epa` | Float64 | Play EPA summed over the team's punts (the punting team is the possession team on a punt). |
| `punt_returns` | Int64 | Punt returns by the team (punts received that were neither a touchback, fair catch, downed, out of bounds nor blocked). |
| `punt_return_yards` | Float64 | Punt return yards summed over the team's returns (yds_punt_return). |
| `punt_return_tds` | Int64 | The team's punt returns that scored a touchdown. |
| `punt_return_epa` | Float64 | Play EPA summed over the team's punt returns, negated so it reads from the return team's side. |
| `fg_attempts` | Int64 | The team's field-goal attempts that stood. |
| `fg_made` | Int64 | The team's field goals made. |
| `fgs_blocked` | Int64 | The team's field-goal attempts that were blocked. |
| `fg_epa` | Float64 | Play EPA summed over the team's field-goal attempts. |
| `punt_blocks_by` | Int64 | Opponent punts the team blocked. |
| `fg_blocks_by` | Int64 | Opponent field-goal attempts the team blocked. |
| `games` | UInt32 | Per-game rows summed into this season row -- the games in which this key appeared in the section -- so it counts games with activity, not games played. |
| `punt_net_yards` | Float64 | punt_yards minus punt_return_yards_allowed minus 20 yards per touchback. |
| `kickoff_touchback_rate` | Float64 | kickoff_touchbacks / kickoffs; null with no kickoffs. |
| `kickoff_return_avg_allowed` | Float64 | kickoff_return_yards_allowed / kickoff_returns_allowed; null with no returns allowed. |
| `fg_pct` | Float64 | fg_made / fg_attempts; null with no attempts. |
| `punt_avg` | Float64 | punt_yards / punts: gross punting average; null with no punts. |
| `punt_net_avg` | Float64 | punt_net_yards / punts: net punting average; null with no punts. |
| `punt_return_avg_allowed` | Float64 | punt_return_yards_allowed / punt_returns_allowed; null with no returns allowed. |
| `kick_return_avg` | Float64 | kick_return_yards / kick_returns; null with no kickoff returns. |
| `punt_return_avg` | Float64 | punt_return_yards / punt_returns; null with no punt returns. |

```python
load_nfl_usage_st_team(seasons=2024)
```
