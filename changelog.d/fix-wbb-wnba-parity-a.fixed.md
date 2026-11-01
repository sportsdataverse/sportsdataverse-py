- **WBB/WNBA:** `espn_{wbb,wnba,nba}_pbp` leave a game with no betting line null (spread, home favorite, home
  spread, total), as MBB does; they invented a 2.5-point home-favored line. NHL dropped its 1.5 / 5.5 line and
  reads a lone provider. The release builder nulls a stored invented line when no spread was available. (#1047)
- **WBB/WNBA:** release loader floors follow the published assets: WBB shots / game_rosters / standings /
  season stats / schedule_crosswalk, WNBA shots / game_rosters / officials / standings / season stats /
  crosswalks / draft, and WNBA Stats coaches / draft / game_rosters / officials / rosters. (#1047)
- **WBB/WNBA:** ids join without a cast: WNBA `game_rosters` / `officials` `game_id`, `rosters` `athlete_id` and
  WBB `game_rosters` `game_id` are Int32, as are the crosswalk `espn_*` ids; the WNBA crosswalk `wnba_*` team /
  player ids are Int64, as WNBA Stats ships them. (#1047)
- **WBB/WNBA:** the rds writer stores an integer column outside int32 as character, so 18-digit WBB play ids no
  longer collide in R; the WNBA win-probability ids turn into text through Int64. (#1047)
