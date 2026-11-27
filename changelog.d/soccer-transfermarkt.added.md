- **Soccer:** `load_soccer_transfermarkt_{players,clubs,player_valuations,transfers,competitions}()` load the CC0
  Transfermarkt dataset (snapshot to 2026-07-06) with Int64 ids; `soccer_espn_transfermarkt_team_crosswalk()` maps ESPN
  team ids to Transfermarkt `club_id` (first tiers: 139/144 in the clubs table). (#1357)
