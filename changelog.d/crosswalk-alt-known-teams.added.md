- **CFB crosswalks:** `_crosswalk_alt.match_season(known_teams=)` takes fixed ESPN -> provider team pairs for teams
  that share every game date (an FCS team with one FBS opponent, any team early in a season); `known_teams("cbs",
  "cfb")` serves the CBS resolver's map, so CBS CFB games match 99% of ESPN's schedule in most seasons. (#835)
