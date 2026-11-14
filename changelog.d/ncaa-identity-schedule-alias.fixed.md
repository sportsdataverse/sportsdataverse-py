- **NCAA MBB/WBB:** game enrichment resolves a team whose game-page name differs from the season's team list, through
  the schedules. Pages show a school's current short name even on old games ("FDU", "IU Indy", "Saint Francis", "East
  Texas A&M"), while the team list keeps that season's name ("Fairleigh Dickinson", "IUPUI", "Saint Francis (PA)",
  "Texas A&M-Commerce"), so those programs' shots, pbp and box rows carried no `ncaa_team_id` / `espn_team_id`
  (ncaa-mbb-hoops-data#24, ncaa-wbb-hoops-data#23). A side is mapped only from its own team's schedule row, and never
  onto the team the other side already resolves to.
