- **NCAA MBB/WBB:** game enrichment resolves a team whose game-page name ("FDU", "IU Indy") differs from the season's
  team list ("Fairleigh Dickinson", "IUPUI") through the schedules, so those programs' shots, pbp and box rows carry
  `ncaa_team_id` / `espn_team_id` again (ncaa-mbb-hoops-data#24, ncaa-wbb-hoops-data#23). (#1206)
- **NCAA MBB/WBB:** a side is mapped only from its own team's schedule row, and never onto the team the other side
  already resolves to. (#1206)
