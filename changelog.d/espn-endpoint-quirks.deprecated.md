- **ESPN:** the Site v2 team routes `espn_<league>_team_record`, `_team_injuries`, `_team_news`,
  `_team_transactions`, `_team_leaders` and `_team_history` warn `DeprecationWarning` (removed in
  0.4.0): ESPN answers them with an empty object. Each warning names the league feed to use. (#PR)
