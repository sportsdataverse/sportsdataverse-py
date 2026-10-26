- **NFL:** the kickoff-touchback spot is now the rule spot per season (`model_vars.touchback_yardline`: 80 / 75 / 70
  for 2024 / 65 from 2025) in the EP feature substitution, the overtime re-score and the ESPN `yds_kickoff_return`;
  nflfastR still scores 2024+ kickoffs at 75, so kickoff-row `ep` diverges from nflverse there by design. (#744)
