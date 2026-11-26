- **College basketball:** the men's rank sample gates are refit on box possessions at the college free-throw weight
  0.475: `off_rtg` now ranks from 280 possessions (was 270), `def_rtg` from 450 (460) and `tov_pct` from 460 (450);
  the women's gates are unchanged. (#1343)
- **College basketball:** the ratings engine, the player-value features and the expected player box keep the pros'
  0.44. Refit at 0.475, box-BPM and the expected box failed their no-regression gates, and the ratings engine passed
  its own but broke a season-sim test (numbers in `sportsdataverse._common.possessions`). (#1343)
