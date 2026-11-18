- **Basketball:** the EPV start label (`epv_states`' `remaining_points` at `step` 0) now holds a score the team made on
  defense since its previous possession (a technical or flagrant free throw, or a basket a misplaced boundary puts in
  the other team's possession), credited as the possessions release credits it: to the scoring team's next
  possession (`credit_defense_scoring`). The label was short (by 1 to 3 points) on 233 of the 243,735 NBA 2024-25 possessions and
  39 of the 44,589 WNBA 2025 ones, and is now the release's `points` on every one (also WNBA 2012 and 2016). Event
  `points` are unchanged (that score is no event's). The NBA and WNBA EPV boosters are refit on the fixed labels, so
  served EPV moves slightly; every gate still passes with the thresholds unchanged. College is not refit: 2 of the
  874,700 men's 2025 possessions carry one (none in 2024, none in the women's 2024 or 2025).
