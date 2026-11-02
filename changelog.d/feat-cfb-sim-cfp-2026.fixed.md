- **CFB:** `cfb_standings` / `cfb_simulations` join float team ids as integers (`87.0` is team `"87"`), and raise
  on duplicate team rows in `rankings` / `tiebreaker_data` instead of duplicating standings rows. (#1058)
- **CFB:** `cfb_simulations` `first_round_bye` counts the bracket's real byes for any field size (it assumed four),
  and a division-format conference's ineligible division winner hands its berth to its own division. (#1058)
