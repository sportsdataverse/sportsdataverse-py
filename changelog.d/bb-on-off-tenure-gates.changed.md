- **Basketball:** the NBA / WNBA on/off rank gates (`ON_OFF_MIN_N`) are refit with the off side over the player's
  tenure span, as `player_on_off` now counts it: NBA `off_rtg_diff` 2,400 -> 2,500, `def_rtg_diff` 3,400 -> 3,900 and
  `net_rtg_diff` 7,900 -> 7,100; WNBA `off_rtg_diff` 720 -> 750 (BB-D5). (#1221)
- **Basketball:** WNBA `net_rtg_diff` is no longer ranked (owner ruling: no half signal on the seasons after the fit).
  (#1221)
- **Basketball:** the same refit moves NBA `net_rtg_on` 3,200 -> 3,100 (release drift), so the player impact net ranks
  and `advanced_min_poss` (now 1,550) follow it. (#1221)
