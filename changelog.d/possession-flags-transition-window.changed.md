- **NBA / WNBA:** `add_possession_flags`'s transition window (`TRANSITION_SECONDS`) is 8 seconds, not 10. On
  2023-24 it tracks Synergy's `Transition` play type better (r 0.892 vs 0.844, 0.97 vs 1.29 of its level). (#NNN)
- **NBA / WNBA:** the EPV `transition` state keeps the 10-second window its boosters were fitted on
  (`EPV_TRANSITION_SECONDS`), so EPV values do not change. (#NNN)
- **NBA / WNBA:** situational splits re-flag a possessions release that already carries BB-D7's flags, which
  the producers now publish, instead of raising on the clash. (#NNN)
