- **CFB:** a rush, pass, sack or penalty whose ESPN end block is `{down 0, yardLine 0, yardsToEndzone 0}` takes the
  next snap's down, distance and spot when the same team snaps next in the period (a 4th-down play short of the line
  to gain stays a turnover on downs); EPA moves on those rows, e.g. -1.85 -> -0.95 on a 2nd & 5 incompletion. (#905)
- **CFB:** the mirrored-end repair reads across a Timeout or End Period row to the next real snap when that snap starts
  where the play's `statYardage` ends it (a Q3-closing rush to the 1: EPA -4.62 -> 0.46). (#905)
- **CFB:** a defensive Timeout row's `pos_team` is the offense, as nflfastR's `posteam`, so its `wp_before` adds the
  offense's margin to the offense's EP; its EPA stays 0. (#905)
