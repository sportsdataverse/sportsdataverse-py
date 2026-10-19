- **CFB:** a rush, pass, sack or penalty whose ESPN end block is `{down 0, yardLine 0, yardsToEndzone 0}` takes the next snap's down, distance
  and spot when the same team snaps next in the period (401754585's 2nd & 5 incompletion: EPA -1.85 -> -0.95); not a 4th-down play short of the
  line to gain (still a turnover on downs). A sweep of 2,591 cfb-raw games moved 1,695 such rows (1,335 EPA); supp-002 covered fumbles only.
- **CFB:** the mirrored-end repair reads across a Timeout or End Period row to the next real snap when that snap starts where the play's
  `statYardage` ends it (401856797's Q3-closing rush to the 1: EPA -4.62 -> 0.46; 91 rows in the sweep).
- **CFB:** a defensive Timeout row's `pos_team` is the offense, as nflfastR's `posteam` (owner ruling), so its `wp_before` adds the offense's
  margin to the offense's EP; its EPA stays 0 (1,803 Timeout rows in the sweep).
