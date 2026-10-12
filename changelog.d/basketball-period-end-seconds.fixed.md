- **NBA / WNBA / MBB / WBB pbp:** a period's first play ends where the next play starts, not at the
  full period length after it started (WNBA Q2 at 9:41: 1781 -> 1800); a period's last play ends at
  0:00. Clock minutes are Int32 (WBB seconds Int32, NBA / WNBA Float64), not Float32. (#841)
