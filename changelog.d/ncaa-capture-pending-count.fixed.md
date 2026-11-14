- **NCAA:** capture's `pending=` line counts only contests with no bundle on disk, and prints the master's uncaptured
  count and each shard's `to_fetch`. The master's `captured` flag is never refreshed, so the old count included games
  already captured (2026-11-13: 691 shown, 196 real). The shard split is unchanged. (#1180)
