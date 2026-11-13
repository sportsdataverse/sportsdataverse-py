- **NCAA:** capture's `pending=` line counts only contests with no bundle on disk, and also prints the master's
  uncaptured count and each shard's `to_fetch`. The master's `captured` flag is never refreshed, so the old count
  included every game already captured (2026-11-13: 691 shown, 196 real). The shard split is unchanged. (#1180)
