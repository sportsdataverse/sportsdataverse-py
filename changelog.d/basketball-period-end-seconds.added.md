- **Validation:** `pbp_invariants` checks basketball clock seconds: `end <= start` on every
  `*_seconds_remaining` pair, and `end` equals the next play's `start` within a period. (#841)
