- **Validation:** `pbp_invariants` holds a period's last basketball play to the period's 0:00
  (`seconds.<scope>_period_last_row_ends_at_zero`), so a producer that clamps `end` to
  `min(end, start)` no longer passes.
