- **NBA:** `nba_draft_model` and `nba_rookie_projection` score live combine data. They raised on the
  wrappers' shared name columns, never read the five drill and shooting columns the wrappers name
  differently, and zero-filled missing measurements instead of using the training medians. (#1314)
