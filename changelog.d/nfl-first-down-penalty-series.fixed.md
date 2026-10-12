- **NFL:** `NFLPlayProcess` reads a first down by penalty from the NFL play text (it was false on nearly every row),
  and `series` / `fixed_drive` follow nflfastR on muffed punts, timeout rows and the kickoff after a defensive
  touchdown; the published ESPN NFL pbp and its series tables need the reprocess. (#806)
