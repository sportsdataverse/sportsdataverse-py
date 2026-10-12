- **NFL:** `NFLPlayProcess` reads a first down by penalty from the 2009+ NFL foul text (2002-2008 text names no accepted
  foul, so those seasons stay unrecovered), and `series` / `fixed_drive` follow nflfastR on muffed punts, timeout rows
  and the kickoff after a defensive touchdown; the ESPN NFL pbp and its series tables need the reprocess. (#806)
