- **ESPN soccer:** summaries null the `(0.0, 0.0)` "no location" coordinate pairs and strip trailing spaces from names.
  (#738)
- **Cricket:** `cricket_match_state` reads a DLS-reduced innings' revised over allocation from the score string instead
  of the format's full ball budget. (#738)
- **MLB:** `mlb_statcast_search` clamps a window before 2008-03-25 to the tracking floor with a warning, and
  `get_baselines` warns when it falls back to the nearest committed season. (#738)
- **Basketball courts:** `get_court(league_id, season=)` returns the three-point geometry in force that season (the NBA
  22-ft arc of 1994-97, the WNBA 19'9" / 20'6.25" / FIBA eras). (#738)
- **Spring football:** `get_sf_constants(league, season=)` resolves the UFL 2024 / 2025 / 2026, XFL and CFL rulesets.
  (#738)
- **HockeyTech:** `ot_period_length` carries the regular-season OT length for PWHL (300), ECHL and USHL (420) and BCHL
  (600). (#738)
