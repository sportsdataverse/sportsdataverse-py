- **CFB:** `cfb_playoff_seeds` without `season`, and `sim`-keyed standings, use the 2026 CFP rule and the current
  conference tiebreakers (the 2026 ACC policy among them); pass `season=2025` for the previous ones. (#1058)
- **CFB:** `CONFERENCE_TIEBREAKERS` values are season-scoped epoch lists (`{"from", "rungs", "pool"}`), not rung
  lists, mirroring cfbseedR 0.2.0. (#1058)
