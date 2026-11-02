- **CFB:** `cfb_playoff_seeds(season=...)` selects the 2026 CFP field (ACC / Big Ten / Big 12 / SEC champions,
  the best Group-of-6 champion, Notre Dame if top 12, at-large) or the 2024-2025 rule; `cfb_standings` and
  `cfb_simulations` pick it from a `season`-keyed input. (#1058)
- **CFB:** `CONFERENCE_TIEBREAKERS` gains the American, Conference USA, Mountain West and Sun Belt procedures, the
  ACC 2026 policy and the MAC's published three-step procedure (cfbseedR 0.2.0 parity), plus `teams`
  `conf_division` / `postseason_eligible` and `tiebreaker_data` `cfp_rankings` / `apr` inputs. (#1058)
- **CFB:** `cfb_simulations(tiebreaker_data=...)` passes analytics ratings, CFP rankings and APR to every standings
  call, so the Big Ten / Big 12 / ACC / MAC analytics rungs no longer fall to a coin toss in simulation. (#1058)
