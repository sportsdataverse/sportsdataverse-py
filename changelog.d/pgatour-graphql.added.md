- **PGA TOUR GraphQL:** `sportsdataverse.golf` adds ten runtime-only `pgatour_*` wrappers over the PGA TOUR's own
  API: schedule, leaderboard, scorecards, tee times, players, stat catalog and stat tables, FedExCup standings and
  ShotLink shots, each parsed to polars. The public site key is read at runtime (`PGA_API_KEY` overrides). (#1289)
