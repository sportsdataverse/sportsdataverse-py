- **ESPN international basketball wrappers:** `leagues.yaml` registers ten more ESPN basketball leagues, so codegen
  emits the universal `espn_<prefix>_*` family for each: `nbl` (Australian NBL), `fiba` (FIBA events, e.g. the
  2026 Women's World Cup), `olympics_mbk` / `olympics_wbk` (Olympic men's / women's basketball), and the NBA Summer
  League sites `nbasl_lv`, `nbasl_ut`, `nbasl_ca`, `nbasl_sac`, `nbasl_orl`, `nbasl_gs`. Every slug was checked against
  ESPN's live teams and scoreboard endpoints. ESPN's `nbl` carries box scores but no play-by-play for 2025-26 (the
  NBL moved stats provider); that gap is filled by the NBL Rosetta family, not by these wrappers.
