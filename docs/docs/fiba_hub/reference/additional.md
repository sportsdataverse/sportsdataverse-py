---
title: FIBA (digital API and game pages) — additional Python functions
sidebar_label: Additional functions
description: "FIBA (digital API and game pages) — additional Python functions — additional functions in sdv-py, the SportsDataverse Python package."
sidebar_position: 50
---
# FIBA (digital API and game pages) — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.basketball.fiba_hub`
not covered by the generated API-endpoint reference above.

## FIBA digital API and game pages

| Function | Summary |
|---|---|
| [fiba_afrobasket_editions](additional/fiba-digital-api-and-game-pages.md#fiba_afrobasket_editions) | Every edition of FIBA AfroBasket. |
| [fiba_afrobasket_pbp](additional/fiba-digital-api-and-game-pages.md#fiba_afrobasket_pbp) | Play-by-play of one FIBA AfroBasket game. |
| [fiba_afrobasket_player_box](additional/fiba-digital-api-and-game-pages.md#fiba_afrobasket_player_box) | Player box score of one FIBA AfroBasket game. |
| [fiba_afrobasket_schedule](additional/fiba-digital-api-and-game-pages.md#fiba_afrobasket_schedule) | Schedule and results of one FIBA AfroBasket edition, one row per game. |
| [fiba_afrobasket_shots](additional/fiba-digital-api-and-game-pages.md#fiba_afrobasket_shots) | Shot chart of one FIBA AfroBasket game. |
| [fiba_afrobasket_standings](additional/fiba-digital-api-and-game-pages.md#fiba_afrobasket_standings) | Final standings of one FIBA AfroBasket edition. |
| [fiba_afrobasket_team_box](additional/fiba-digital-api-and-game-pages.md#fiba_afrobasket_team_box) | Team box score of one FIBA AfroBasket game. |
| [fiba_afrobasket_teams](additional/fiba-digital-api-and-game-pages.md#fiba_afrobasket_teams) | Teams of one FIBA AfroBasket edition. |
| [fiba_bcl_editions](additional/fiba-digital-api-and-game-pages.md#fiba_bcl_editions) | Every edition of Basketball Champions League (FIBA Men's European Club Competitions Tier 1). |
| [fiba_bcl_pbp](additional/fiba-digital-api-and-game-pages.md#fiba_bcl_pbp) | Play-by-play of one Basketball Champions League (FIBA Men's European Club Competitions Tier 1) game. |
| [fiba_bcl_player_box](additional/fiba-digital-api-and-game-pages.md#fiba_bcl_player_box) | Player box score of one Basketball Champions League (FIBA Men's European Club Competitions Tier 1) game. |
| [fiba_bcl_schedule](additional/fiba-digital-api-and-game-pages.md#fiba_bcl_schedule) | Schedule and results of one Basketball Champions League (FIBA Men's European Club Competitions Tier 1) edition, one row per game. |
| [fiba_bcl_shots](additional/fiba-digital-api-and-game-pages.md#fiba_bcl_shots) | Shot chart of one Basketball Champions League (FIBA Men's European Club Competitions Tier 1) game. |
| [fiba_bcl_standings](additional/fiba-digital-api-and-game-pages.md#fiba_bcl_standings) | Final standings of one Basketball Champions League (FIBA Men's European Club Competitions Tier 1) edition. |
| [fiba_bcl_team_box](additional/fiba-digital-api-and-game-pages.md#fiba_bcl_team_box) | Team box score of one Basketball Champions League (FIBA Men's European Club Competitions Tier 1) game. |
| [fiba_bcl_teams](additional/fiba-digital-api-and-game-pages.md#fiba_bcl_teams) | Teams of one Basketball Champions League (FIBA Men's European Club Competitions Tier 1) edition. |
| [fiba_elw_editions](additional/fiba-digital-api-and-game-pages.md#fiba_elw_editions) | Every edition of EuroLeague Women (FIBA Women's European Club Competitions Tier 1). |
| [fiba_elw_pbp](additional/fiba-digital-api-and-game-pages.md#fiba_elw_pbp) | Play-by-play of one EuroLeague Women (FIBA Women's European Club Competitions Tier 1) game. |
| [fiba_elw_player_box](additional/fiba-digital-api-and-game-pages.md#fiba_elw_player_box) | Player box score of one EuroLeague Women (FIBA Women's European Club Competitions Tier 1) game. |
| [fiba_elw_schedule](additional/fiba-digital-api-and-game-pages.md#fiba_elw_schedule) | Schedule and results of one EuroLeague Women (FIBA Women's European Club Competitions Tier 1) edition, one row per game. |
| [fiba_elw_shots](additional/fiba-digital-api-and-game-pages.md#fiba_elw_shots) | Shot chart of one EuroLeague Women (FIBA Women's European Club Competitions Tier 1) game. |
| [fiba_elw_standings](additional/fiba-digital-api-and-game-pages.md#fiba_elw_standings) | Final standings of one EuroLeague Women (FIBA Women's European Club Competitions Tier 1) edition. |
| [fiba_elw_team_box](additional/fiba-digital-api-and-game-pages.md#fiba_elw_team_box) | Team box score of one EuroLeague Women (FIBA Women's European Club Competitions Tier 1) game. |
| [fiba_elw_teams](additional/fiba-digital-api-and-game-pages.md#fiba_elw_teams) | Teams of one EuroLeague Women (FIBA Women's European Club Competitions Tier 1) edition. |
| [fiba_oly_editions](additional/fiba-digital-api-and-game-pages.md#fiba_oly_editions) | Every edition of Men's Olympic Basketball Tournament. |
| [fiba_oly_pbp](additional/fiba-digital-api-and-game-pages.md#fiba_oly_pbp) | Play-by-play of one Men's Olympic Basketball Tournament game. |
| [fiba_oly_player_box](additional/fiba-digital-api-and-game-pages.md#fiba_oly_player_box) | Player box score of one Men's Olympic Basketball Tournament game. |
| [fiba_oly_schedule](additional/fiba-digital-api-and-game-pages.md#fiba_oly_schedule) | Schedule and results of one Men's Olympic Basketball Tournament edition, one row per game. |
| [fiba_oly_shots](additional/fiba-digital-api-and-game-pages.md#fiba_oly_shots) | Shot chart of one Men's Olympic Basketball Tournament game. |
| [fiba_oly_standings](additional/fiba-digital-api-and-game-pages.md#fiba_oly_standings) | Final standings of one Men's Olympic Basketball Tournament edition. |
| [fiba_oly_team_box](additional/fiba-digital-api-and-game-pages.md#fiba_oly_team_box) | Team box score of one Men's Olympic Basketball Tournament game. |
| [fiba_oly_teams](additional/fiba-digital-api-and-game-pages.md#fiba_oly_teams) | Teams of one Men's Olympic Basketball Tournament edition. |
| [fiba_wc_editions](additional/fiba-digital-api-and-game-pages.md#fiba_wc_editions) | Every edition of FIBA Basketball World Cup. |
| [fiba_wc_pbp](additional/fiba-digital-api-and-game-pages.md#fiba_wc_pbp) | Play-by-play of one FIBA Basketball World Cup game. |
| [fiba_wc_player_box](additional/fiba-digital-api-and-game-pages.md#fiba_wc_player_box) | Player box score of one FIBA Basketball World Cup game. |
| [fiba_wc_schedule](additional/fiba-digital-api-and-game-pages.md#fiba_wc_schedule) | Schedule and results of one FIBA Basketball World Cup edition, one row per game. |
| [fiba_wc_shots](additional/fiba-digital-api-and-game-pages.md#fiba_wc_shots) | Shot chart of one FIBA Basketball World Cup game. |
| [fiba_wc_standings](additional/fiba-digital-api-and-game-pages.md#fiba_wc_standings) | Final standings of one FIBA Basketball World Cup edition. |
| [fiba_wc_team_box](additional/fiba-digital-api-and-game-pages.md#fiba_wc_team_box) | Team box score of one FIBA Basketball World Cup game. |
| [fiba_wc_teams](additional/fiba-digital-api-and-game-pages.md#fiba_wc_teams) | Teams of one FIBA Basketball World Cup edition. |
| [fiba_weurobasket_editions](additional/fiba-digital-api-and-game-pages.md#fiba_weurobasket_editions) | Every edition of FIBA Women's EuroBasket. |
| [fiba_weurobasket_pbp](additional/fiba-digital-api-and-game-pages.md#fiba_weurobasket_pbp) | Play-by-play of one FIBA Women's EuroBasket game. |
| [fiba_weurobasket_player_box](additional/fiba-digital-api-and-game-pages.md#fiba_weurobasket_player_box) | Player box score of one FIBA Women's EuroBasket game. |
| [fiba_weurobasket_schedule](additional/fiba-digital-api-and-game-pages.md#fiba_weurobasket_schedule) | Schedule and results of one FIBA Women's EuroBasket edition, one row per game. |
| [fiba_weurobasket_shots](additional/fiba-digital-api-and-game-pages.md#fiba_weurobasket_shots) | Shot chart of one FIBA Women's EuroBasket game. |
| [fiba_weurobasket_standings](additional/fiba-digital-api-and-game-pages.md#fiba_weurobasket_standings) | Final standings of one FIBA Women's EuroBasket edition. |
| [fiba_weurobasket_team_box](additional/fiba-digital-api-and-game-pages.md#fiba_weurobasket_team_box) | Team box score of one FIBA Women's EuroBasket game. |
| [fiba_weurobasket_teams](additional/fiba-digital-api-and-game-pages-2.md#fiba_weurobasket_teams) | Teams of one FIBA Women's EuroBasket edition. |
| [fiba_woly_editions](additional/fiba-digital-api-and-game-pages-2.md#fiba_woly_editions) | Every edition of Women's Olympic Basketball Tournament. |
| [fiba_woly_pbp](additional/fiba-digital-api-and-game-pages-2.md#fiba_woly_pbp) | Play-by-play of one Women's Olympic Basketball Tournament game. |
| [fiba_woly_player_box](additional/fiba-digital-api-and-game-pages-2.md#fiba_woly_player_box) | Player box score of one Women's Olympic Basketball Tournament game. |
| [fiba_woly_schedule](additional/fiba-digital-api-and-game-pages-2.md#fiba_woly_schedule) | Schedule and results of one Women's Olympic Basketball Tournament edition, one row per game. |
| [fiba_woly_shots](additional/fiba-digital-api-and-game-pages-2.md#fiba_woly_shots) | Shot chart of one Women's Olympic Basketball Tournament game. |
| [fiba_woly_standings](additional/fiba-digital-api-and-game-pages-2.md#fiba_woly_standings) | Final standings of one Women's Olympic Basketball Tournament edition. |
| [fiba_woly_team_box](additional/fiba-digital-api-and-game-pages-2.md#fiba_woly_team_box) | Team box score of one Women's Olympic Basketball Tournament game. |
| [fiba_woly_teams](additional/fiba-digital-api-and-game-pages-2.md#fiba_woly_teams) | Teams of one Women's Olympic Basketball Tournament edition. |
| [fiba_wwc_editions](additional/fiba-digital-api-and-game-pages-2.md#fiba_wwc_editions) | Every edition of FIBA Women's Basketball World Cup. |
| [fiba_wwc_pbp](additional/fiba-digital-api-and-game-pages-2.md#fiba_wwc_pbp) | Play-by-play of one FIBA Women's Basketball World Cup game. |
| [fiba_wwc_player_box](additional/fiba-digital-api-and-game-pages-2.md#fiba_wwc_player_box) | Player box score of one FIBA Women's Basketball World Cup game. |
| [fiba_wwc_schedule](additional/fiba-digital-api-and-game-pages-2.md#fiba_wwc_schedule) | Schedule and results of one FIBA Women's Basketball World Cup edition, one row per game. |
| [fiba_wwc_shots](additional/fiba-digital-api-and-game-pages-2.md#fiba_wwc_shots) | Shot chart of one FIBA Women's Basketball World Cup game. |
| [fiba_wwc_standings](additional/fiba-digital-api-and-game-pages-2.md#fiba_wwc_standings) | Final standings of one FIBA Women's Basketball World Cup edition. |
| [fiba_wwc_team_box](additional/fiba-digital-api-and-game-pages-2.md#fiba_wwc_team_box) | Team box score of one FIBA Women's Basketball World Cup game. |
| [fiba_wwc_teams](additional/fiba-digital-api-and-game-pages-2.md#fiba_wwc_teams) | Teams of one FIBA Women's Basketball World Cup edition. |
