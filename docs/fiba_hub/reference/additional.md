# FIBA (digital API and game pages) — additional Python functions

> FIBA (digital API and game pages) — additional Python functions — additional functions in sdv-py, the SportsDataverse Python package.

Additional functions in `sportsdataverse.basketball.fiba_hub` that are not generated API-endpoint wrappers (those
have their own reference pages): per-competition functions minted from the competition registry
(each binds a base wrapper to one competition), plus hand-written wrappers, loaders, and helpers.

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
| [fiba_americup_editions](additional/fiba-digital-api-and-game-pages.md#fiba_americup_editions) | Every edition of FIBA AmeriCup. |
| [fiba_americup_pbp](additional/fiba-digital-api-and-game-pages-2.md#fiba_americup_pbp) | Play-by-play of one FIBA AmeriCup game. |
| [fiba_americup_player_box](additional/fiba-digital-api-and-game-pages-2.md#fiba_americup_player_box) | Player box score of one FIBA AmeriCup game. |
| [fiba_americup_schedule](additional/fiba-digital-api-and-game-pages-2.md#fiba_americup_schedule) | Schedule and results of one FIBA AmeriCup edition, one row per game. |
| [fiba_americup_shots](additional/fiba-digital-api-and-game-pages-2.md#fiba_americup_shots) | Shot chart of one FIBA AmeriCup game. |
| [fiba_americup_standings](additional/fiba-digital-api-and-game-pages-2.md#fiba_americup_standings) | Final standings of one FIBA AmeriCup edition. |
| [fiba_americup_team_box](additional/fiba-digital-api-and-game-pages-2.md#fiba_americup_team_box) | Team box score of one FIBA AmeriCup game. |
| [fiba_americup_teams](additional/fiba-digital-api-and-game-pages-2.md#fiba_americup_teams) | Teams of one FIBA AmeriCup edition. |
| [fiba_asiacup_editions](additional/fiba-digital-api-and-game-pages-2.md#fiba_asiacup_editions) | Every edition of FIBA Asia Cup. |
| [fiba_asiacup_pbp](additional/fiba-digital-api-and-game-pages-2.md#fiba_asiacup_pbp) | Play-by-play of one FIBA Asia Cup game. |
| [fiba_asiacup_player_box](additional/fiba-digital-api-and-game-pages-3.md#fiba_asiacup_player_box) | Player box score of one FIBA Asia Cup game. |
| [fiba_asiacup_schedule](additional/fiba-digital-api-and-game-pages-3.md#fiba_asiacup_schedule) | Schedule and results of one FIBA Asia Cup edition, one row per game. |
| [fiba_asiacup_shots](additional/fiba-digital-api-and-game-pages-3.md#fiba_asiacup_shots) | Shot chart of one FIBA Asia Cup game. |
| [fiba_asiacup_standings](additional/fiba-digital-api-and-game-pages-3.md#fiba_asiacup_standings) | Final standings of one FIBA Asia Cup edition. |
| [fiba_asiacup_team_box](additional/fiba-digital-api-and-game-pages-3.md#fiba_asiacup_team_box) | Team box score of one FIBA Asia Cup game. |
| [fiba_asiacup_teams](additional/fiba-digital-api-and-game-pages-3.md#fiba_asiacup_teams) | Teams of one FIBA Asia Cup edition. |
| [fiba_bcl_editions](additional/fiba-digital-api-and-game-pages-3.md#fiba_bcl_editions) | Every edition of Basketball Champions League (FIBA Men's European Club Competitions Tier 1). |
| [fiba_bcl_pbp](additional/fiba-digital-api-and-game-pages-3.md#fiba_bcl_pbp) | Play-by-play of one Basketball Champions League (FIBA Men's European Club Competitions Tier 1) game. |
| [fiba_bcl_player_box](additional/fiba-digital-api-and-game-pages-3.md#fiba_bcl_player_box) | Player box score of one Basketball Champions League (FIBA Men's European Club Competitions Tier 1) game. |
| [fiba_bcl_schedule](additional/fiba-digital-api-and-game-pages-4.md#fiba_bcl_schedule) | Schedule and results of one Basketball Champions League (FIBA Men's European Club Competitions Tier 1) edition, one row per game. |
| [fiba_bcl_shots](additional/fiba-digital-api-and-game-pages-4.md#fiba_bcl_shots) | Shot chart of one Basketball Champions League (FIBA Men's European Club Competitions Tier 1) game. |
| [fiba_bcl_standings](additional/fiba-digital-api-and-game-pages-4.md#fiba_bcl_standings) | Final standings of one Basketball Champions League (FIBA Men's European Club Competitions Tier 1) edition. |
| [fiba_bcl_team_box](additional/fiba-digital-api-and-game-pages-4.md#fiba_bcl_team_box) | Team box score of one Basketball Champions League (FIBA Men's European Club Competitions Tier 1) game. |
| [fiba_bcl_teams](additional/fiba-digital-api-and-game-pages-4.md#fiba_bcl_teams) | Teams of one Basketball Champions League (FIBA Men's European Club Competitions Tier 1) edition. |
| [fiba_bcla_editions](additional/fiba-digital-api-and-game-pages-4.md#fiba_bcla_editions) | Every edition of Basketball Champions League Americas. |
| [fiba_bcla_pbp](additional/fiba-digital-api-and-game-pages-4.md#fiba_bcla_pbp) | Play-by-play of one Basketball Champions League Americas game. |
| [fiba_bcla_player_box](additional/fiba-digital-api-and-game-pages-4.md#fiba_bcla_player_box) | Player box score of one Basketball Champions League Americas game. |
| [fiba_bcla_schedule](additional/fiba-digital-api-and-game-pages-5.md#fiba_bcla_schedule) | Schedule and results of one Basketball Champions League Americas edition, one row per game. |
| [fiba_bcla_shots](additional/fiba-digital-api-and-game-pages-5.md#fiba_bcla_shots) | Shot chart of one Basketball Champions League Americas game. |
| [fiba_bcla_standings](additional/fiba-digital-api-and-game-pages-5.md#fiba_bcla_standings) | Final standings of one Basketball Champions League Americas edition. |
| [fiba_bcla_team_box](additional/fiba-digital-api-and-game-pages-5.md#fiba_bcla_team_box) | Team box score of one Basketball Champions League Americas game. |
| [fiba_bcla_teams](additional/fiba-digital-api-and-game-pages-5.md#fiba_bcla_teams) | Teams of one Basketball Champions League Americas edition. |
| [fiba_ecw_editions](additional/fiba-digital-api-and-game-pages-5.md#fiba_ecw_editions) | Every edition of EuroCup Women (FIBA Women's European Club Competitions Tier 2). |
| [fiba_ecw_pbp](additional/fiba-digital-api-and-game-pages-5.md#fiba_ecw_pbp) | Play-by-play of one EuroCup Women (FIBA Women's European Club Competitions Tier 2) game. |
| [fiba_ecw_player_box](additional/fiba-digital-api-and-game-pages-5.md#fiba_ecw_player_box) | Player box score of one EuroCup Women (FIBA Women's European Club Competitions Tier 2) game. |
| [fiba_ecw_schedule](additional/fiba-digital-api-and-game-pages-6.md#fiba_ecw_schedule) | Schedule and results of one EuroCup Women (FIBA Women's European Club Competitions Tier 2) edition, one row per game. |
| [fiba_ecw_shots](additional/fiba-digital-api-and-game-pages-6.md#fiba_ecw_shots) | Shot chart of one EuroCup Women (FIBA Women's European Club Competitions Tier 2) game. |
| [fiba_ecw_standings](additional/fiba-digital-api-and-game-pages-6.md#fiba_ecw_standings) | Final standings of one EuroCup Women (FIBA Women's European Club Competitions Tier 2) edition. |
| [fiba_ecw_team_box](additional/fiba-digital-api-and-game-pages-6.md#fiba_ecw_team_box) | Team box score of one EuroCup Women (FIBA Women's European Club Competitions Tier 2) game. |
| [fiba_ecw_teams](additional/fiba-digital-api-and-game-pages-6.md#fiba_ecw_teams) | Teams of one EuroCup Women (FIBA Women's European Club Competitions Tier 2) edition. |
| [fiba_elw_editions](additional/fiba-digital-api-and-game-pages-6.md#fiba_elw_editions) | Every edition of EuroLeague Women (FIBA Women's European Club Competitions Tier 1). |
| [fiba_elw_pbp](additional/fiba-digital-api-and-game-pages-6.md#fiba_elw_pbp) | Play-by-play of one EuroLeague Women (FIBA Women's European Club Competitions Tier 1) game. |
| [fiba_elw_player_box](additional/fiba-digital-api-and-game-pages-6.md#fiba_elw_player_box) | Player box score of one EuroLeague Women (FIBA Women's European Club Competitions Tier 1) game. |
| [fiba_elw_schedule](additional/fiba-digital-api-and-game-pages-7.md#fiba_elw_schedule) | Schedule and results of one EuroLeague Women (FIBA Women's European Club Competitions Tier 1) edition, one row per game. |
| [fiba_elw_shots](additional/fiba-digital-api-and-game-pages-7.md#fiba_elw_shots) | Shot chart of one EuroLeague Women (FIBA Women's European Club Competitions Tier 1) game. |
| [fiba_elw_standings](additional/fiba-digital-api-and-game-pages-7.md#fiba_elw_standings) | Final standings of one EuroLeague Women (FIBA Women's European Club Competitions Tier 1) edition. |
| [fiba_elw_team_box](additional/fiba-digital-api-and-game-pages-7.md#fiba_elw_team_box) | Team box score of one EuroLeague Women (FIBA Women's European Club Competitions Tier 1) game. |
| [fiba_elw_teams](additional/fiba-digital-api-and-game-pages-7.md#fiba_elw_teams) | Teams of one EuroLeague Women (FIBA Women's European Club Competitions Tier 1) edition. |
| [fiba_eurobasket_editions](additional/fiba-digital-api-and-game-pages-7.md#fiba_eurobasket_editions) | Every edition of FIBA EuroBasket. |
| [fiba_eurobasket_pbp](additional/fiba-digital-api-and-game-pages-7.md#fiba_eurobasket_pbp) | Play-by-play of one FIBA EuroBasket game. |
| [fiba_eurobasket_player_box](additional/fiba-digital-api-and-game-pages-7.md#fiba_eurobasket_player_box) | Player box score of one FIBA EuroBasket game. |
| [fiba_eurobasket_schedule](additional/fiba-digital-api-and-game-pages-8.md#fiba_eurobasket_schedule) | Schedule and results of one FIBA EuroBasket edition, one row per game. |
| [fiba_eurobasket_shots](additional/fiba-digital-api-and-game-pages-8.md#fiba_eurobasket_shots) | Shot chart of one FIBA EuroBasket game. |
| [fiba_eurobasket_standings](additional/fiba-digital-api-and-game-pages-8.md#fiba_eurobasket_standings) | Final standings of one FIBA EuroBasket edition. |
| [fiba_eurobasket_team_box](additional/fiba-digital-api-and-game-pages-8.md#fiba_eurobasket_team_box) | Team box score of one FIBA EuroBasket game. |
| [fiba_eurobasket_teams](additional/fiba-digital-api-and-game-pages-8.md#fiba_eurobasket_teams) | Teams of one FIBA EuroBasket edition. |
| [fiba_icup_editions](additional/fiba-digital-api-and-game-pages-8.md#fiba_icup_editions) | Every edition of FIBA Intercontinental Cup. |
| [fiba_icup_pbp](additional/fiba-digital-api-and-game-pages-8.md#fiba_icup_pbp) | Play-by-play of one FIBA Intercontinental Cup game. |
| [fiba_icup_player_box](additional/fiba-digital-api-and-game-pages-8.md#fiba_icup_player_box) | Player box score of one FIBA Intercontinental Cup game. |
| [fiba_icup_schedule](additional/fiba-digital-api-and-game-pages-9.md#fiba_icup_schedule) | Schedule and results of one FIBA Intercontinental Cup edition, one row per game. |
| [fiba_icup_shots](additional/fiba-digital-api-and-game-pages-9.md#fiba_icup_shots) | Shot chart of one FIBA Intercontinental Cup game. |
| [fiba_icup_standings](additional/fiba-digital-api-and-game-pages-9.md#fiba_icup_standings) | Final standings of one FIBA Intercontinental Cup edition. |
| [fiba_icup_team_box](additional/fiba-digital-api-and-game-pages-9.md#fiba_icup_team_box) | Team box score of one FIBA Intercontinental Cup game. |
| [fiba_icup_teams](additional/fiba-digital-api-and-game-pages-9.md#fiba_icup_teams) | Teams of one FIBA Intercontinental Cup edition. |
| [fiba_oly_editions](additional/fiba-digital-api-and-game-pages-9.md#fiba_oly_editions) | Every edition of Men's Olympic Basketball Tournament. |
| [fiba_oly_pbp](additional/fiba-digital-api-and-game-pages-9.md#fiba_oly_pbp) | Play-by-play of one Men's Olympic Basketball Tournament game. |
| [fiba_oly_player_box](additional/fiba-digital-api-and-game-pages-9.md#fiba_oly_player_box) | Player box score of one Men's Olympic Basketball Tournament game. |
| [fiba_oly_schedule](additional/fiba-digital-api-and-game-pages-10.md#fiba_oly_schedule) | Schedule and results of one Men's Olympic Basketball Tournament edition, one row per game. |
| [fiba_oly_shots](additional/fiba-digital-api-and-game-pages-10.md#fiba_oly_shots) | Shot chart of one Men's Olympic Basketball Tournament game. |
| [fiba_oly_standings](additional/fiba-digital-api-and-game-pages-10.md#fiba_oly_standings) | Final standings of one Men's Olympic Basketball Tournament edition. |
| [fiba_oly_team_box](additional/fiba-digital-api-and-game-pages-10.md#fiba_oly_team_box) | Team box score of one Men's Olympic Basketball Tournament game. |
| [fiba_oly_teams](additional/fiba-digital-api-and-game-pages-10.md#fiba_oly_teams) | Teams of one Men's Olympic Basketball Tournament edition. |
| [fiba_wafrobasket_editions](additional/fiba-digital-api-and-game-pages-10.md#fiba_wafrobasket_editions) | Every edition of FIBA Women's AfroBasket. |
| [fiba_wafrobasket_pbp](additional/fiba-digital-api-and-game-pages-10.md#fiba_wafrobasket_pbp) | Play-by-play of one FIBA Women's AfroBasket game. |
| [fiba_wafrobasket_player_box](additional/fiba-digital-api-and-game-pages-10.md#fiba_wafrobasket_player_box) | Player box score of one FIBA Women's AfroBasket game. |
| [fiba_wafrobasket_schedule](additional/fiba-digital-api-and-game-pages-11.md#fiba_wafrobasket_schedule) | Schedule and results of one FIBA Women's AfroBasket edition, one row per game. |
| [fiba_wafrobasket_shots](additional/fiba-digital-api-and-game-pages-11.md#fiba_wafrobasket_shots) | Shot chart of one FIBA Women's AfroBasket game. |
| [fiba_wafrobasket_standings](additional/fiba-digital-api-and-game-pages-11.md#fiba_wafrobasket_standings) | Final standings of one FIBA Women's AfroBasket edition. |
| [fiba_wafrobasket_team_box](additional/fiba-digital-api-and-game-pages-11.md#fiba_wafrobasket_team_box) | Team box score of one FIBA Women's AfroBasket game. |
| [fiba_wafrobasket_teams](additional/fiba-digital-api-and-game-pages-11.md#fiba_wafrobasket_teams) | Teams of one FIBA Women's AfroBasket edition. |
| [fiba_wamericup_editions](additional/fiba-digital-api-and-game-pages-11.md#fiba_wamericup_editions) | Every edition of FIBA Women's AmeriCup. |
| [fiba_wamericup_pbp](additional/fiba-digital-api-and-game-pages-11.md#fiba_wamericup_pbp) | Play-by-play of one FIBA Women's AmeriCup game. |
| [fiba_wamericup_player_box](additional/fiba-digital-api-and-game-pages-11.md#fiba_wamericup_player_box) | Player box score of one FIBA Women's AmeriCup game. |
| [fiba_wamericup_schedule](additional/fiba-digital-api-and-game-pages-12.md#fiba_wamericup_schedule) | Schedule and results of one FIBA Women's AmeriCup edition, one row per game. |
| [fiba_wamericup_shots](additional/fiba-digital-api-and-game-pages-12.md#fiba_wamericup_shots) | Shot chart of one FIBA Women's AmeriCup game. |
| [fiba_wamericup_standings](additional/fiba-digital-api-and-game-pages-12.md#fiba_wamericup_standings) | Final standings of one FIBA Women's AmeriCup edition. |
| [fiba_wamericup_team_box](additional/fiba-digital-api-and-game-pages-12.md#fiba_wamericup_team_box) | Team box score of one FIBA Women's AmeriCup game. |
| [fiba_wamericup_teams](additional/fiba-digital-api-and-game-pages-12.md#fiba_wamericup_teams) | Teams of one FIBA Women's AmeriCup edition. |
| [fiba_wasiacup_editions](additional/fiba-digital-api-and-game-pages-12.md#fiba_wasiacup_editions) | Every edition of FIBA Women's Asia Cup (Division A). |
| [fiba_wasiacup_pbp](additional/fiba-digital-api-and-game-pages-12.md#fiba_wasiacup_pbp) | Play-by-play of one FIBA Women's Asia Cup (Division A) game. |
| [fiba_wasiacup_player_box](additional/fiba-digital-api-and-game-pages-12.md#fiba_wasiacup_player_box) | Player box score of one FIBA Women's Asia Cup (Division A) game. |
| [fiba_wasiacup_schedule](additional/fiba-digital-api-and-game-pages-13.md#fiba_wasiacup_schedule) | Schedule and results of one FIBA Women's Asia Cup (Division A) edition, one row per game. |
| [fiba_wasiacup_shots](additional/fiba-digital-api-and-game-pages-13.md#fiba_wasiacup_shots) | Shot chart of one FIBA Women's Asia Cup (Division A) game. |
| [fiba_wasiacup_standings](additional/fiba-digital-api-and-game-pages-13.md#fiba_wasiacup_standings) | Final standings of one FIBA Women's Asia Cup (Division A) edition. |
| [fiba_wasiacup_team_box](additional/fiba-digital-api-and-game-pages-13.md#fiba_wasiacup_team_box) | Team box score of one FIBA Women's Asia Cup (Division A) game. |
| [fiba_wasiacup_teams](additional/fiba-digital-api-and-game-pages-13.md#fiba_wasiacup_teams) | Teams of one FIBA Women's Asia Cup (Division A) edition. |
| [fiba_wc_editions](additional/fiba-digital-api-and-game-pages-13.md#fiba_wc_editions) | Every edition of FIBA Basketball World Cup. |
| [fiba_wc_pbp](additional/fiba-digital-api-and-game-pages-13.md#fiba_wc_pbp) | Play-by-play of one FIBA Basketball World Cup game. |
| [fiba_wc_player_box](additional/fiba-digital-api-and-game-pages-13.md#fiba_wc_player_box) | Player box score of one FIBA Basketball World Cup game. |
| [fiba_wc_schedule](additional/fiba-digital-api-and-game-pages-14.md#fiba_wc_schedule) | Schedule and results of one FIBA Basketball World Cup edition, one row per game. |
| [fiba_wc_shots](additional/fiba-digital-api-and-game-pages-14.md#fiba_wc_shots) | Shot chart of one FIBA Basketball World Cup game. |
| [fiba_wc_standings](additional/fiba-digital-api-and-game-pages-14.md#fiba_wc_standings) | Final standings of one FIBA Basketball World Cup edition. |
| [fiba_wc_team_box](additional/fiba-digital-api-and-game-pages-14.md#fiba_wc_team_box) | Team box score of one FIBA Basketball World Cup game. |
| [fiba_wc_teams](additional/fiba-digital-api-and-game-pages-14.md#fiba_wc_teams) | Teams of one FIBA Basketball World Cup edition. |
| [fiba_weurobasket_editions](additional/fiba-digital-api-and-game-pages-14.md#fiba_weurobasket_editions) | Every edition of FIBA Women's EuroBasket. |
| [fiba_weurobasket_pbp](additional/fiba-digital-api-and-game-pages-14.md#fiba_weurobasket_pbp) | Play-by-play of one FIBA Women's EuroBasket game. |
| [fiba_weurobasket_player_box](additional/fiba-digital-api-and-game-pages-14.md#fiba_weurobasket_player_box) | Player box score of one FIBA Women's EuroBasket game. |
| [fiba_weurobasket_schedule](additional/fiba-digital-api-and-game-pages-15.md#fiba_weurobasket_schedule) | Schedule and results of one FIBA Women's EuroBasket edition, one row per game. |
| [fiba_weurobasket_shots](additional/fiba-digital-api-and-game-pages-15.md#fiba_weurobasket_shots) | Shot chart of one FIBA Women's EuroBasket game. |
| [fiba_weurobasket_standings](additional/fiba-digital-api-and-game-pages-15.md#fiba_weurobasket_standings) | Final standings of one FIBA Women's EuroBasket edition. |
| [fiba_weurobasket_team_box](additional/fiba-digital-api-and-game-pages-15.md#fiba_weurobasket_team_box) | Team box score of one FIBA Women's EuroBasket game. |
| [fiba_weurobasket_teams](additional/fiba-digital-api-and-game-pages-15.md#fiba_weurobasket_teams) | Teams of one FIBA Women's EuroBasket edition. |
| [fiba_woly_editions](additional/fiba-digital-api-and-game-pages-15.md#fiba_woly_editions) | Every edition of Women's Olympic Basketball Tournament. |
| [fiba_woly_pbp](additional/fiba-digital-api-and-game-pages-15.md#fiba_woly_pbp) | Play-by-play of one Women's Olympic Basketball Tournament game. |
| [fiba_woly_player_box](additional/fiba-digital-api-and-game-pages-15.md#fiba_woly_player_box) | Player box score of one Women's Olympic Basketball Tournament game. |
| [fiba_woly_schedule](additional/fiba-digital-api-and-game-pages-16.md#fiba_woly_schedule) | Schedule and results of one Women's Olympic Basketball Tournament edition, one row per game. |
| [fiba_woly_shots](additional/fiba-digital-api-and-game-pages-16.md#fiba_woly_shots) | Shot chart of one Women's Olympic Basketball Tournament game. |
| [fiba_woly_standings](additional/fiba-digital-api-and-game-pages-16.md#fiba_woly_standings) | Final standings of one Women's Olympic Basketball Tournament edition. |
| [fiba_woly_team_box](additional/fiba-digital-api-and-game-pages-16.md#fiba_woly_team_box) | Team box score of one Women's Olympic Basketball Tournament game. |
| [fiba_woly_teams](additional/fiba-digital-api-and-game-pages-16.md#fiba_woly_teams) | Teams of one Women's Olympic Basketball Tournament edition. |
| [fiba_wwc_editions](additional/fiba-digital-api-and-game-pages-16.md#fiba_wwc_editions) | Every edition of FIBA Women's Basketball World Cup. |
| [fiba_wwc_pbp](additional/fiba-digital-api-and-game-pages-16.md#fiba_wwc_pbp) | Play-by-play of one FIBA Women's Basketball World Cup game. |
| [fiba_wwc_player_box](additional/fiba-digital-api-and-game-pages-16.md#fiba_wwc_player_box) | Player box score of one FIBA Women's Basketball World Cup game. |
| [fiba_wwc_schedule](additional/fiba-digital-api-and-game-pages-17.md#fiba_wwc_schedule) | Schedule and results of one FIBA Women's Basketball World Cup edition, one row per game. |
| [fiba_wwc_shots](additional/fiba-digital-api-and-game-pages-17.md#fiba_wwc_shots) | Shot chart of one FIBA Women's Basketball World Cup game. |
| [fiba_wwc_standings](additional/fiba-digital-api-and-game-pages-17.md#fiba_wwc_standings) | Final standings of one FIBA Women's Basketball World Cup edition. |
| [fiba_wwc_team_box](additional/fiba-digital-api-and-game-pages-17.md#fiba_wwc_team_box) | Team box score of one FIBA Women's Basketball World Cup game. |
| [fiba_wwc_teams](additional/fiba-digital-api-and-game-pages-17.md#fiba_wwc_teams) | Teams of one FIBA Women's Basketball World Cup edition. |
