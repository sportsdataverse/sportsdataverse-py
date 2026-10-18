---
title: GENIUS — additional Python functions
sidebar_label: Additional functions
description: "GENIUS — additional Python functions — additional functions in sdv-py, the SportsDataverse Python package."
sidebar_position: 50
---
# GENIUS — additional Python functions

Additional functions in `sportsdataverse.basketball.genius` not covered by the generated API-endpoint
reference above: per-competition functions minted from the competition registry (each
binds a base wrapper to one competition), plus hand-written wrappers, loaders, and helpers.

## Genius Sports LiveStats

| Function | Summary |
|---|---|
| [bal_competitions](additional/genius-sports-livestats.md#bal_competitions) | Every competition of the BAL Genius org, with its END-year season. |
| [bal_pbp](additional/genius-sports-livestats.md#bal_pbp) | Play-by-play of a finished Basketball Africa League game. |
| [bal_player_box](additional/genius-sports-livestats.md#bal_player_box) | Player box score of a finished Basketball Africa League game. |
| [bal_schedule](additional/genius-sports-livestats.md#bal_schedule) | Every match of one Basketball Africa League season (hosted WebStats): ids, status, venue, teams and scores. |
| [bal_shots](additional/genius-sports-livestats.md#bal_shots) | Shot chart of a finished Basketball Africa League game. |
| [bal_team_box](additional/genius-sports-livestats.md#bal_team_box) | Team box score of a finished Basketball Africa League game. |
| [esake_competitions](additional/genius-sports-livestats.md#esake_competitions) | Every competition of the ESAKE Genius org, with its END-year season. |
| [esake_pbp](additional/genius-sports-livestats.md#esake_pbp) | Play-by-play of a finished Greek Basket League (ESAKE) game. |
| [esake_player_box](additional/genius-sports-livestats.md#esake_player_box) | Player box score of a finished Greek Basket League (ESAKE) game. |
| [esake_schedule](additional/genius-sports-livestats.md#esake_schedule) | Every match of one Greek Basket League (ESAKE) season (hosted WebStats): ids, status, venue, teams and scores. |
| [esake_shots](additional/genius-sports-livestats.md#esake_shots) | Shot chart of a finished Greek Basket League (ESAKE) game. |
| [esake_team_box](additional/genius-sports-livestats-2.md#esake_team_box) | Team box score of a finished Greek Basket League (ESAKE) game. |
| [lfb_competitions](additional/genius-sports-livestats-2.md#lfb_competitions) | Every competition of the FFBB Genius org, with its END-year season. |
| [lfb_pbp](additional/genius-sports-livestats-2.md#lfb_pbp) | Play-by-play of a finished Ligue Feminine de Basket (France, FFBB) game. |
| [lfb_player_box](additional/genius-sports-livestats-2.md#lfb_player_box) | Player box score of a finished Ligue Feminine de Basket (France, FFBB) game. |
| [lfb_schedule](additional/genius-sports-livestats-2.md#lfb_schedule) | Every match of one Ligue Feminine de Basket (France, FFBB) season (hosted WebStats): ids, status, venue, teams and scores. |
| [lfb_shots](additional/genius-sports-livestats-2.md#lfb_shots) | Shot chart of a finished Ligue Feminine de Basket (France, FFBB) game. |
| [lfb_team_box](additional/genius-sports-livestats-2.md#lfb_team_box) | Team box score of a finished Ligue Feminine de Basket (France, FFBB) game. |
| [lnbm_competitions](additional/genius-sports-livestats-2.md#lnbm_competitions) | Every competition of the FRB Genius org, with its END-year season. |
| [lnbm_pbp](additional/genius-sports-livestats-2.md#lnbm_pbp) | Play-by-play of a finished Liga Nationala de Baschet Masculin (Romania) game. |
| [lnbm_player_box](additional/genius-sports-livestats-3.md#lnbm_player_box) | Player box score of a finished Liga Nationala de Baschet Masculin (Romania) game. |
| [lnbm_schedule](additional/genius-sports-livestats-3.md#lnbm_schedule) | Every match of one Liga Nationala de Baschet Masculin (Romania) season (hosted WebStats): ids, status, venue, teams and scores. |
| [lnbm_shots](additional/genius-sports-livestats-3.md#lnbm_shots) | Shot chart of a finished Liga Nationala de Baschet Masculin (Romania) game. |
| [lnbm_team_box](additional/genius-sports-livestats-3.md#lnbm_team_box) | Team box score of a finished Liga Nationala de Baschet Masculin (Romania) game. |
| [lub_competitions](additional/genius-sports-livestats-3.md#lub_competitions) | Every competition of the FUBB Genius org, with its END-year season. |
| [lub_pbp](additional/genius-sports-livestats-3.md#lub_pbp) | Play-by-play of a finished Liga Uruguaya de Basquetbol game. |
| [lub_player_box](additional/genius-sports-livestats-3.md#lub_player_box) | Player box score of a finished Liga Uruguaya de Basquetbol game. |
| [lub_schedule](additional/genius-sports-livestats-3.md#lub_schedule) | Every match of one Liga Uruguaya de Basquetbol season (hosted WebStats): ids, status, venue, teams and scores. |
| [lub_shots](additional/genius-sports-livestats-3.md#lub_shots) | Shot chart of a finished Liga Uruguaya de Basquetbol game. |
| [lub_team_box](additional/genius-sports-livestats-4.md#lub_team_box) | Team box score of a finished Liga Uruguaya de Basquetbol game. |
| [nznbl_competitions](additional/genius-sports-livestats-4.md#nznbl_competitions) | Every competition of the NZNBL Genius org, with its END-year season. |
| [nznbl_pbp](additional/genius-sports-livestats-4.md#nznbl_pbp) | Play-by-play of a finished New Zealand National Basketball League game. |
| [nznbl_player_box](additional/genius-sports-livestats-4.md#nznbl_player_box) | Player box score of a finished New Zealand National Basketball League game. |
| [nznbl_schedule](additional/genius-sports-livestats-4.md#nznbl_schedule) | Every match of one New Zealand National Basketball League season (hosted WebStats): ids, status, venue, teams and scores. |
| [nznbl_shots](additional/genius-sports-livestats-4.md#nznbl_shots) | Shot chart of a finished New Zealand National Basketball League game. |
| [nznbl_team_box](additional/genius-sports-livestats-4.md#nznbl_team_box) | Team box score of a finished New Zealand National Basketball League game. |
| [slb_competitions](additional/genius-sports-livestats-4.md#slb_competitions) | Every competition of the SLB Genius org, with its END-year season. |
| [slb_pbp](additional/genius-sports-livestats-4.md#slb_pbp) | Play-by-play of a finished Super League Basketball (UK) game. |
| [slb_player_box](additional/genius-sports-livestats-5.md#slb_player_box) | Player box score of a finished Super League Basketball (UK) game. |
| [slb_schedule](additional/genius-sports-livestats-5.md#slb_schedule) | Every match of one Super League Basketball (UK) season (hosted WebStats): ids, status, venue, teams and scores. |
| [slb_shots](additional/genius-sports-livestats-5.md#slb_shots) | Shot chart of a finished Super League Basketball (UK) game. |
| [slb_team_box](additional/genius-sports-livestats-5.md#slb_team_box) | Team box score of a finished Super League Basketball (UK) game. |
| [wbbl_competitions](additional/genius-sports-livestats-5.md#wbbl_competitions) | Every competition of the WBBL Genius org, with its END-year season. |
| [wbbl_pbp](additional/genius-sports-livestats-5.md#wbbl_pbp) | Play-by-play of a finished Women's British Basketball League game. |
| [wbbl_player_box](additional/genius-sports-livestats-5.md#wbbl_player_box) | Player box score of a finished Women's British Basketball League game. |
| [wbbl_schedule](additional/genius-sports-livestats-5.md#wbbl_schedule) | Every match of one Women's British Basketball League season (hosted WebStats): ids, status, venue, teams and scores. |
| [wbbl_shots](additional/genius-sports-livestats-5.md#wbbl_shots) | Shot chart of a finished Women's British Basketball League game. |
| [wbbl_team_box](additional/genius-sports-livestats-6.md#wbbl_team_box) | Team box score of a finished Women's British Basketball League game. |

## IDs and crosswalks

| Function | Summary |
|---|---|
| [genius_game_crosswalk](additional/ids-and-crosswalks.md#genius_game_crosswalk) | Provider game key <-> Genius LiveStats match id, from a frame that carries both. |
| [genius_team_crosswalk](additional/ids-and-crosswalks.md#genius_team_crosswalk) | Sportality platform team uuid <-> Genius team id, from a Sportality frame that carries both. |
