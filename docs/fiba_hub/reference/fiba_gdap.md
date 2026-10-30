# FIBA (digital API and game pages) — FIBA digital API (GDAP, digital-api.fiba.basketball)

> FIBA (digital API and game pages) — FIBA digital API (GDAP, digital-api.fiba.basketball) — endpoint reference in sdv-py, the SportsDataverse Python package.

`sportsdataverse.basketball.fiba_hub` — 31 endpoints.

> **Note:** FIBA's terms restrict this source. The GDAP developer-portal terms limit API use to 'Authorized Users' (federations, organisers, FIBA partners) and say any broader use 'is not authorised'; the fiba.basketball terms say no portion of the site may be duplicated, redistributed or manipulated. The package owner reviewed both clauses on 2026-10-12, accepted them and approved the full intake, published data releases included. Users who are not themselves authorised by FIBA should check the terms before using the wrappers.

> **Note:** The GDAP subscription key is public (fiba.basketball ships it in every page); the runtime reads it from the live page at run time and never logs it. Officials' contact details and coaches' and officials' dates of birth are dropped from every body.

> **Note:** Id key spaces: competition_id is one per edition (2023 World Cup = 208182) and game_id is global across competitions (the <gameId> of a game-page URL). team_id is per team per competition (Germany is 279569 at the 2023 World Cup and 281545 at Paris 2024), while organisation_id is the stable federation or club (64 = Germany, every year); the game page's T_<id> and the play-by-play oId are organisation ids, not team ids.

## Federation

| Function | Summary |
|---|---|
| [fiba_gdap_federation_games](fiba_gdap/federation.md#fiba_gdap_federation_games) | A federation's recent and upcoming national-team games. |
| [fiba_gdap_federation_ranking](fiba_gdap/federation.md#fiba_gdap_federation_ranking) | FIBA world ranking of national teams. |

## Game

| Function | Summary |
|---|---|
| [fiba_gdap_game_officials](fiba_gdap/game.md#fiba_gdap_game_officials) | Officials nominated to one game (referees, table officials, commissioner). |
| [fiba_gdap_game_periods](fiba_gdap/game.md#fiba_gdap_game_periods) | Period scores of one game. |
| [fiba_gdap_games_between](fiba_gdap/game.md#fiba_gdap_games_between) | Games of EVERY FIBA competition in a local-time window (how competitions and games are discovered). |

## Player

| Function | Summary |
|---|---|
| [fiba_gdap_player_game_stats](fiba_gdap/player.md#fiba_gdap_player_game_stats) | A player's game log (full box lines) at a competition. |
| [fiba_gdap_player_info](fiba_gdap/player.md#fiba_gdap_player_info) | A player's bio and per-game averages with ranks at a competition. |
| [fiba_gdap_player_stats](fiba_gdap/player.md#fiba_gdap_player_stats) | A player's per-game averages, totals and highs at a competition. |

## Team

| Function | Summary |
|---|---|
| [fiba_gdap_team_games](fiba_gdap/team.md#fiba_gdap_team_games) | A team's games with each team's leaders. |
| [fiba_gdap_team_player_stats](fiba_gdap/team.md#fiba_gdap_team_player_stats) | Per-game averages of every player of a team at a competition. |
| [fiba_gdap_team_staff](fiba_gdap/team.md#fiba_gdap_team_staff) | Coaches of every team of a competition. |
| [fiba_gdap_team_stats](fiba_gdap/team.md#fiba_gdap_team_stats) | A team's per-game averages and totals at a competition, with the bench totals. |
| [fiba_gdap_teams](fiba_gdap/team.md#fiba_gdap_teams) | Teams of a competition with their profile (final ranking, per-game averages). |

## Other

| Function | Summary |
|---|---|
| [fiba_gdap_awards](fiba_gdap/other.md#fiba_gdap_awards) | Awards of a competition (MVP, All-Star Five...). |
| [fiba_gdap_competition](fiba_gdap/other.md#fiba_gdap_competition) | One competition (edition): season, dates, hosts, zone, category and competition family. |
| [fiba_gdap_editions](fiba_gdap/other.md#fiba_gdap_editions) | Every edition of a competition family between two seasons. |
| [fiba_gdap_final_standings](fiba_gdap/other.md#fiba_gdap_final_standings) | Final ranking of a competition. |
| [fiba_gdap_game](fiba_gdap/other.md#fiba_gdap_game) | One game's header: teams, score, venue, attendance and status codes (no box score or play-by-play: see fiba_gamepage). |
| [fiba_gdap_gateway_competition_ids](fiba_gdap/other.md#fiba_gdap_gateway_competition_ids) | Ids of the competitions that run in a date window. |
| [fiba_gdap_gateway_federation_ranking](fiba_gdap/other.md#fiba_gdap_gateway_federation_ranking) | FIBA world ranking from the server-side gateway (same items as fiba_gdap_federation_ranking). |
| [fiba_gdap_medal_count](fiba_gdap/other.md#fiba_gdap_medal_count) | All-time medal table of a competition family. |
| [fiba_gdap_national_teams](fiba_gdap/other.md#fiba_gdap_national_teams) | Every national federation FIBA lists. |
| [fiba_gdap_nominations](fiba_gdap/other.md#fiba_gdap_nominations) | Officials nominated for a competition. |
| [fiba_gdap_ranking_zones](fiba_gdap/other.md#fiba_gdap_ranking_zones) | FIBA world-ranking zones. |
| [fiba_gdap_roster](fiba_gdap/other.md#fiba_gdap_roster) | A team's roster at a competition. |
| [fiba_gdap_roster_coaches](fiba_gdap/other.md#fiba_gdap_roster_coaches) | A team's coaches on its roster at a competition. |
| [fiba_gdap_roster_latest](fiba_gdap/other.md#fiba_gdap_roster_latest) | A team's latest roster (same shape as the roster). |
| [fiba_gdap_round_gamedays](fiba_gdap/other.md#fiba_gdap_round_gamedays) | Rounds of a competition with their game days. |
| [fiba_gdap_round_standings](fiba_gdap/other.md#fiba_gdap_round_standings) | Group standings of every round of a competition. |
| [fiba_gdap_rounds](fiba_gdap/other-2.md#fiba_gdap_rounds) | Rounds of a competition (group stage, knockout rounds). |
| [fiba_gdap_schedule](fiba_gdap/other-2.md#fiba_gdap_schedule) | Schedule and results of one competition (edition): one row per game. |
