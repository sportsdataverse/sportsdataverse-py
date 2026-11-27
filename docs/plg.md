# P. LEAGUE+

> sdv-py P. LEAGUE+: endpoint references, dataset loaders and parsers for P. LEAGUE+ in the SportsDataverse Python package.

# P. LEAGUE+ (`sportsdataverse.basketball.plg`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [P. LEAGUE+ (Taiwan) and Preciser](#p-league-taiwan-and-preciser) | `api.preciser.io`, `pleagueofficial.com` | 19 | none |
| [Additional functions](reference/additional) | hand-written wrappers & helpers | 2 | — |

## P. LEAGUE+ (Taiwan) and Preciser {#p-league-taiwan-and-preciser}

| Reference | Functions |
|---|---:|
| [P. LEAGUE+ site (pleagueofficial.com, Taiwan)](reference/pleague) | 11 |
| [P. LEAGUE+ vendor API (api.preciser.io, Taiwan)](reference/pleague_preciser) | 7 |
| [Hand-written wrappers](reference/additional) | 1 |
## Tools and helpers

### IDs and crosswalks {#ids-and-crosswalks}

- [`pleague_team_crosswalk`](reference/additional#pleague_team_crosswalk)

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)

## Python ↔ R parity

Each `sportsdataverse` function and its equivalent in the sister R package, [`hoopR`](https://hoopR.sportsdataverse.org). Same-named where possible; the R column links the package's pkgdown reference.

| `sportsdataverse.basketball.plg` (Python) | `hoopR` (R) |
|---|---|
| [`pleague_box_players`](reference/pleague#pleague_box_players) | [`pleague_box_players`](https://hoopR.sportsdataverse.org/reference/pleague_box_players.html) |
| [`pleague_box_teams`](reference/pleague#pleague_box_teams) | [`pleague_box_teams`](https://hoopR.sportsdataverse.org/reference/pleague_box_teams.html) |
| [`pleague_game`](reference/pleague#pleague_game) | [`pleague_game`](https://hoopR.sportsdataverse.org/reference/pleague_game.html) |
| [`pleague_officials`](reference/pleague#pleague_officials) | [`pleague_officials`](https://hoopR.sportsdataverse.org/reference/pleague_officials.html) |
| [`pleague_pbp`](reference/pleague#pleague_pbp) | [`pleague_pbp`](https://hoopR.sportsdataverse.org/reference/pleague_pbp.html) |
| [`pleague_periods`](reference/pleague#pleague_periods) | [`pleague_periods`](https://hoopR.sportsdataverse.org/reference/pleague_periods.html) |
| [`pleague_player`](reference/pleague#pleague_player) | [`pleague_player`](https://hoopR.sportsdataverse.org/reference/pleague_player.html) |
| [`pleague_player_seasons`](reference/pleague#pleague_player_seasons) | [`pleague_player_seasons`](https://hoopR.sportsdataverse.org/reference/pleague_player_seasons.html) |
| [`pleague_preciser_injury`](reference/pleague_preciser#pleague_preciser_injury) | [`pleague_preciser_injury`](https://hoopR.sportsdataverse.org/reference/pleague_preciser_injury.html) |
| [`pleague_preciser_lead_flow`](reference/pleague_preciser#pleague_preciser_lead_flow) | [`pleague_preciser_lead_flow`](https://hoopR.sportsdataverse.org/reference/pleague_preciser_lead_flow.html) |
| [`pleague_preciser_rotation`](reference/pleague_preciser#pleague_preciser_rotation) | [`pleague_preciser_rotation`](https://hoopR.sportsdataverse.org/reference/pleague_preciser_rotation.html) |
| [`pleague_preciser_score_log`](reference/pleague_preciser#pleague_preciser_score_log) | [`pleague_preciser_score_log`](https://hoopR.sportsdataverse.org/reference/pleague_preciser_score_log.html) |
| [`pleague_preciser_shots`](reference/pleague_preciser#pleague_preciser_shots) | [`pleague_preciser_shots`](https://hoopR.sportsdataverse.org/reference/pleague_preciser_shots.html) |
| [`pleague_preciser_summary_teams`](reference/pleague_preciser#pleague_preciser_summary_teams) | [`pleague_preciser_summary_teams`](https://hoopR.sportsdataverse.org/reference/pleague_preciser_summary_teams.html) |
| [`pleague_preciser_win_probability`](reference/pleague_preciser#pleague_preciser_win_probability) | [`pleague_preciser_win_probability`](https://hoopR.sportsdataverse.org/reference/pleague_preciser_win_probability.html) |
| [`pleague_schedule`](reference/pleague#pleague_schedule) | [`pleague_schedule`](https://hoopR.sportsdataverse.org/reference/pleague_schedule.html) |
| [`pleague_standings`](reference/pleague#pleague_standings) | [`pleague_standings`](https://hoopR.sportsdataverse.org/reference/pleague_standings.html) |
| [`pleague_stat_player`](reference/pleague#pleague_stat_player) | [`pleague_stat_player`](https://hoopR.sportsdataverse.org/reference/pleague_stat_player.html) |
| [`pleague_team_crosswalk`](reference/additional#pleague_team_crosswalk) | [`pleague_team_crosswalk`](https://hoopR.sportsdataverse.org/reference/pleague_team_crosswalk.html) |
