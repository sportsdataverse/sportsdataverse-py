# CFB dataset loaders — Team: adjustment

> CFB dataset loaders — Team: adjustment — function reference in sdv-py, the SportsDataverse Python package.

## load_cfb_team_adjustment_ledger

Release: [cfb_team_adjustment_ledger](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfb_team_adjustment_ledger) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/cfb_team_adjustment_ledger/cfb_team_adjustment_ledger_{season}.parquet`
### Returns {#load_cfb_team_adjustment_ledger-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season of the game (the year the season starts). |
| `game_id` | Int64 | ESPN game id of the team-game (the play-by-play game_id). |
| `week` | Int64 | ESPN week; the postseason restarts at 1. |
| `season_type` | Int64 | ESPN season type of the game: 2 regular season, 3 postseason. |
| `team_id` | Int64 | ESPN team id of the rated team (the pos_team_id the fit uses). |
| `pos_team` | String | The team's name as the play-by-play carries it. |
| `opponent_id` | Int64 | ESPN team id of the opponent. |
| `site` | String | home, away or neutral, for the team. |
| `off_plays` | Int64 | The team's offensive plays in the adjusted-EPA fit that game (FBS-vs-FBS passes and rushes that are not garbage time by the score-margin rule). |
| `def_plays` | Int64 | The team's defensive plays in the fit that game; 0 when it had none, and its defense mean then skips the game. |
| `raw_off_epa` | Float64 | The team's EPA per offensive play that game, on the fit's plays. |
| `raw_def_epa` | Float64 | EPA per play the team's defense allowed that game, on the fit's plays; null when def_plays is 0. |
| `raw_net_epa` | Float64 | raw_off_epa minus raw_def_epa; null when def_plays is 0. |
| `off_strength_faced` | Float64 | The opponent's fitted offense, EPA per play. |
| `def_strength_faced` | Float64 | The opponent's fitted defense, EPA per play allowed. |
| `schedule_adj` | Float64 | off_strength_faced minus def_strength_faced: what the schedule term adds to the game's raw net EPA. |
| `adj_off_epa` | Float64 | raw_off_epa minus def_strength_faced. |
| `adj_def_epa` | Float64 | raw_def_epa minus off_strength_faced; null when def_plays is 0. |
| `adj_net_epa` | Float64 | adj_off_epa minus adj_def_epa, which equals raw_net_epa plus schedule_adj; null when def_plays is 0. |
| `off_weight` | Float64 | The game's weight in the season offense mean: 1 over the team's games with offensive plays in the fit. |
| `def_weight` | Float64 | The game's weight in the season defense mean: 1 over the team's games with defensive plays in the fit. Null when def_plays is 0. |
| `raw_contribution` | Float64 | The game's raw part of the season value: off_weight times raw_off_epa minus def_weight times raw_def_epa. |
| `schedule_contribution` | Float64 | The game's schedule part of the season value: def_weight times off_strength_faced minus off_weight times def_strength_faced. |
| `contribution` | Float64 | raw_contribution plus schedule_contribution; a team's rows sum to its net_adj_epa. |
| `net_adj_epa` | Float64 | The team's season opponent-adjusted net EPA per play, as cfb_team_summaries publishes it, repeated on each row. |
| `loo_net_adj_epa` | Float64 | The season value refit as if this game had never been played; null when the team would keep fewer than two valid games. |
| `game_influence` | Float64 | net_adj_epa minus loo_net_adj_epa: how far this game moved the season value. Null when loo_net_adj_epa is null. |

```python
load_cfb_team_adjustment_ledger(seasons=2024)
```
