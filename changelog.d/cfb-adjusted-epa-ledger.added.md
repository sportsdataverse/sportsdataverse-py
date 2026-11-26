- **CFB:** `cfb_adjusted_epa_ledger` lays out the season adjusted EPA game by game: raw net, the schedule term, each
  game's weight and contribution (they sum to `net_adj_epa`), and with `influence=True` a leave-one-game-out refit
  giving how far each game moved the team's season value. (#1334)
