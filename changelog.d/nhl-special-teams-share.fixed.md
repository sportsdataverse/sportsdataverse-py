- **NHL / PWHL:** special-teams value gave every skater on a PP/PK unit the unit's whole gap to the league rate, so GAR
  counted each PP goal ~5x and PK goal ~4x; each skater now takes an equal share. The NHL PP/PK rate is refitted on the
  retrained xG, 6.2 -> 5.91 xGF/60 (`tools/fit_nhl_special_teams_rates.py`). (#1211)
