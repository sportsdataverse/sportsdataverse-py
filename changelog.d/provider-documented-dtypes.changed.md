- **Provider columns take their documented types:** CBS / Yahoo flags are Boolean (never "nan" / "True"
  strings), Odds API `outcome_point` and ESPN player-stat columns are always Float64, EuroLeague header
  `round` and college baseball pandas counts are integers, Statcast search chunks pin their returns table.
