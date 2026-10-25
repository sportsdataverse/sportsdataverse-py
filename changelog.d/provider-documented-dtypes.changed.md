- **Provider flags and numbers take their documented types:** CBS / Yahoo flags are Boolean (never "nan" / "True"),
  Odds API `outcome_point` is Float64 where markets exist, EuroLeague header `round` is Int64, college baseball
  pandas integers stay `Int64`, and the On3 rankings tables document their integer columns as integer. (#979)
- **Statcast search:** every chunk takes its returns table's dtypes; `zone`, `release_spin_rate`, `spin_axis` and the
  `*_days_*` columns are Int64 on all three routes, and an empty window has those dtypes instead of `Null`. (#979)
- **ESPN player stats / Fox:** a stat with no `value` takes a plain-number `displayValue` ("0") and is otherwise null
  (NHL "0:00"); the column is always Float64. Fox `frame()` warns when a column mixes text and numbers. (#979)
