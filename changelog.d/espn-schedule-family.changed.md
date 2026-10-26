- **ESPN schedules:** `espn_cfb_schedule` and `espn_mlb_schedule` return an empty frame with the documented columns on a
  day with no games; they returned `None`. (#855)
- **ESPN schedules:** the seven scoreboard-backed `espn_<lg>_schedule` emit their documented columns in documented order
  on every day (a column the day left empty is a typed null, not absent); columns ESPN adds later follow. A documented
  integer column no longer arrives as Float64 when one event lacks it (`tournament_id`). (#855)
