- **Raw payload schemas:** `espn_summary` accepts `espnWP` and `teamInfo` as either a dict or a list. NBA and WNBA
  summaries carry `espnWP` as an empty list (and WNBA raw captures `teamInfo` too), which `validate_payload` reported
  as a type error on every committed WNBA capture. (#PR)
