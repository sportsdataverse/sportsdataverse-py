- **Football / participants:** `espn_play_participants` no longer panics in polars 2.0 when the athlete
  lookup is empty. The long frame keeps `String` columns, so the empty-list `fill_null` never sees a
  `List(Null)` column. The panic got past `except Exception` and killed the GOP API worker. (#1251)
