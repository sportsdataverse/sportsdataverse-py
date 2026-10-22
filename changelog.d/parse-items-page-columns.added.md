- **ESPN Core v2 lists:** every wrapper `parse_items` parses (`athletes_index`, `venues`, `season_athletes`, ...) returns
  two Int64 page-envelope columns, `count` (items across every page) and `page_count`, polars and pandas alike, so one
  page of several shows in the frame; null where the payload has no envelope (`athlete_statisticslog`).
