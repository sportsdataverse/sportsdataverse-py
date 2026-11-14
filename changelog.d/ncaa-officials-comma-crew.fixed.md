- **NCAA MBB/WBB:** `parse_ncaa_bb_officials` returns one row per official on pre-2017 stats.ncaa.org pages, which
  list the whole crew in one comma-separated cell; it returned one row holding all three names. A name suffix
  (", Jr.") stays with its name. Found by the 2026-11-13 D-I tab probe (median 1 row per game through 2016-17). (#1205)
