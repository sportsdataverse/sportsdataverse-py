- **CFB play-by-play:** a `drive.*` column no drive of a game carries (no `end` block in 2005-09 games, no
  `start.clock` or `team` in a few others) is an all-null column of its documented String / Int64 type, not
  `Null`; every `drive.*` field is typed from the returns table. (#980)
