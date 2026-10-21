- **NFL.com Shield and Statcast parsers:** a record that is not an object (`parse_nfl_team(["a"])`,
  `{"rosters": ["a"]}`) is zero rows with a `SchemaDriftWarning` on pandas 2 and 3; it was an N x 0 frame on
  pandas 2, and Statcast raised `TypeError` on pandas 3. A null NFL record is no row. (#TBD)
- **NFL.com Shield / 247Sports parsers:** `parse_nfl_standings` no longer raises on a week that is not an object;
  247Sports wraps every scalar in a record list as `value` (not only a leading one) and keeps a gap null on
  pandas 2 instead of the string `"nan"`. (#TBD)
