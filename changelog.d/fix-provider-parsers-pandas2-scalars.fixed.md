- **NFL.com Shield and Statcast parsers:** records that are not a list, or a record that is not an object
  (`parse_nfl_team(["a"])`, `{"rosters": 5}`), are zero rows with a `SchemaDriftWarning` on pandas 2 and 3; it was
  an N x 0 frame on pandas 2, a raise on pandas 3. A null row (`None` / `NaN` / `pd.NA`) is no row. (#945)
- **NFL.com Shield / 247Sports / MLS parsers:** `parse_nfl_standings` no longer raises on a non-object week or a
  non-list `standings`; `parse_sports247_teams`, `_institution_rankings` and `parse_mls_entity` read a scalar array
  as drift, not `value` rows; 247Sports keeps a pandas-2 gap null, never `"nan"`. (#945)
