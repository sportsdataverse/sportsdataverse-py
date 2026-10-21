- **ESPN:** `parse_team_roster` on a position-grouped roster returns zero rows with a `SchemaDriftWarning` when any
  group or player is not an object; 0.1.5 returned the other groups' rows. (#941)
