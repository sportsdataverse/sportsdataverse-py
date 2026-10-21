- **ESPN:** since #907, `parse_team_roster` on a grouped roster returns zero rows with a `SchemaDriftWarning`
  when any group or player is not an object; it used to return the other groups' rows. (#941)
