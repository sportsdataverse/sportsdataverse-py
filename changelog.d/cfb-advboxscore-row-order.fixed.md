- **CFB `advBoxScore` row order:** `team` / `situational` / `defensive` / `drives` list the home team first, and the
  usage and special-teams sections (CFB and NFL) sort by their keys, so a game serialises to the same box in every
  process; before, these rows reshuffled from run to run. Values are unchanged. (#910)
