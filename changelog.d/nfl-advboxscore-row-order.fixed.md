- **NFL `advBoxScore` row order:** `team` / `situational` / `defensive` / `drives` list the home team first, so these
  four tables serialise in the same order in every process; before, their rows reshuffled from run to run. Values
  are unchanged. (#917)
