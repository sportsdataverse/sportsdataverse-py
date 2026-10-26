- **Hockey:** `sihf_phases` gives a null `phase_name` when a phase's name is missing, blank or not
  a string, like the other SIHF parsers; it gave `""` for a missing or blank name and the
  stringified value (`"['x']"`) for a list or object. (#827)
