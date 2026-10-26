- **CFB / NFL:** the advanced box's `drives` rows carry `avg_start_ep`, the team's average drive start in expected
  points: each drive's first scrimmage snap valued on the league's bundled field-position curve
  (`cfb_field_position_ep` / `nfl_field_position_ep`). It is Game on Paper's Drives-table EP row, moved here. (#836)
- **CFB / NFL:** on a slice of `plays_frame` (Game on Paper's quarter and half boxes), a drive is valued at its first
  snap of the whole game, so a carried-over drive keeps its real start; a drive with no scrimmage snap is not averaged.
  The `espn_{cfb,nfl}_adv_drives` releases pick it up on their next rebuild. (#836)
