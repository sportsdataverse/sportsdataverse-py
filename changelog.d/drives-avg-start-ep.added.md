- **CFB / NFL:** the advanced box's `drives` rows carry `avg_start_ep`, the team's drive starts in
  expected points: each drive valued at its first scrimmage snap's `start.yardsToEndzone` on the
  league's bundled field-position curve (`cfb_field_position_ep` / `nfl_field_position_ep`), averaged
  over the team's drives. It is Game on Paper's Drives-table EP row and Paper Index field-position
  input, moved into the package. On a window of a game (`create_box_score` on a slice of
  `plays_frame`, as Game on Paper's quarter and half boxes do) each drive with a snap in the window
  is valued at its first snap of the whole game, so a drive carried over from the previous quarter
  keeps its real start. Only scrimmage snaps count (Game on Paper's definition), so a drive with no
  scrimmage snap is not averaged. The `espn_{cfb,nfl}_adv_drives` releases pick it up on their next rebuild.
