- **NFL, CFB:** processed plays keep their documented Float64 for the EP/WP family, `first_down_prob`, `prob_2pt` and
  `drive_start` (they were Float32 when a model scored), with no value change, and NFL `teamParticipants` has one struct
  field order; `create_box_score(plays_frame)` still reproduces `advBoxScore` exactly. (#797)
