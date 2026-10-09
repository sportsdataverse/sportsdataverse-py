- **HockeyTech empty-net goals from a team's own half were measured to the wrong net.**
  `add_shot_distance_angle` measures to the nearer net (`89 - |x|`), which per-event validation
  showed is right for own-half events with a goalie in net: they are near-net events whose
  coordinates the feed mirrored (own-half quality shots match ordinary ones at 10 / 24 / 35 ft).
  An own-half empty-net goal is a genuine long shot (27 in 320 PWHL games; median 126 ft,
  measured 59 ft). Empty-net goals (`empty_net` "1") now measure to the net their team attacks;
  every other event is unchanged (sdv-internal-refs #52). `shot_distance`, `shot_angle`,
  `scoring_chance` and the xG features of those goals change.
