- **HockeyTech:** an empty-net goal (`empty_net` "1") is measured to the net its team attacks, so a goal from the
  team's own half is the long shot it is; every other event still measures to the nearer net (`89 - |x|`), which is
  right for own-half events with a goalie in net, whose coordinates the feed mirrors. (#735)
- **HockeyTech:** `shot_distance`, `shot_angle`, `scoring_chance` and the xG features of those empty-net goals change.
  (#735)
