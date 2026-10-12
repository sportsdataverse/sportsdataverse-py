- **CFB:** a Timeout row (filed under the team that called it) is no longer a change of possession: `change_of_pos_team` reads across
  timeouts and period ends to the next snap; the next play's `EP_between` reads the last real play's possession and score, and a play's
  `wp_after` the next real play's board, never the timeout's stale one. Score, timeout, kickoff no longer gives a phantom `EP_between`. (#834)
