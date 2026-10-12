- **CFB:** a Timeout row (filed under the team that called it) is no longer a change of possession: `change_of_pos_team` reads across
  timeouts and period ends to the next snap, and the next play's `EP_between` reads the last real play's possession and score, not
  the timeout's. A score, a timeout and the kickoff no longer give the kickoff a phantom `EP_between`. (#834)
