- **NFL play-by-play (ESPN):** 2009-13 passes ESPN types plain "Pass" now count as completions when the text reads as
  a catch, a penalty-nullified pass is no attempt or target, fumble-typed runs are rushes, and the box score keeps one
  name per player. 2009-13 passer completions exact vs nflverse: 4% to 95%. (#1037)
- **NFL win probability (ESPN):** a game ESPN has no line for (most of 2009-2011) takes the nflverse schedule's
  spread and total instead of home by 2.5; a supplied summary still makes no request. (#1037)
- **NFL CPOE:** completion probability is scored only on nflfastR's valid passes (no throwaways or spikes), and
  `cp_model.ubj` is refit from 2009; team-week CPOE bias vs nflverse goes from -3.0 (2024) and -9.2 (2013) to near 0. (#1037)
