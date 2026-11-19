- **NFL Shield PBP `play_type`:** a Shield `UNSPECIFIED` play with stats and a penalty or no pass / rush / punt / FG
  attempt is `no_play` (was null), as in nflfastR's `translate_play_type_nfl`; on 2025 that is 111 rows, and all 135
  `UNSPECIFIED` rows now match nflverse, whose 10 stat-less ones stay null (#1238)
