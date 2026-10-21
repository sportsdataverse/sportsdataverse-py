- **NFL:** `NFLPlayProcess` (ESPN and the CBS/Fox/Yahoo/Shield adapters) takes a play's end team/down/distance from
  the next play in the game and half, and keeps a half-ending plain snap's possession, not ESPN's `end` (corrupt
  2014-2021); the published 2014-2021 ESPN NFL pbp and everything built from it need the reprocess. (#785)
