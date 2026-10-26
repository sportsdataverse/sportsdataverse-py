- **CFB:** `fox_cfb_play_process` reads Fox's absolute `yardStart` through `fox_common`'s geometry, so
  away-offense snaps are no longer mirrored (EP/EPA/WPA); an uncovered game raises `NoDataError`, the
  backup makes no ESPN request, and the source contract rejects an adapted summary with a mirrored field. (#783)
