- **CFB:** `CFBPlayProcess` logs through its own `sdv.cfb_pbp` logger; its root-module `logging.debug` calls ran
  `logging.basicConfig()` and installed a stderr handler on the host application's root logger. (#895)
