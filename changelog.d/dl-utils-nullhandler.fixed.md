- **`download()`:** a call made without a `logger` no longer adds another `NullHandler` to the
  `sdv.dl_utils` logger each time; the module adds its one handler at import. (#992)
