- **NCAA:** a deleted stats.ncaa.org contest ("Contest not found ...", HTTP 200, 57 bytes) is an answer, not an unsolved
  challenge: the fetcher raises `NoDataError` and the capture moves on. The canary browser transports (`decodo_patchright`,
  `managed_cdp`) also hand a short `"<tab> not available"` answer back instead of re-solving it. Before this, deleted MBB
  exhibition 6646892 exhausted the proxy pool and halted every nightly MBB capture from 2026-11-01, stranding 178 games.
