- **NCAA:** a deleted stats.ncaa.org contest ("Contest not found ...", HTTP 200, 57 bytes) is an answer, not an
  unsolved challenge: the fetcher raises `NoDataError` and the capture moves on. Before this, deleted MBB exhibition
  6646892 exhausted the proxy pool and halted every nightly MBB capture from 2026-11-01, stranding 178 games. (#1171)
- **NCAA:** the canary browser transports (`decodo_patchright`, `managed_cdp`) hand a short `"<tab> not available"`
  answer back instead of re-solving it. (#1171)
