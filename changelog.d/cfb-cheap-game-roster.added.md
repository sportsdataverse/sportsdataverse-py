- **CFB:** `CFBPlayProcess(resolve_missing=...)` is passed to `espn_cfb_play_participants`, so a caller serving
  requests can skip the up-to-50 per-athlete lookups for names the participants sidecar omits. (#1034)
- **HTTP:** `SDV_PY_HTTP_MAX_WAIT` (whole seconds) overrides `download`'s 180 s total retry-wait budget, alongside
  `SDV_PY_HTTP_TIMEOUT` and `SDV_PY_HTTP_RETRIES`. (#1034)
